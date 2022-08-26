package namenode

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"go-fs/pkg/e"
	"go-fs/pkg/tree"
	"go-fs/pkg/util"
	dn "go-fs/proto/datanode"
	namenode_pb "go-fs/proto/namenode"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

type NameNodeMetaData struct {
	BlockId        string
	BlockAddresses []util.DataNodeInstance
}

type ReDistributeDataRequest struct {
	DataNodeUri string
}

type UnderReplicatedBlocks struct {
	BlockId           string
	HealthyDataNodeId int64
}

type Service struct {
	namenode_pb.UnimplementedNameNodeServiceServer

	Port               uint16
	BlockSize          uint64
	ReplicationFactor  uint64
	IdToDataNodes      map[int64]util.DataNodeInstance
	FileNameToBlocks   map[string][]string
	BlockToDataNodeIds map[string][]int64
	DataNodeMessageMap map[string]DataNodeMessage
	DataNodeHeartBeat  map[string]time.Time
	DirTree            *tree.DirTree
	RaftNode           *raft.Raft
	RaftLog            *boltdb.BoltStore
}

type DataNodeMessage struct {
	UsedDisk   uint64
	UsedMem    uint64
	CpuPercent float32
	TotalMem   uint64
	TotalDisk  uint64
}

func NewService(r *raft.Raft, log *boltdb.BoltStore, blockSize uint64, replicationFactor uint64, serverPort uint16) *Service {
	return &Service{
		RaftNode:           r,
		RaftLog:            log,
		Port:               serverPort,
		BlockSize:          blockSize,
		ReplicationFactor:  replicationFactor,
		FileNameToBlocks:   make(map[string][]string),
		IdToDataNodes:      make(map[int64]util.DataNodeInstance),
		BlockToDataNodeIds: make(map[string][]int64),
		DataNodeMessageMap: make(map[string]DataNodeMessage),
		DataNodeHeartBeat:  make(map[string]time.Time),
		DirTree:            initDirTree(),
	}
}

func initDirTree() *tree.DirTree {
	root := &tree.DirTreeNode{
		Name:     "/",
		Children: []*tree.DirTreeNode{},
	}
	return &tree.DirTree{Root: root}
}

// SelectRandomDataNodes 选择datanode节点
func (nameNode *Service) SelectRandomDataNodes(availableDataNodes []int64, replicationFactor uint64) (randomSeletctedDataNodes []int64) {
	//已经被选的 data node, 不应该在被选择
	dataNodePresentMap := make(map[int64]struct{})

	//根据CPU、Disk、Mem情况选择备份的data node 现新增key作为判断参数，key为datanode加入的时间戳
	chooseDataNode := make(map[float32][]int64)
	for _, i := range availableDataNodes {
		addr := nameNode.IdToDataNodes[i].Host + ":" + nameNode.IdToDataNodes[i].ServicePort
		message := nameNode.DataNodeMessageMap[addr]
		//计算磁盘占用率
		diskPercent := message.UsedDisk / message.TotalDisk
		//计算内存占用率
		memPercent := message.UsedMem / message.TotalMem
		//计算该datanode的加权值,表明按磁盘占用率30%、内存占用率30%、datanode加入时间30%、CPU利用率10%的权重比进行挑选datanode
		calculate := float32(diskPercent)*0.3 + float32(memPercent)*0.3 + message.CpuPercent*0.1 + float32(i)*0.3
		//存入临时map
		chooseDataNode[calculate] = append(chooseDataNode[calculate], i)
	}

	//将map的key存入切片进行排序
	var list []float64
	for k, _ := range chooseDataNode {
		list = append(list, float64(k))
	}
	sort.Float64s(list)

	for i := uint64(0); i < replicationFactor; {
		chosenKeys := float32(list[0])
		list = list[1:]
		for _, v := range chooseDataNode[chosenKeys] {
			if _, ok := dataNodePresentMap[v]; !ok {
				dataNodePresentMap[v] = struct{}{}
				randomSeletctedDataNodes = append(randomSeletctedDataNodes, v)
				i++
				//选定节点后还要估算增量去更新map,目前以指定的BlockSize来估算的，如果要精确估算，计算量会增大
				instance := nameNode.IdToDataNodes[v]
				message := nameNode.DataNodeMessageMap[instance.Host+":"+instance.ServicePort]
				log.Println("估算前datanode的信息：", message)
				nameNode.DataNodeMessageMap[instance.Host+":"+instance.ServicePort] = DataNodeMessage{
					UsedDisk:   message.UsedDisk + nameNode.BlockSize,
					UsedMem:    message.UsedMem + nameNode.BlockSize,
					CpuPercent: message.CpuPercent,
					TotalMem:   message.TotalMem,
					TotalDisk:  message.TotalDisk,
				}
				log.Println("估算后datanode的信息:", nameNode.DataNodeMessageMap[instance.Host+":"+instance.ServicePort])
			}
		}
	}
	return
}

// gRPC methods

// GetBlockSize 获取name node的block size
func (nn *Service) GetBlockSize(ctx context.Context, req *namenode_pb.GetBlockSizeRequest) (*namenode_pb.GetBlockSizeResponse, error) {
	var res namenode_pb.GetBlockSizeResponse

	if req.Request {
		res.BlockSize = nn.BlockSize
	}

	return &res, nil
}

func DataNodeInstance2PB(dni util.DataNodeInstance) *namenode_pb.DataNodeInstance {
	dataNodeInstance := &namenode_pb.DataNodeInstance{
		Host:        dni.Host,
		ServicePort: dni.ServicePort,
	}

	return dataNodeInstance
}

func NameNodeMetaData2PB(nnmd NameNodeMetaData) *namenode_pb.NameNodeMetaData {
	var blockAddresses []*namenode_pb.DataNodeInstance
	for _, dni := range nnmd.BlockAddresses {
		blockAddresses = append(blockAddresses, DataNodeInstance2PB(dni))
	}

	nameNodeMetaData := &namenode_pb.NameNodeMetaData{
		BlockId:        nnmd.BlockId,
		BlockAddresses: blockAddresses,
	}
	return nameNodeMetaData
}

// ReadData 返回metadata, 包含该文件每一个block的id与data node的地址
func (nn *Service) ReadData(ctx context.Context, req *namenode_pb.ReadRequst) (*namenode_pb.ReadResponse, error) {
	modedFilePath := util.ModFilePath(req.FileName)
	zap.S().Debug("file map: ", nn.FileNameToBlocks)
	zap.S().Debug("want file: ", modedFilePath)

	var res namenode_pb.ReadResponse

	_, ok := nn.FileNameToBlocks[modedFilePath]
	if !ok {
		return nil, e.ErrFileDoesNotExist
	}
	fileBlocks := nn.FileNameToBlocks[modedFilePath]

	for _, block := range fileBlocks {
		var blockAddresses []util.DataNodeInstance

		zap.S().Debug("读取到的blockId为：", block)
		targetDataNodeIds := nn.BlockToDataNodeIds[block]
		for _, dataNodeId := range targetDataNodeIds {
			zap.S().Debug("读取到的blockAddresses为：", blockAddresses)
			blockAddresses = append(blockAddresses, nn.IdToDataNodes[dataNodeId])
		}

		res.NameNodeMetaDataList = append(res.NameNodeMetaDataList, NameNodeMetaData2PB(NameNodeMetaData{BlockId: block, BlockAddresses: blockAddresses}))
	}

	bytes, err := json.Marshal(nn)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.ReadResponse{}, err
	}
	nn.RaftNode.Apply(bytes, time.Second*1)
	return &res, nil
}

// WriteData 返回metadata, 包含写入文件的每一个block的id与data node的地址
func (nn *Service) WriteData(ctx context.Context, req *namenode_pb.WriteRequest) (*namenode_pb.WriteResponse, error) {
	modedFilePath := util.ModFilePath(req.FileName)
	// 不允许重复写
	if _, ok := nn.FileNameToBlocks[modedFilePath]; ok {
		return &namenode_pb.WriteResponse{}, e.ErrDuplicatedWrite
	}

	var res namenode_pb.WriteResponse

	nn.FileNameToBlocks[modedFilePath] = []string{}

	filename := util.ModPath(req.FileName)
	insert := nn.DirTree.Insert(filename)
	if !insert {
		return &namenode_pb.WriteResponse{}, e.ErrSubDirTree
	}
	zap.S().Debugf("插入后filemap为: %v", nn.FileNameToBlocks)
	zap.S().Debugf("插入后目录树为: %v", nn.DirTree.LookAll())

	/*numberOfBlocksToAllocate := uint64(math.Ceil(float64(req.FileSize) / float64(nn.BlockSize)))
	log.Println("分配块的数量:", numberOfBlocksToAllocate)
	*/
	nameNodeMetaDataList := nn.allocateBlocks(modedFilePath, req.BlockNumber)
	zap.S().Debugf("分配块的信息： %v", nameNodeMetaDataList)

	for _, nnmd := range nameNodeMetaDataList {
		res.NameNodeMetaDataList = append(res.NameNodeMetaDataList, NameNodeMetaData2PB(nnmd))
	}
	bytes, err := json.Marshal(nn)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.WriteResponse{}, err
	}
	nn.RaftNode.Apply(bytes, time.Second*1)
	return &res, nil
}

func (nameNode *Service) allocateBlocks(fileName string, numberOfBlocks uint64) (metadata []NameNodeMetaData) {
	//创建写入文件的 slot
	nameNode.FileNameToBlocks[fileName] = []string{}

	// 获取所有 data nodes 的id
	var dataNodesAvailable []int64
	for k, _ := range nameNode.IdToDataNodes {
		dataNodesAvailable = append(dataNodesAvailable, k)
	}
	dataNodesAvailableCount := uint64(len(dataNodesAvailable))

	for i := uint64(0); i < numberOfBlocks; i++ {
		//添加 block 的 slot
		blockId := uuid.New().String()
		nameNode.FileNameToBlocks[fileName] = append(nameNode.FileNameToBlocks[fileName], blockId)

		//每一个block进行备份
		//如果全局设置的备份个数 > data1 node 的个数, 则只备份文件 data1 node 个
		// 否则, 备份文件全局设置的备份个数
		var blockAddresses []util.DataNodeInstance
		var replicationFactor uint64
		if nameNode.ReplicationFactor > dataNodesAvailableCount {
			replicationFactor = dataNodesAvailableCount
		} else {
			replicationFactor = nameNode.ReplicationFactor
		}

		// 将block分配给各个data nodes
		targetDataNodeIds := nameNode.assignDataNodes(blockId, dataNodesAvailable, replicationFactor)
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}

		// 创建此次写入的 meta data1
		metadata = append(metadata, NameNodeMetaData{BlockId: blockId, BlockAddresses: blockAddresses})
	}
	return
}

func (nameNode *Service) assignDataNodes(blockId string, dataNodesAvailable []int64, replicationFactor uint64) []int64 {
	// 随机选择block备份的data nodes
	targetDataNodeIds := nameNode.SelectRandomDataNodes(dataNodesAvailable, replicationFactor)
	nameNode.BlockToDataNodeIds[blockId] = targetDataNodeIds
	return targetDataNodeIds
}

func (nameNode *Service) ReDistributeData(request *ReDistributeDataRequest, reply *bool) error {
	log.Printf("DataNode %s is dead, trying to redistribute data1\n", request.DataNodeUri)
	deadDataNodeSlice := strings.Split(request.DataNodeUri, ":")
	var deadDataNodeId int64

	// de-register the dead DataNode from IdToDataNodes meta
	for id, dn := range nameNode.IdToDataNodes {
		if dn.Host == deadDataNodeSlice[0] && dn.ServicePort == deadDataNodeSlice[1] {
			deadDataNodeId = id
			break
		}
	}
	delete(nameNode.IdToDataNodes, deadDataNodeId)

	// construct under-replicated blocks list and
	// de-register the block entirely in favour of re-creation
	var underReplicatedBlocksList []UnderReplicatedBlocks
	for blockId, dnIds := range nameNode.BlockToDataNodeIds {
		for i, dnId := range dnIds {
			if dnId == deadDataNodeId {
				healthyDataNodeId := nameNode.BlockToDataNodeIds[blockId][(i+1)%len(dnIds)]
				underReplicatedBlocksList = append(
					underReplicatedBlocksList,
					UnderReplicatedBlocks{blockId, healthyDataNodeId},
				)
				delete(nameNode.BlockToDataNodeIds, blockId)
				// TODO: trigger data1 deletion on the existing data1 nodes
				break
			}
		}
	}

	// verify if re-replication would be possible
	if len(nameNode.IdToDataNodes) < int(nameNode.ReplicationFactor) {
		log.Println("Replication not possible due to unavailability of sufficient DataNode(s)")
		return nil
	}

	var availableNodes []int64
	for k, _ := range nameNode.IdToDataNodes {
		availableNodes = append(availableNodes, k)
	}

	// attempt re-replication of under-replicated blocks
	for _, blockToReplicate := range underReplicatedBlocksList {

		// fetch the data1 from the healthy DataNode
		healthyDataNode := nameNode.IdToDataNodes[blockToReplicate.HealthyDataNodeId]
		dataNodeInstance, rpcErr := grpc.Dial(healthyDataNode.Host+":"+healthyDataNode.ServicePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if rpcErr != nil {
			continue
		}

		defer dataNodeInstance.Close()

		getRequest := dn.GetReq{
			//BlockId: blockToReplicate.BlockId,
			BlockId: blockToReplicate.BlockId,
		}

		resp, rpcErr := dn.NewDataNodeClient(dataNodeInstance).Get(context.Background(), &getRequest)
		util.Check(rpcErr)
		blockContents := resp.Data

		// initiate the replication of the block contents
		targetDataNodeIds := nameNode.assignDataNodes(blockToReplicate.BlockId, availableNodes, nameNode.ReplicationFactor)
		var blockAddresses []*dn.DataNodeInstance
		for _, dataNodeId := range targetDataNodeIds {
			instance := nameNode.IdToDataNodes[dataNodeId]
			blockAddresses = append(blockAddresses, &dn.DataNodeInstance{
				Host:        instance.Host,
				ServicePort: instance.ServicePort,
			})
		}
		startingDataNode := blockAddresses[0]
		remainingDataNodes := blockAddresses[1:]

		targetDataNodeInstance, rpcErr := grpc.Dial(startingDataNode.Host+":"+startingDataNode.ServicePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		util.Check(rpcErr)
		defer targetDataNodeInstance.Close()

		putRequest := dn.PutReq{
			BlockId:          blockToReplicate.BlockId,
			Data:             blockContents,
			ReplicationNodes: remainingDataNodes,
		}
		_, rpcErr = dn.NewDataNodeClient(dataNodeInstance).Put(context.Background(), &putRequest)
		util.Check(rpcErr)

		log.Printf("Block %s replication completed for %+v\n", blockToReplicate.BlockId, targetDataNodeIds)
	}
	bytes, err := json.Marshal(nameNode)
	if err != nil {
		log.Println("cannot marshal data")
		return err
	}
	nameNode.RaftNode.Apply(bytes, time.Second*1)
	return nil
}

func (s *Service) DeleteData(c context.Context, req *namenode_pb.DeleteDataReq) (*namenode_pb.DeleteDataResp, error) {
	dirTreeFilePath := util.ModPath(req.FileName)
	modedFilePath := util.ModFilePath(req.FileName)

	var res namenode_pb.DeleteDataResp
	s.DirTree.Delete(s.DirTree.Root, dirTreeFilePath)
	zap.S().Debug("删除文件后目录树为:", s.DirTree.LookAll())

	_, ok := s.FileNameToBlocks[modedFilePath]
	if !ok {
		return nil, e.ErrFileDoesNotExist
	}

	fileBlocks := s.FileNameToBlocks[modedFilePath]

	for _, block := range fileBlocks {
		var blockAddresses []util.DataNodeInstance

		zap.S().Debug("读取到的blockId为：", block)
		targetDataNodeIds := s.BlockToDataNodeIds[block]
		for _, dataNodeId := range targetDataNodeIds {
			zap.S().Debug("读取到的blockAddresses为：", blockAddresses)
			blockAddresses = append(blockAddresses, s.IdToDataNodes[dataNodeId])
		}

		res.NameNodeMetaDataList = append(res.NameNodeMetaDataList, NameNodeMetaData2PB(NameNodeMetaData{BlockId: block, BlockAddresses: blockAddresses}))
	}

	delete(s.FileNameToBlocks, modedFilePath)
	zap.S().Debug("删除文件后filemap为:", s.FileNameToBlocks)

	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.DeleteDataResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	return &res, nil
}

// StatData TODO 有些方法都是根据文件名获取block集合可以抽取成公共方法，方法名可以改为GetBlocksFromFileName
func (s *Service) StatData(c context.Context, req *namenode_pb.StatDataReq) (*namenode_pb.StatDataResp, error) {
	modedFilePath := util.ModFilePath(req.FileName)

	var res namenode_pb.StatDataResp

	_, ok := s.FileNameToBlocks[modedFilePath]
	if !ok {
		return nil, e.ErrFileDoesNotExist
	}
	fileBlocks := s.FileNameToBlocks[modedFilePath]

	for _, block := range fileBlocks {
		var blockAddresses []util.DataNodeInstance

		log.Println("读取到的blockId为：", block)
		targetDataNodeIds := s.BlockToDataNodeIds[block]
		for _, dataNodeId := range targetDataNodeIds {
			log.Println("读取到的blockAddresses为：", blockAddresses)
			blockAddresses = append(blockAddresses, s.IdToDataNodes[dataNodeId])
		}

		res.NameNodeMetaDataList = append(res.NameNodeMetaDataList, NameNodeMetaData2PB(NameNodeMetaData{BlockId: block, BlockAddresses: blockAddresses}))
	}

	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.StatDataResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	return &res, nil
}

func (s *Service) GetDataNodes(c context.Context, req *namenode_pb.GetDataNodesReq) (*namenode_pb.GetDataNodesResp, error) {
	var list []string
	for _, dni := range s.IdToDataNodes {
		list = append(list, dni.Host+":"+dni.ServicePort)
	}
	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.GetDataNodesResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	return &namenode_pb.GetDataNodesResp{
		DataNodeList: list,
	}, nil
}

func (s *Service) IsDir(c context.Context, req *namenode_pb.IsDirReq) (*namenode_pb.IsDirResp, error) {
	filename := util.ModPath(req.Filename)
	//空文件夹下面会有..文件夹
	dir := s.DirTree.FindSubDir(filename)
	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.IsDirResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	if len(dir) == 0 {
		return &namenode_pb.IsDirResp{Ok: false}, nil
	} else {
		return &namenode_pb.IsDirResp{Ok: true}, nil
	}
}

func (s *Service) Rename(c context.Context, req *namenode_pb.RenameReq) (*namenode_pb.RenameResp, error) {
	list := s.FileNameToBlocks[req.OldFileName]
	s.FileNameToBlocks[req.NewFileName] = list
	delete(s.FileNameToBlocks, req.OldFileName)
	zap.S().Debug(s.FileNameToBlocks)
	zap.S().Debugf("%v", s.DirTree)
	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.RenameResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	return &namenode_pb.RenameResp{
		Success: true,
	}, nil
}

func (s *Service) Mkdir(c context.Context, req *namenode_pb.MkdirReq) (*namenode_pb.MkdirResp, error) {
	path := util.ModPath(req.Path)
	ok := s.DirTree.Insert(path)
	if !ok {
		zap.S().Error("插入目录失败，请确认操作是否有误")
		return &namenode_pb.MkdirResp{}, errors.New("插入目录失败，请确认操作是否有误")
	}
	ok = s.DirTree.Insert(path + "../")
	if !ok {
		zap.S().Error("插入目录失败，请确认操作是否有误")
		return &namenode_pb.MkdirResp{}, errors.New("插入目录失败，请确认操作是否有误")
	}
	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.MkdirResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	return &namenode_pb.MkdirResp{}, nil
}

func (s *Service) List(c context.Context, req *namenode_pb.ListReq) (*namenode_pb.ListResp, error) {
	path := util.ModPath(req.ParentPath)
	dir := s.DirTree.FindSubDir(path)
	var dirNameList []string
	var fileNameList []string
	for _, str := range dir {
		resp, err := s.IsDir(context.Background(), &namenode_pb.IsDirReq{
			Filename: path + str + "/",
		})
		if err != nil {
			log.Println("NameNode IsDir Error:", err)
			return &namenode_pb.ListResp{}, err
		}
		if resp.Ok {
			//是目录
			dirNameList = append(dirNameList, str)
		} else {
			fileNameList = append(fileNameList, str)
		}
	}
	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.ListResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	return &namenode_pb.ListResp{
		FileName: fileNameList,
		DirName:  dirNameList,
	}, nil

}

func (s *Service) ReDirTree(c context.Context, req *namenode_pb.ReDirTreeReq) (*namenode_pb.ReDirTreeResp, error) {
	old := util.ModPath(req.OldPath)
	newPath := util.ModPath(req.NewPath)
	s.DirTree.Rename(s.DirTree.Root, old, newPath)
	log.Println("更名后的tree:", s.DirTree)
	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.ReDirTreeResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	return &namenode_pb.ReDirTreeResp{Success: true}, nil
}

func (s *Service) HeartBeat(c context.Context, req *namenode_pb.HeartBeatReq) (*namenode_pb.HeartBeatResp, error) {
	log.Println("receive heartbeat success:", req.Addr)
	s.DataNodeHeartBeat[req.Addr] = time.Now()
	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.HeartBeatResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	return &namenode_pb.HeartBeatResp{Success: true}, nil
}

func (s *Service) RegisterDataNode(c context.Context, req *namenode_pb.RegisterDataNodeReq) (*namenode_pb.RegisterDataNodeResp, error) {
	s.DataNodeHeartBeat[req.Addr] = time.Now()
	s.DataNodeMessageMap[req.Addr] = DataNodeMessage{
		UsedDisk:   req.UsedDisk,
		UsedMem:    req.UsedMem,
		TotalMem:   req.TotalMem,
		TotalDisk:  req.TotalDisk,
		CpuPercent: req.CpuPercent,
	}
	host, port, err := net.SplitHostPort(req.Addr)
	if err != nil {
		return &namenode_pb.RegisterDataNodeResp{}, err
	}
	s.IdToDataNodes[time.Now().Unix()] = util.DataNodeInstance{
		Host:        host,
		ServicePort: port,
	}
	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.RegisterDataNodeResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	return &namenode_pb.RegisterDataNodeResp{Success: true}, nil
}

func (s *Service) UpdateDataNodeMessage(c context.Context, req *namenode_pb.UpdateDataNodeMessageReq) (*namenode_pb.UpdateDataNodeMessageResp, error) {
	s.DataNodeMessageMap[req.Addr] = DataNodeMessage{
		UsedDisk:   req.UsedDisk,
		UsedMem:    req.UsedMem,
		TotalMem:   req.TotalMem,
		TotalDisk:  req.TotalDisk,
		CpuPercent: req.CpuPercent,
	}
	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.UpdateDataNodeMessageResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	return &namenode_pb.UpdateDataNodeMessageResp{Success: true}, nil
}

func (s *Service) JoinCluster(c context.Context, req *namenode_pb.JoinClusterReq) (*namenode_pb.JoinClusterResp, error) {
	log.Println("申请加入集群的节点信息为:", req.Id, " ", req.Addr)
	voter := s.RaftNode.AddVoter(raft.ServerID(req.Id), raft.ServerAddress(req.Addr), req.PreviousIndex, 0)
	if voter.Error() != nil {
		return &namenode_pb.JoinClusterResp{}, voter.Error()
	}
	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.JoinClusterResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	return &namenode_pb.JoinClusterResp{Success: true}, nil
}

func (s *Service) FindLeader(c context.Context, req *namenode_pb.FindLeaderReq) (*namenode_pb.FindLeaderResp, error) {
	id, _ := s.RaftNode.LeaderWithID()
	if id == "" {
		return &namenode_pb.FindLeaderResp{}, errors.New("cannot find leader")
	}
	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.FindLeaderResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	return &namenode_pb.FindLeaderResp{
		Addr: string(id),
	}, nil
}

func (s *Service) ECAssignDataNode(c context.Context, req *namenode_pb.ECAssignDataNodeReq) (*namenode_pb.ECAssignDataNodeResp, error) {
	s.FileNameToBlocks[req.Filename] = []string{}
	var metaDataList []*namenode_pb.NameNodeMetaData
	var dataNodesAvailable []int64
	for k, _ := range s.IdToDataNodes {
		dataNodesAvailable = append(dataNodesAvailable, k)
	}
	//已经被选的 data node, 不应该在被选择
	dataNodePresentMap := make(map[int64]struct{})
	for i := 0; i < int(req.DatanodeNumber); i++ {
		blockId := uuid.New().String()
		s.FileNameToBlocks[req.Filename] = append(s.FileNameToBlocks[req.Filename], blockId)
		var dataNodes []int64
		for k := range s.IdToDataNodes {
			_, ok := dataNodePresentMap[k]
			if !ok {
				dataNodes = append(dataNodes, k)
				dataNodePresentMap[k] = struct{}{}
				break
			}
		}
		var blockAddresses []*namenode_pb.DataNodeInstance
		for _, id := range dataNodes {
			blockAddresses = append(blockAddresses, &namenode_pb.DataNodeInstance{
				Host:        s.IdToDataNodes[id].Host,
				ServicePort: s.IdToDataNodes[id].ServicePort,
			})
		}
		s.BlockToDataNodeIds[blockId] = dataNodes
		metaDataList = append(metaDataList, &namenode_pb.NameNodeMetaData{
			BlockId:        blockId,
			BlockAddresses: blockAddresses,
		})
	}
	bytes, err := json.Marshal(s)
	if err != nil {
		log.Println("cannot marshal data")
		return &namenode_pb.ECAssignDataNodeResp{}, err
	}
	s.RaftNode.Apply(bytes, time.Second*1)
	return &namenode_pb.ECAssignDataNodeResp{
		NameNodeMetaData: metaDataList,
	}, nil
}
