package namenode

import (
	"context"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/opentrx/seata-golang/v2/pkg/client/tm"
	"go-fs/pkg/tree"
	"go-fs/pkg/util"
	namenode_pb "go-fs/proto/namenode"
	"log"
	"time"
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

// gRPC methods

// GetBlockSize 获取name node的block size
func (nn *Service) GetBlockSize(ctx context.Context, req *namenode_pb.GetBlockSizeRequest) (*namenode_pb.GetBlockSizeResponse, error) {
	proxyService := nn.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.GetBlockSize(ctx, req)
	if err != nil {
		return &namenode_pb.GetBlockSizeResponse{}, err
	}
	return resp, nil
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
	proxyService := nn.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.ReadData(ctx, req)
	if err != nil {
		return &namenode_pb.ReadResponse{}, err
	}
	return resp, nil
}

// WriteData 返回metadata, 包含写入文件的每一个block的id与data node的地址
func (nn *Service) WriteData(ctx context.Context, req *namenode_pb.WriteRequest) (*namenode_pb.WriteResponse, error) {
	proxyService := nn.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.WriteData(ctx, req)
	if err != nil {
		return &namenode_pb.WriteResponse{}, err
	}
	return resp, nil
}

func (nameNode *Service) ReDistributeData(request *ReDistributeDataRequest, reply *bool) error {
	proxyService := nameNode.GetProxyService()
	tm.Implement(proxyService)
	err := proxyService.ReDistributeData(request, reply)
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) DeleteData(c context.Context, req *namenode_pb.DeleteDataReq) (*namenode_pb.DeleteDataResp, error) {
	proxyService := s.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.DeleteData(c, req)
	if err != nil {
		return &namenode_pb.DeleteDataResp{}, err
	}
	return resp, nil
}

// StatData TODO 有些方法都是根据文件名获取block集合可以抽取成公共方法，方法名可以改为GetBlocksFromFileName
func (s *Service) StatData(c context.Context, req *namenode_pb.StatDataReq) (*namenode_pb.StatDataResp, error) {
	proxyService := s.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.StatData(c, req)
	if err != nil {
		return &namenode_pb.StatDataResp{}, err
	}
	return resp, nil
}

func (s *Service) GetDataNodes(c context.Context, req *namenode_pb.GetDataNodesReq) (*namenode_pb.GetDataNodesResp, error) {
	proxyService := s.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.GetDataNodes(c, req)
	if err != nil {
		return &namenode_pb.GetDataNodesResp{}, err
	}
	return resp, nil
}

func (s *Service) IsDir(c context.Context, req *namenode_pb.IsDirReq) (*namenode_pb.IsDirResp, error) {
	proxyService := s.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.IsDir(c, req)
	if err != nil {
		return &namenode_pb.IsDirResp{}, err
	}
	return resp, nil
}

func (s *Service) Rename(c context.Context, req *namenode_pb.RenameReq) (*namenode_pb.RenameResp, error) {
	proxyService := s.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.Rename(c, req)
	if err != nil {
		return &namenode_pb.RenameResp{}, err
	}
	return resp, nil
}

func (s *Service) Mkdir(c context.Context, req *namenode_pb.MkdirReq) (*namenode_pb.MkdirResp, error) {
	proxyService := s.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.Mkdir(c, req)
	if err != nil {
		return &namenode_pb.MkdirResp{}, err
	}
	return resp, nil
}

func (s *Service) List(c context.Context, req *namenode_pb.ListReq) (*namenode_pb.ListResp, error) {
	proxyService := s.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.List(c, req)
	if err != nil {
		return &namenode_pb.ListResp{}, err
	}
	return resp, nil

}

func (s *Service) ReDirTree(c context.Context, req *namenode_pb.ReDirTreeReq) (*namenode_pb.ReDirTreeResp, error) {
	proxyService := s.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.ReDirTree(c, req)
	if err != nil {
		return &namenode_pb.ReDirTreeResp{}, err
	}
	return resp, nil
}

func (s *Service) HeartBeat(c context.Context, req *namenode_pb.HeartBeatReq) (*namenode_pb.HeartBeatResp, error) {
	proxyService := s.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.HeartBeat(c, req)
	if err != nil {
		return &namenode_pb.HeartBeatResp{}, err
	}
	return resp, nil
}

/*func (s *Service) RegisterDataNode1(c context.Context, req *namenode_pb.RegisterDataNodeReq) (*namenode_pb.RegisterDataNodeResp, error) {
	proxyService := s.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.RegisterDataNode(c, req)
	if err != nil {
		log.Println(err)
		return &namenode_pb.RegisterDataNodeResp{}, err
	}
	return resp, nil
}*/

func (s *Service) RegisterDataNode(req namenode_pb.NameNodeService_RegisterDataNodeServer) error {
	proxyService := s.GetProxyService()
	tm.Implement(proxyService)
	err := proxyService.RegisterDataNode(req)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (s *Service) GetProxyService() *ProxyService {
	return &ProxyService{
		ProtoService: &ProtoService{
			Port:               s.Port,
			BlockSize:          s.BlockSize,
			ReplicationFactor:  s.ReplicationFactor,
			IdToDataNodes:      s.IdToDataNodes,
			FileNameToBlocks:   s.FileNameToBlocks,
			BlockToDataNodeIds: s.BlockToDataNodeIds,
			DataNodeMessageMap: s.DataNodeMessageMap,
			DataNodeHeartBeat:  s.DataNodeHeartBeat,
			DirTree:            s.DirTree,
			RaftNode:           s.RaftNode,
			RaftLog:            s.RaftLog,
		},
	}
}

func (s *Service) UpdateDataNodeMessage(c context.Context, req *namenode_pb.UpdateDataNodeMessageReq) (*namenode_pb.UpdateDataNodeMessageResp, error) {
	proxyService := s.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.UpdateDataNodeMessage(c, req)
	if err != nil {
		return &namenode_pb.UpdateDataNodeMessageResp{}, err
	}
	return resp, nil
}

/*func (s *Service) JoinCluster(c context.Context, req *namenode_pb.JoinClusterReq) (*namenode_pb.JoinClusterResp, error) {
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
*/
func (s *Service) ECAssignDataNode(c context.Context, req *namenode_pb.ECAssignDataNodeReq) (*namenode_pb.ECAssignDataNodeResp, error) {
	proxyService := s.GetProxyService()
	tm.Implement(proxyService)
	resp, err := proxyService.ECAssignDataNode(c, req)
	if err != nil {
		return &namenode_pb.ECAssignDataNodeResp{}, err
	}
	return resp, nil
}
