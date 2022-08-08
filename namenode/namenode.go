package namenode

import (
	"context"
	"go-fs/datanode"
	"go-fs/pkg/e"
	"go-fs/pkg/util"
	namenode_pb "go-fs/proto/namenode"
	"log"
	"math"
	"math/rand"
	"net/rpc"
	"strings"

	"github.com/google/uuid"
)

type NameNodeMetaData struct {
	BlockId        string
	BlockAddresses []util.DataNodeInstance
}

type NameNodeReadRequest struct {
	FileName string
}

type NameNodeWriteRequest struct {
	FileName string
	FileSize uint64
}

type ReDistributeDataRequest struct {
	DataNodeUri string
}

type UnderReplicatedBlocks struct {
	BlockId           string
	HealthyDataNodeId uint64
}

type Service struct {
	namenode_pb.UnimplementedNameNodeServiceServer

	Port               uint16
	BlockSize          uint64
	ReplicationFactor  uint64
	IdToDataNodes      map[uint64]util.DataNodeInstance
	FileNameToBlocks   map[string][]string
	BlockToDataNodeIds map[string][]uint64
}

func NewService(blockSize uint64, replicationFactor uint64, serverPort uint16) *Service {
	return &Service{
		Port:               serverPort,
		BlockSize:          blockSize,
		ReplicationFactor:  replicationFactor,
		FileNameToBlocks:   make(map[string][]string),
		IdToDataNodes:      make(map[uint64]util.DataNodeInstance),
		BlockToDataNodeIds: make(map[string][]uint64),
	}
}

// selectRandomDataNodes
func selectRandomDataNodes(availableDataNodes []uint64, replicationFactor uint64) (randomSeletctedDataNodes []uint64) {
	//已经被选的 data node, 不应该在被选择
	dataNodePresentMap := make(map[uint64]struct{})
	//随机选择备份的data node
	for i := uint64(0); i < replicationFactor; {
		chosenItem := availableDataNodes[rand.Intn(len(availableDataNodes))]
		if _, ok := dataNodePresentMap[chosenItem]; !ok {
			dataNodePresentMap[chosenItem] = struct{}{}
			randomSeletctedDataNodes = append(randomSeletctedDataNodes, chosenItem)
			i++
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
	var res namenode_pb.ReadResponse

	_, ok := nn.FileNameToBlocks[req.FileName]
	if !ok {
		return nil, e.FileDoesNotExist
	}
	fileBlocks := nn.FileNameToBlocks[req.FileName]

	for _, block := range fileBlocks {
		var blockAddresses []util.DataNodeInstance

		targetDataNodeIds := nn.BlockToDataNodeIds[block]
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nn.IdToDataNodes[dataNodeId])
		}

		res.NameNodeMetaDataList = append(res.NameNodeMetaDataList, NameNodeMetaData2PB(NameNodeMetaData{BlockId: block, BlockAddresses: blockAddresses}))
	}

	return &res, nil
}

// WriteData 返回metadata, 包含写入文件的每一个block的id与data node的地址
func (nn *Service) WriteData(ctx context.Context, req *namenode_pb.WriteRequest) (*namenode_pb.WriteResponse, error) {
	var res namenode_pb.WriteResponse

	nn.FileNameToBlocks[req.FileName] = []string{}

	numberOfBlocksToAllocate := uint64(math.Ceil(float64(req.FileSize) / float64(nn.BlockSize)))

	nameNodeMetaDataList := nn.allocateBlocks(req.FileName, numberOfBlocksToAllocate)

	for _, nnmd := range nameNodeMetaDataList {
		res.NameNodeMetaDataList = append(res.NameNodeMetaDataList, NameNodeMetaData2PB(nnmd))
	}
	return &res, nil
}

func (nameNode *Service) allocateBlocks(fileName string, numberOfBlocks uint64) (metadata []NameNodeMetaData) {
	//创建写入文件的 slot
	nameNode.FileNameToBlocks[fileName] = []string{}

	// 获取所有 data nodes 的id
	var dataNodesAvailable []uint64
	for k, _ := range nameNode.IdToDataNodes {
		dataNodesAvailable = append(dataNodesAvailable, k)
	}
	dataNodesAvailableCount := uint64(len(dataNodesAvailable))

	for i := uint64(0); i < numberOfBlocks; i++ {
		//添加 block 的 slot
		blockId := uuid.New().String()
		nameNode.FileNameToBlocks[fileName] = append(nameNode.FileNameToBlocks[fileName], blockId)

		//每一个block进行备份
		//如果全局设置的备份个数 > data node 的个数, 则只备份文件 data node 个
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

		// 创建此次写入的 meta data
		metadata = append(metadata, NameNodeMetaData{BlockId: blockId, BlockAddresses: blockAddresses})
	}
	return
}

func (nameNode *Service) assignDataNodes(blockId string, dataNodesAvailable []uint64, replicationFactor uint64) []uint64 {
	// 随机选择block备份的data nodes
	targetDataNodeIds := selectRandomDataNodes(dataNodesAvailable, replicationFactor)
	nameNode.BlockToDataNodeIds[blockId] = targetDataNodeIds
	return targetDataNodeIds
}

func (nameNode *Service) ReDistributeData(request *ReDistributeDataRequest, reply *bool) error {
	log.Printf("DataNode %s is dead, trying to redistribute data\n", request.DataNodeUri)
	deadDataNodeSlice := strings.Split(request.DataNodeUri, ":")
	var deadDataNodeId uint64

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
				// TODO: trigger data deletion on the existing data nodes
				break
			}
		}
	}

	// verify if re-replication would be possible
	if len(nameNode.IdToDataNodes) < int(nameNode.ReplicationFactor) {
		log.Println("Replication not possible due to unavailability of sufficient DataNode(s)")
		return nil
	}

	var availableNodes []uint64
	for k, _ := range nameNode.IdToDataNodes {
		availableNodes = append(availableNodes, k)
	}

	// attempt re-replication of under-replicated blocks
	for _, blockToReplicate := range underReplicatedBlocksList {

		// fetch the data from the healthy DataNode
		healthyDataNode := nameNode.IdToDataNodes[blockToReplicate.HealthyDataNodeId]
		dataNodeInstance, rpcErr := rpc.Dial("tcp", healthyDataNode.Host+":"+healthyDataNode.ServicePort)
		if rpcErr != nil {
			continue
		}

		defer dataNodeInstance.Close()

		// 找到要迁移的block的文件路径
		var filePath string
		var foundFilePath bool

		for fp, blockIds := range nameNode.FileNameToBlocks {
			for _, blockId := range blockIds {
				if blockToReplicate.BlockId == blockId {
					filePath = fp
					foundFilePath = true
					break
				}
			}

			if foundFilePath {
				break
			} else {
				log.Println("Cannot found filePath for a UnderReplicatedBlock, reDistributeData failed")
			}
		}

		getRequest := datanode.DataNodeGetRequest{
			//BlockId: blockToReplicate.BlockId,
			FilePath: filePath,
		}
		var getReply datanode.DataNodeData

		rpcErr = dataNodeInstance.Call("Service.GetData", getRequest, &getReply)
		util.Check(rpcErr)
		blockContents := getReply.Data

		// initiate the replication of the block contents
		targetDataNodeIds := nameNode.assignDataNodes(blockToReplicate.BlockId, availableNodes, nameNode.ReplicationFactor)
		var blockAddresses []util.DataNodeInstance
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}
		startingDataNode := blockAddresses[0]
		remainingDataNodes := blockAddresses[1:]

		targetDataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
		util.Check(rpcErr)
		defer targetDataNodeInstance.Close()

		putRequest := datanode.DataNodePutRequest{
			BlockId:          blockToReplicate.BlockId,
			Data:             blockContents,
			ReplicationNodes: remainingDataNodes,
		}
		var putReply datanode.DataNodeWriteStatus

		rpcErr = targetDataNodeInstance.Call("Service.PutData", putRequest, &putReply)
		util.Check(rpcErr)

		log.Printf("Block %s replication completed for %+v\n", blockToReplicate.BlockId, targetDataNodeIds)
	}

	return nil
}
