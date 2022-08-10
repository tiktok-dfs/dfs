package client

import (
	"context"
	"go-fs/pkg/converter"
	"go-fs/pkg/util"
	dn "go-fs/proto/datanode"
	namenode_pb "go-fs/proto/namenode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"os"
)

func Put(nameNodeConn *grpc.ClientConn, sourceFilePath string, destFilePath string) bool {
	nameNodeInstance := namenode_pb.NewNameNodeServiceClient(nameNodeConn)

	fileSizeHandler, err := os.Stat(sourceFilePath)
	util.Check(err)

	// 拿到size为了给文件分片(block), 每个block会被分配到不同的data node中
	fileSize := uint64(fileSizeHandler.Size())

	fileName := destFilePath
	util.Check(err)

	namenodeWriteRequest := &namenode_pb.WriteRequest{FileName: fileName, FileSize: fileSize}

	// namenode 的 writeData并不是真的写入, 返回的reply包含每一个文件的block应该被写入的data node 的地址
	writeResponse, err := nameNodeInstance.WriteData(context.Background(), namenodeWriteRequest)
	util.Check(err)
	log.Println("NameNode里的写数据信息：", writeResponse.NameNodeMetaDataList)

	var blockSize uint64

	namenodeGetBlockSizeRequest := &namenode_pb.GetBlockSizeRequest{Request: true}

	blockSizeResponse, err := nameNodeInstance.GetBlockSize(context.Background(), namenodeGetBlockSizeRequest)
	util.Check(err)
	log.Println("NameNode里获取到的块大小：", blockSizeResponse.BlockSize)

	blockSize = blockSizeResponse.BlockSize

	fileHandler, err := os.Open(sourceFilePath)
	util.Check(err)

	// buffer 每次只读全局设定的block size 或 更少的数据
	dataStagingBytes := make([]byte, blockSize)
	putStatus := false
	for _, pbMetaData := range writeResponse.NameNodeMetaDataList {
		metaData := converter.Pb2NameNodeMetaData(pbMetaData)

		// n代表着实际读到的byte数
		n, err := fileHandler.Read(dataStagingBytes)
		util.Check(err)
		dataStagingBytes = dataStagingBytes[:n]

		blockId := metaData.BlockId
		blockAddresses := metaData.BlockAddresses

		startingDataNode := blockAddresses[0]
		remainingDataNodes := blockAddresses[1:]

		var datanodes []*dn.DataNodeInstance
		for _, dni := range remainingDataNodes {
			datanodes = append(datanodes, &dn.DataNodeInstance{
				Host:        dni.Host,
				ServicePort: dni.ServicePort,
			})
		}
		// data1 node 此时真正的准备写入数据
		dataNodeInstance, rpcErr := grpc.Dial(startingDataNode.Host+":"+startingDataNode.ServicePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		util.Check(rpcErr)
		defer dataNodeInstance.Close()

		request := dn.PutReq{
			Path:             sourceFilePath,
			BlockId:          blockId,
			Data:             string(dataStagingBytes),
			ReplicationNodes: datanodes,
		}
		// 写入数据
		log.Println("已经写入的BLockId为：", blockId)
		resp, rpcErr := dn.NewDataNodeClient(dataNodeInstance).Put(context.Background(), &request)
		util.Check(rpcErr)
		log.Println("put data success:", resp)
		if resp.Success {
			putStatus = true
			continue
		} else {
			putStatus = false
		}
	}
	return putStatus
}

func Get(nameNodeConn *grpc.ClientConn, sourceFilePath string) (fileContents string, getStatus bool) {
	nameNodeInstance := namenode_pb.NewNameNodeServiceClient(nameNodeConn)

	fileName := sourceFilePath

	nameNodeReadRequest := &namenode_pb.ReadRequst{FileName: fileName}

	// name node 并不是真的读数据, 返回的reply包含每一个文件的block 存放在data node 的地址
	readResponse, err := nameNodeInstance.ReadData(context.Background(), nameNodeReadRequest)
	util.Check(err)

	log.Println("调用NameNode读取数据的信息为：", readResponse.NameNodeMetaDataList)
	fileContents = ""

	for _, pbMetaData := range readResponse.NameNodeMetaDataList {

		//blockId := metaData.BlockId
		blockAddresses := pbMetaData.BlockAddresses
		// block被获取的标志位
		blockFetchStatus := false

		// 每一个block, 都被备份了x次, 但只需要拿一次.
		for _, selectedDataNode := range blockAddresses {
			// data1 node 此时真正的准备读数据
			dataNodeInstance, rpcErr := grpc.Dial(selectedDataNode.Host+":"+selectedDataNode.ServicePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if rpcErr != nil {
				continue
			}

			defer dataNodeInstance.Close()

			request := dn.GetReq{
				BlockId: pbMetaData.BlockId,
			}

			// 读数据
			resp, rpcErr := dn.NewDataNodeClient(dataNodeInstance).Get(context.Background(), &request)
			util.Check(rpcErr)
			// 追加内容
			fileContents += resp.Data
			// 读取成功后, 将标志位置为true, 此block不再获取
			blockFetchStatus = true
			break
		}

		// 如果一个block重试x次都没有拿到数据, 则返回文件没有get到
		if !blockFetchStatus {
			getStatus = false
			return
		}
	}

	// 所有block被拿到, 返回文件成功get到
	getStatus = true
	return
}
