package client

import (
	"go-fs/datanode"
	"go-fs/namenode"
	"go-fs/util"
	"net/rpc"
	"os"
)

func Put(nameNodeInstance *rpc.Client, sourceFilePath string, destFilePath string) (putStatus bool) {
	fileSizeHandler, err := os.Stat(sourceFilePath)
	util.Check(err)

	// 拿到size为了给文件分片(block), 每个block会被分配到不同的data node中
	fileSize := uint64(fileSizeHandler.Size())
	request := namenode.NameNodeWriteRequest{FileName: sourceFilePath, FileSize: fileSize}
	var reply []namenode.NameNodeMetaData

	// namenode 的 writeData并不是真的写入, 返回的reply包含每一个文件的block应该被写入的data node 的地址
	err = nameNodeInstance.Call("Service.WriteData", request, &reply)
	util.Check(err)

	var blockSize uint64
	err = nameNodeInstance.Call("Service.GetBlockSize", true, &blockSize)
	util.Check(err)

	fileHandler, err := os.Open(sourceFilePath)
	util.Check(err)

	// buffer 每次只读全局设定的block size 或 更少的数据
	dataStagingBytes := make([]byte, blockSize)
	for _, metaData := range reply {
		// n代表着实际读到的byte数
		n, err := fileHandler.Read(dataStagingBytes)
		util.Check(err)
		dataStagingBytes = dataStagingBytes[:n]

		blockId := metaData.BlockId
		blockAddresses := metaData.BlockAddresses

		startingDataNode := blockAddresses[0]
		remainingDataNodes := blockAddresses[1:]

		// data node 此时真正的准备写入数据
		dataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
		util.Check(rpcErr)
		defer dataNodeInstance.Close()

		request := datanode.DataNodePutRequest{
			FilePath:         destFilePath,
			BlockId:          blockId,
			Data:             string(dataStagingBytes),
			ReplicationNodes: remainingDataNodes,
		}
		var reply datanode.DataNodeWriteStatus

		// 写入数据
		rpcErr = dataNodeInstance.Call("Service.PutData", request, &reply)
		util.Check(rpcErr)
		putStatus = true
	}
	return
}

func Get(nameNodeInstance *rpc.Client, fileName string) (fileContents string, getStatus bool) {
	request := namenode.NameNodeReadRequest{FileName: fileName}
	var reply []namenode.NameNodeMetaData

	// name node 并不是真的读数据, 返回的reply包含每一个文件的block 存放在data node 的地址
	err := nameNodeInstance.Call("Service.ReadData", request, &reply)
	util.Check(err)

	fileContents = ""

	for _, metaData := range reply {
		blockId := metaData.BlockId
		blockAddresses := metaData.BlockAddresses
		// block被获取的标志位
		blockFetchStatus := false

		// 每一个block, 都被备份了x次, 但只需要拿一次.
		for _, selectedDataNode := range blockAddresses {
			// data node 此时真正的准备读数据
			dataNodeInstance, rpcErr := rpc.Dial("tcp", selectedDataNode.Host+":"+selectedDataNode.ServicePort)
			if rpcErr != nil {
				continue
			}

			defer dataNodeInstance.Close()

			request := datanode.DataNodeGetRequest{
				BlockId: blockId,
			}
			var reply datanode.DataNodeData

			// 读数据
			rpcErr = dataNodeInstance.Call("Service.GetData", request, &reply)
			util.Check(rpcErr)
			// 追加内容
			fileContents += reply.Data
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
