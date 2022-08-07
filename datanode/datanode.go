package datanode

import (
	"bufio"
	"errors"
	"go-fs/pkg/e"
	"go-fs/pkg/util"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
)

type Service struct {
	DataDirectory string
	ServicePort   uint16
	NameNodeHost  string
	NameNodePort  uint16
}

type DataNodePutRequest struct {
	FilePath         string
	BlockId          string
	Data             string
	ReplicationNodes []util.DataNodeInstance
}

type DataNodeGetRequest struct {
	//BlockId string
	FilePath string
}

type DataNodeWriteStatus struct {
	Status bool
}

type DataNodeData struct {
	Data string
}

type NameNodePingRequest struct {
	Host string
	Port uint16
}

type NameNodePingResponse struct {
	Ack bool
}

func (dataNode *Service) Ping(request *NameNodePingRequest, reply *NameNodePingResponse) error {
	dataNode.NameNodeHost = request.Host
	dataNode.NameNodePort = request.Port
	log.Printf("Received ping from NameNode, recorded as {NameNodeHost: %s, NameNodePort: %d}\n", dataNode.NameNodeHost, dataNode.NameNodePort)

	*reply = NameNodePingResponse{Ack: true}
	return nil
}

func (dataNode *Service) Heartbeat(request bool, response *bool) error {
	if request {
		log.Println("Received heartbeat from NameNode")
		*response = true
		return nil
	}
	return errors.New("HeartBeatError")
}

func (dataNode *Service) forwardForReplication(request *DataNodePutRequest, reply *DataNodeWriteStatus) error {
	blockId := request.BlockId
	blockAddresses := request.ReplicationNodes

	if len(blockAddresses) == 0 {
		return nil
	}

	startingDataNode := blockAddresses[0]
	remainingDataNodes := blockAddresses[1:]

	dataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
	util.Check(rpcErr)
	defer dataNodeInstance.Close()

	payloadRequest := DataNodePutRequest{
		FilePath:         request.FilePath,
		BlockId:          blockId,
		Data:             request.Data,
		ReplicationNodes: remainingDataNodes,
	}

	rpcErr = dataNodeInstance.Call("Service.PutData", payloadRequest, &reply)
	util.Check(rpcErr)
	return nil
}

func (dataNode *Service) PutData(request *DataNodePutRequest, reply *DataNodeWriteStatus) error {
	filePath, fileName := path.Split(path.Join(dataNode.DataDirectory, request.FilePath))
	filePathExist, err := util.PathExist(filePath)
	util.Check(err)

	if !filePathExist {
		err := os.MkdirAll(filePath, 0750)
		util.Check(err)
	}

	fileWriteHandler, err := os.Create(path.Join(filePath, fileName))
	util.Check(err)
	defer fileWriteHandler.Close()

	fileWriter := bufio.NewWriter(fileWriteHandler)
	_, err = fileWriter.WriteString(request.Data)
	util.Check(err)
	fileWriter.Flush()
	*reply = DataNodeWriteStatus{Status: true}

	return dataNode.forwardForReplication(request, reply)
}

func (dataNode *Service) GetData(request *DataNodeGetRequest, reply *DataNodeData) error {
	filePath := path.Join(dataNode.DataDirectory, request.FilePath)
	//dataBytes, err := ioutil.ReadFile(dataNode.DataDirectory + request.BlockId)
	filePathExist, err := util.PathExist(filePath)
	util.Check(err)

	if !filePathExist {
		return e.FileDoesNotExist
	}

	dataBytes, err := ioutil.ReadFile(filePath)
	util.Check(err)

	*reply = DataNodeData{Data: string(dataBytes)}
	return nil
}
