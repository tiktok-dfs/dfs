package datanode

import (
	"bufio"
	"context"
	"errors"
	"go-fs/pkg/util"
	dn "go-fs/proto/datanode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io/ioutil"
	"log"
	"os"
)

type Server struct {
	dn.DataNodeServer
	DataDirectory string
	ServicePort   uint32
	NameNodeHost  string
	NameNodePort  uint32
}

func (s *Server) Ping(c context.Context, req *dn.PingReq) (*dn.PingResp, error) {
	//接收到NameNode的Ping请求
	s.NameNodeHost = req.Host
	s.NameNodePort = req.Port
	log.Println("I am alive")
	return &dn.PingResp{Success: true}, nil
}

func (s *Server) HeartBeat(c context.Context, req *dn.HeartBeatReq) (*dn.HeartBeatResp, error) {
	if req.Request {
		log.Println("receive heart beat success")
		return &dn.HeartBeatResp{Success: true}, nil
	}
	return nil, errors.New("HeartBeatError")
}

func (s *Server) forwardForReplication(request *dn.PutReq, reply *dn.PutResp) (*dn.PutResp, error) {
	blockId := request.BlockId
	blockAddresses := request.ReplicationNodes
	if len(blockAddresses) == 0 {
		log.Println("backup success")
		return &dn.PutResp{Success: true}, nil
	}

	startingDataNode := blockAddresses[0]
	log.Println("已经写入的blockAddress为:", startingDataNode)
	remainingDataNodes := blockAddresses[1:]
	log.Println("剩余可用的的blockAddress为:", remainingDataNodes)

	dataNodeInstance, rpcErr := grpc.Dial(startingDataNode.Host+":"+startingDataNode.ServicePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	util.Check(rpcErr)
	defer dataNodeInstance.Close()

	log.Println("backup data to another datanode:", startingDataNode.Host, startingDataNode.ServicePort)
	payloadRequest := dn.PutReq{
		Path:             request.Path,
		BlockId:          blockId,
		Data:             request.Data,
		ReplicationNodes: remainingDataNodes,
	}
	resp, rpcErr := dn.NewDataNodeClient(dataNodeInstance).Put(context.Background(), &payloadRequest)
	util.Check(rpcErr)
	return resp, nil
}

func (s *Server) Put(c context.Context, req *dn.PutReq) (*dn.PutResp, error) {
	log.Println("写入blockId为", req.BlockId)
	fileWriteHandler, err := os.Create(s.DataDirectory + req.BlockId)
	if err != nil {
		log.Println("data directory create error:", err)
	}
	log.Println("data directory create success")
	util.Check(err)
	defer fileWriteHandler.Close()

	fileWriter := bufio.NewWriter(fileWriteHandler)
	_, err = fileWriter.WriteString(req.Data)
	util.Check(err)
	fileWriter.Flush()
	resp := dn.PutResp{Success: true}
	replication, err := s.forwardForReplication(req, &resp)
	return replication, nil
}

func (s *Server) Get(c context.Context, req *dn.GetReq) (*dn.GetResp, error) {
	log.Println("读取的BlockId为：", req.BlockId)
	dataBytes, err := ioutil.ReadFile(s.DataDirectory + req.BlockId)
	if err != nil {
		return &dn.GetResp{}, err
	}
	return &dn.GetResp{Data: string(dataBytes)}, nil
}

func (s *Server) Delete(c context.Context, req *dn.DeleteReq) (*dn.DeleteResp, error) {
	_, err := os.Open(s.DataDirectory + req.BlockId)
	if err != nil {
		log.Println("文件已经被删掉")
		return &dn.DeleteResp{Success: true}, nil
	}
	err = os.Remove(s.DataDirectory + req.BlockId)
	if err != nil {
		return &dn.DeleteResp{}, err
	}
	log.Println("成功删除文件")
	return &dn.DeleteResp{Success: true}, nil
}
