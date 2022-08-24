package client

import (
	"context"
	"errors"
	"go-fs/client"
	"go-fs/pkg/util"
	nn "go-fs/proto/namenode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"strings"
	"time"
)

type Service struct {
	NameNodeHost string
	NameNodePort string
}

func PutHandler(nameNodeAddress string, sourceFilePath string, destFilePath string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Put(rpcClient, sourceFilePath, destFilePath)
}

func GetHandler(nameNodeAddress string, sourceFilePath string) (string, bool) {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Get(rpcClient, sourceFilePath)
}

func DeleteHandler(nameNodeAddress string, sourceFilePath string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Delete(rpcClient, sourceFilePath)
}

func StatHandler(nameNodeAddress string, sourceFilePath string) (*client.StatResp, error) {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Stat(rpcClient, sourceFilePath)
}

func MkdirHandler(nameNodeAddress string, sourceFilePath string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Mkdir(rpcClient, sourceFilePath)
}

func RenameHandle(nameNodeAddress string, remoteFilePath string, renameDestPath string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Rename(rpcClient, remoteFilePath, renameDestPath)
}

func ListHandler(nameNodeAddress string, parentPath string) (*client.ListResp, error) {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.List(rpcClient, parentPath)
}

func initializeClientUtil(nameNodeAddress string) (*grpc.ClientConn, error) {
	s := new(Service)
	go listenLeader(s, nameNodeAddress)
	for true {
		if s.NameNodeHost != "" {
			break
		}
	}
	return grpc.Dial(s.NameNodeHost+":"+s.NameNodePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func listenLeader(s *Service, address string) {
	for range time.Tick(time.Second * 1) {
		nameNodes := strings.Split(address, ",")
		for _, n := range nameNodes {
			conn, err := grpc.Dial(n, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				//表明连接不上，继续遍历节点
				continue
			}
			resp, err := nn.NewNameNodeServiceClient(conn).FindLeader(context.Background(), &nn.FindLeaderReq{})
			if err != nil {
				continue
			}
			host, port, err := net.SplitHostPort(resp.Addr)
			if err != nil {
				panic(err)
			}
			s.NameNodeHost = host
			s.NameNodePort = port
		}
		if s.NameNodePort == "" {
			err := errors.New("there is no alive name node")
			if err != nil {
				panic(err)
			}
		}
	}
}
