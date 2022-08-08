package client

import (
	"go-fs/client"
	"go-fs/pkg/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
)

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

func initializeClientUtil(nameNodeAddress string) (*grpc.ClientConn, error) {
	host, port, err := net.SplitHostPort(nameNodeAddress)
	util.Check(err)

	return grpc.Dial(host+":"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
