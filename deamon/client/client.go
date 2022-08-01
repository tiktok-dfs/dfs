package client

import (
	"go-fs/client"
	"go-fs/util"
	"log"
	"net"
	"net/rpc"
)

func PutHandler(nameNodeAddress string, sourceFilePath string, destFilePath string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Put(rpcClient, sourceFilePath, destFilePath)
}

func GetHandler(nameNodeAddress string, fileName string) (string, bool) {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Get(rpcClient, fileName)
}

func initializeClientUtil(nameNodeAddress string) (*rpc.Client, error) {
	host, port, err := net.SplitHostPort(nameNodeAddress)
	util.Check(err)

	log.Printf("NameNode to connect to is %s\n", nameNodeAddress)
	return rpc.Dial("tcp", host+":"+port)
}
