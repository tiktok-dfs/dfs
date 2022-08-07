package datanode

import (
	"errors"
	"go-fs/datanode"
	"go-fs/pkg/util"
	"log"
	"net"
	"net/rpc"
	"strconv"
)

func InitializeDataNodeUtil(serverPort int, dataLocation string) {
	dataNodeInstance := new(datanode.Service)
	dataNodeInstance.DataDirectory = dataLocation
	dataNodeInstance.ServicePort = uint16(serverPort)

	log.Printf("Data storage location is %s\n", dataLocation)

	err := rpc.Register(dataNodeInstance)
	util.Check(err)

	rpc.HandleHTTP()

	// 创建监听器, 如果端口被占用, 则端口号+1, 直至找到空闲端口
	var listener net.Listener
	initErr := errors.New("init")

	for initErr != nil {
		listener, initErr = net.Listen("tcp", ":"+strconv.Itoa(serverPort))
		serverPort += 1
	}
	log.Printf("DataNode port is %d\n", serverPort-1)
	defer listener.Close()

	rpc.Accept(listener)

	log.Println("DataNode daemon started on port: " + strconv.Itoa(serverPort))
}
