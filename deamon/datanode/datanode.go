package datanode

import (
	"fmt"
	"go-fs/datanode"
	"go-fs/pkg/util"
	dn "go-fs/proto/datanode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func InitializeDataNodeUtil(serverPort int, dataLocation string) {
	dataNodeInstance := new(datanode.Server)
	dataNodeInstance.DataDirectory = dataLocation
	dataNodeInstance.ServicePort = uint32(serverPort)

	log.Printf("Data storage location is %s\n", dataLocation)

	addr := ":" + strconv.Itoa(serverPort)

	listener, err := net.Listen("tcp", addr)
	util.Check(err)

	server := grpc.NewServer()
	dn.RegisterDataNodeServer(server, dataNodeInstance)

	grpc_health_v1.RegisterHealthServer(server, health.NewServer())

	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf(fmt.Sprintf("Server Serve failed in %s", addr), "err", err.Error())
			panic(err)
		}
	}()

	log.Println("DataNode daemon started on port: " + strconv.Itoa(serverPort))

	// graceful shutdown
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGKILL)

	<-sig

	server.GracefulStop()

}
