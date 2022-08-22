package datanode

import (
	"context"
	"fmt"
	"go-fs/datanode"
	"go-fs/pkg/util"
	dn "go-fs/proto/datanode"
	nn "go-fs/proto/namenode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func InitializeDataNodeUtil(nameNodeAddr string, serverPort int, dataLocation string) {
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

	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	for true {
		//向namenode注册
		log.Println("start register to name node:", nameNodeAddr)
		conn, err := grpc.Dial(nameNodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		usedMem, err := datanode.GetUsedMem()
		if err != nil {

		}
		usedDisk, err := datanode.GetUsedDisk()
		if err != nil {

		}
		totalMem, err := datanode.GetTotalMem()
		if err != nil {

		}
		totalDisk, err := datanode.GetTotalDisk()
		if err != nil {

		}
		cpuPercent, err := datanode.GetCpuPercent()
		if err != nil {

		}
		resp, err := nn.NewNameNodeServiceClient(conn).RegisterDataNode(context.Background(), &nn.RegisterDataNodeReq{
			Addr:       hostname + ":" + strconv.Itoa(int(dataNodeInstance.ServicePort)),
			UsedDisk:   usedDisk,
			UsedMem:    usedMem,
			TotalMem:   totalMem,
			TotalDisk:  totalDisk,
			CpuPercent: float32(cpuPercent),
		})
		if err != nil {
			continue
		}
		if resp.Success {
			log.Println("register success")
			break
		}
	}

	log.Println("DataNode daemon started on port: " + strconv.Itoa(serverPort))

	go heartBeatToNameNode(nameNodeAddr, dataNodeInstance, hostname)

	// graceful shutdown
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGKILL)

	<-sig

	server.GracefulStop()

}

func heartBeatToNameNode(addr string, instance *datanode.Server, hostname string) {
	i := 0
	for range time.Tick(time.Second * 5) {
		i++
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Println("moving to new name node")

		}
		client := nn.NewNameNodeServiceClient(conn)
		_, err = client.HeartBeat(context.Background(), &nn.HeartBeatReq{
			Addr: hostname + ":" + strconv.Itoa(int(instance.ServicePort)),
		})
		if err != nil {
			log.Println("moving to new name node")
		}
		//每150秒更新信息
		if i >= 30 {
			usedMem, err := datanode.GetUsedMem()
			if err != nil {

			}
			usedDisk, err := datanode.GetUsedDisk()
			if err != nil {

			}
			totalMem, err := datanode.GetTotalMem()
			if err != nil {

			}
			totalDisk, err := datanode.GetTotalDisk()
			if err != nil {

			}
			cpuPercent, err := datanode.GetCpuPercent()
			if err != nil {

			}
			resp, err := client.UpdateDataNodeMessage(context.Background(), &nn.UpdateDataNodeMessageReq{
				UsedDisk:   usedDisk,
				UsedMem:    usedMem,
				TotalMem:   totalMem,
				TotalDisk:  totalDisk,
				CpuPercent: float32(cpuPercent),
				Addr:       hostname + ":" + strconv.Itoa(int(instance.ServicePort)),
			})
			if err != nil {

			}
			if resp.Success {
				i = 0
			}
		}
	}
}
