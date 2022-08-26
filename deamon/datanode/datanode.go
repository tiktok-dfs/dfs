package datanode

import (
	"context"
	"errors"
	"fmt"
	"go-fs/datanode"
	"go-fs/pkg/util"
	dn "go-fs/proto/datanode"
	nn "go-fs/proto/namenode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func InitializeDataNodeUtil(host, nameNodeAddr string, serverPort int, dataLocation string) {
	dataNodeInstance := new(datanode.Server)
	if !strings.HasSuffix(dataLocation, "/") {
		dataLocation = dataLocation + "/"
	}
	dataNodeInstance.DataDirectory = dataLocation
	dataNodeInstance.ServicePort = uint32(serverPort)

	go listenLeader(nameNodeAddr, dataNodeInstance)

	log.Printf("Data storage location is %s\n", dataLocation)

	addr := ":" + strconv.Itoa(serverPort)

	listener, err := net.Listen("tcp", addr)
	util.Check(err)

	server := grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle: 5 * time.Second,
	}))
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
	if host == "" {
		//为了方便Windows下调试
		hostname = "localhost"
	}
	log.Println("start register to name nodes:", nameNodeAddr)
	for true {
		//向NameNode注册
		if dataNodeInstance.NameNodeHost == "" {
			continue
		}
		addr := dataNodeInstance.NameNodeHost + ":" + strconv.Itoa(int(dataNodeInstance.NameNodePort))
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
		resp, err := nn.NewNameNodeServiceClient(conn).RegisterDataNode(context.Background())
		if err != nil {
			continue
		}
		err = resp.Send(&nn.RegisterDataNodeReq{
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

	}

	log.Println("DataNode daemon started on port: " + strconv.Itoa(serverPort))

	go heartBeatToNameNode(dataNodeInstance, hostname)

	// graceful shutdown
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGKILL)

	<-sig

	server.GracefulStop()

}

func listenLeader(addr string, instance *datanode.Server) {
	for range time.Tick(time.Second * 1) {
		nameNodes := strings.Split(addr, ",")
		for _, n := range nameNodes {
			conn, err := grpc.Dial(n, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				//表明连接不上，继续遍历节点
				continue
			}
			resp, err := nn.NewNameNodeServiceClient(conn).FindLeader(context.Background())
			if err != nil {
				continue
			}
			err = resp.Send(&nn.FindLeaderReq{})
			if err != nil {
				continue
			}
			leaderResp, err := resp.Recv()
			if err != nil {
				continue
			}
			host, port, err := net.SplitHostPort(leaderResp.Addr)
			if err != nil {
				panic(err)
			}
			instance.NameNodeHost = host
			p, err := strconv.Atoi(port)
			if err != nil {
				panic(err)
			}
			instance.NameNodePort = uint32(p)
			break
		}
		if instance.NameNodeHost == "" {
			err := errors.New("there is no alive name node")
			if err != nil {
				panic(err)
			}
		}
		log.Println("DataNode信息为:", instance)
	}
}

func heartBeatToNameNode(instance *datanode.Server, hostname string) {
	i := 0
	for range time.Tick(time.Second * 5) {
		i++
		addr := instance.NameNodeHost + ":" + strconv.Itoa(int(instance.NameNodePort))
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
