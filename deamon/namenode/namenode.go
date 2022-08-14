package namenode

import (
	"context"
	"fmt"
	"go-fs/namenode"
	"go-fs/pkg/util"
	dn "go-fs/proto/datanode"
	namenode_pb "go-fs/proto/namenode"
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

func removeElementFromSlice(elements []string, index int) []string {
	return append(elements[:index], elements[index+1:]...)
}

// discoverDataNodes 发现 data1 node
func discoverDataNodes(nameNodeInstance *namenode.Service, listOfDataNodes *[]string) error {
	nameNodeInstance.IdToDataNodes = make(map[uint64]util.DataNodeInstance)

	var i int
	availableNumberOfDataNodes := len(*listOfDataNodes)
	// 如果传入的data node 为空, 则在本地7000-7050的端口中发现data node.
	if availableNumberOfDataNodes == 0 {
		log.Printf("No DataNodes specified, discovering ...\n")

		host := "localhost"
		serverPort := 7000

		pingRequest := dn.PingReq{
			Host: host,
			Port: uint32(nameNodeInstance.Port),
		}

		for serverPort < 7050 {
			dataNodeUri := host + ":" + strconv.Itoa(serverPort)
			dataNodeInstance, initErr := grpc.Dial(dataNodeUri, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if initErr == nil {
				*listOfDataNodes = append(*listOfDataNodes, dataNodeUri)
				log.Printf("Discovered DataNode %s\n", dataNodeUri)

				resp, pingErr := dn.NewDataNodeClient(dataNodeInstance).Ping(context.Background(), &pingRequest)
				util.Check(pingErr)
				if resp.Success {
					log.Printf("Ack received from %s\n", dataNodeUri)
				} else {
					log.Printf("No ack received from %s\n", dataNodeUri)
				}
			}
			serverPort += 1
		}

	}

	// 将有效的data node 添加到 name node 中
	availableNumberOfDataNodes = len(*listOfDataNodes)
	for i = 0; i < availableNumberOfDataNodes; i++ {
		host, port, err := net.SplitHostPort((*listOfDataNodes)[i])
		util.Check(err)
		dataNodeInstance := util.DataNodeInstance{Host: host, ServicePort: port}
		nameNodeInstance.IdToDataNodes[uint64(i)] = dataNodeInstance
	}

	return nil
}

func InitializeNameNodeUtil(serverPort int, blockSize int, replicationFactor int, listOfDataNodes []string) {
	nameNodeInstance := namenode.NewService(uint64(blockSize), uint64(replicationFactor), uint16(serverPort))

	err := discoverDataNodes(nameNodeInstance, &listOfDataNodes)
	util.Check(err)

	log.Printf("BlockSize is %d\n", blockSize)
	log.Printf("Replication Factor is %d\n", replicationFactor)
	log.Printf("List of DataNode(s) in service is %q\n", listOfDataNodes)
	log.Printf("NameNode port is %d\n", serverPort)

	go heartbeatToDataNodes(listOfDataNodes, nameNodeInstance)

	addr := ":" + strconv.Itoa(serverPort)

	listener, err := net.Listen("tcp", addr)
	util.Check(err)

	server := grpc.NewServer()
	namenode_pb.RegisterNameNodeServiceServer(server, nameNodeInstance)

	grpc_health_v1.RegisterHealthServer(server, health.NewServer())

	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf(fmt.Sprintf("Server Serve failed in %s", addr), "err", err.Error())
			panic(err)
		}
	}()

	log.Println("NameNode daemon started on port: " + strconv.Itoa(serverPort))

	// graceful shutdown
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGKILL)

	<-sig

	server.GracefulStop()

}

// heartbeatToDataNodes 每五秒钟, 进行健康检查
func heartbeatToDataNodes(listOfDataNodes []string, nameNode *namenode.Service) {
	for range time.Tick(time.Second * 5) {
		for i, hostPort := range listOfDataNodes {
			dataNodeClient, connectionErr := grpc.Dial(hostPort, grpc.WithTransportCredentials(insecure.NewCredentials()))

			// 如果连接失败, 进行迁移
			if connectionErr != nil {
				log.Printf("Unable to connect to node %s\n", hostPort)
				var reply bool
				reDistributeError := nameNode.ReDistributeData(&namenode.ReDistributeDataRequest{DataNodeUri: hostPort}, &reply)
				util.Check(reDistributeError)
				delete(nameNode.IdToDataNodes, uint64(i))
				listOfDataNodes = removeElementFromSlice(listOfDataNodes, i)
				continue
			}

			// 如果心跳检测失败, 进行迁移
			response, hbErr := dn.NewDataNodeClient(dataNodeClient).HeartBeat(context.Background(), &dn.HeartBeatReq{
				Request: true,
			})
			if hbErr != nil || !response.Success {
				log.Printf("No heartbeat received from %s\n", hostPort)
				var reply bool
				reDistributeError := nameNode.ReDistributeData(&namenode.ReDistributeDataRequest{DataNodeUri: hostPort}, &reply)
				util.Check(reDistributeError)
				delete(nameNode.IdToDataNodes, uint64(i))
				listOfDataNodes = removeElementFromSlice(listOfDataNodes, i)
			}

			//心跳成功，更新map
			nameNode.DataNodeMessageMap[hostPort] = namenode.DataNodeMessage{
				UsedDisk:   response.UsedDisk,
				UsedMem:    response.UsedMem,
				CpuPercent: response.CpuPercent,
				TotalMem:   response.TotalMem,
				TotalDisk:  response.TotalDisk,
			}
		}
	}
}
