package namenode

import (
	"go-fs/datanode"
	"go-fs/namenode"
	"go-fs/util"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"time"
)

func removeElementFromSlice(elements []string, index int) []string {
	return append(elements[:index], elements[index+1:]...)
}

// discoverDataNodes 发现 data node
func discoverDataNodes(nameNodeInstance *namenode.Service, listOfDataNodes *[]string) error {
	nameNodeInstance.IdToDataNodes = make(map[uint64]util.DataNodeInstance)

	var i int
	availableNumberOfDataNodes := len(*listOfDataNodes)
	// 如果传入的data node 为空, 则在本地7000-7050的端口中发现data node.
	if availableNumberOfDataNodes == 0 {
		log.Printf("No DataNodes specified, discovering ...\n")

		host := "localhost"
		serverPort := 7000

		pingRequest := datanode.NameNodePingRequest{Host: host, Port: nameNodeInstance.Port}
		var pingResponse datanode.NameNodePingResponse

		for serverPort < 7050 {
			dataNodeUri := host + ":" + strconv.Itoa(serverPort)
			dataNodeInstance, initErr := rpc.Dial("tcp", dataNodeUri)
			if initErr == nil {
				*listOfDataNodes = append(*listOfDataNodes, dataNodeUri)
				log.Printf("Discovered DataNode %s\n", dataNodeUri)

				pingErr := dataNodeInstance.Call("Service.Ping", pingRequest, &pingResponse)
				util.Check(pingErr)
				if pingResponse.Ack {
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

	err = rpc.Register(nameNodeInstance)
	util.Check(err)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(serverPort))
	util.Check(err)
	defer listener.Close()

	rpc.Accept(listener)

	log.Println("NameNode daemon started on port: " + strconv.Itoa(serverPort))
}

// heartbeatToDataNodes 每五秒钟, 进行健康检查
func heartbeatToDataNodes(listOfDataNodes []string, nameNode *namenode.Service) {
	for range time.Tick(time.Second * 5) {
		for i, hostPort := range listOfDataNodes {
			dataNodeClient, connectionErr := rpc.Dial("tcp", hostPort)

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
			var response bool
			hbErr := dataNodeClient.Call("Service.Heartbeat", true, &response)
			if hbErr != nil || !response {
				log.Printf("No heartbeat received from %s\n", hostPort)
				var reply bool
				reDistributeError := nameNode.ReDistributeData(&namenode.ReDistributeDataRequest{DataNodeUri: hostPort}, &reply)
				util.Check(reDistributeError)
				delete(nameNode.IdToDataNodes, uint64(i))
				listOfDataNodes = removeElementFromSlice(listOfDataNodes, i)
			}
		}
	}
}
