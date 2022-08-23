package namenode

import (
	"context"
	"errors"
	"fmt"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raftadmin"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"go-fs/common/config"
	"go-fs/namenode"
	"go-fs/pkg/util"
	namenode_pb "go-fs/proto/namenode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func InitializeNameNodeUtil(host string, master bool, follow, raftId string, serverPort int, blockSize int, replicationFactor int) {

	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	if host == "" {
		//为了方便Windows下调试
		hostname = "localhost"
	}

	log.Printf("BlockSize is %d\n", blockSize)
	log.Printf("Replication Factor is %d\n", replicationFactor)
	log.Printf("NameNode port is %d\n", serverPort)

	addr := ":" + strconv.Itoa(serverPort)

	listener, err := net.Listen("tcp", addr)
	util.Check(err)

	var fsm RaftMessage
	raftNode, tm, err := newRaft(master, follow, raftId, hostname+addr, &fsm)
	if err != nil {
		log.Println("start raft cluster fail:", err)
	}
	nameNodeInstance := namenode.NewService(raftNode, uint64(blockSize), uint64(replicationFactor), uint16(serverPort))
	server := grpc.NewServer()
	namenode_pb.RegisterNameNodeServiceServer(server, nameNodeInstance)
	tm.Register(server)
	raftadmin.Register(server, raftNode)
	reflection.Register(server)

	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf(fmt.Sprintf("Server Serve failed in %s", addr), "err", err.Error())
			panic(err)
		}
	}()

	log.Println("NameNode daemon started on port: " + strconv.Itoa(serverPort))

	go func(raftNode *raft.Raft) {
		for range time.Tick(5 * time.Second) {
			if raftNode == nil {
				log.Println("节点加入有误，raft未建立成功")
				continue
			}
			id, serverID := raftNode.LeaderWithID()
			log.Println("当前主节点名称:", id, "host:", serverID)
		}
	}(raftNode)

	// 监测datanode的心跳
	go checkDataNode(nameNodeInstance)

	// graceful shutdown
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGKILL)

	<-sig

	server.GracefulStop()

}

func checkDataNode(instance *namenode.Service) {
	for range time.Tick(time.Millisecond * 1) {
		if instance.IdToDataNodes == nil {
			continue
		}
		for k, v := range instance.IdToDataNodes {
			addr := v.Host + ":" + v.ServicePort
			lastHeartBeatTime := instance.DataNodeHeartBeat[addr]
			if lastHeartBeatTime.Add(time.Second*5).Unix() < time.Now().Unix() {
				var reply bool
				reDistributeError := instance.ReDistributeData(&namenode.ReDistributeDataRequest{DataNodeUri: addr}, &reply)
				util.Check(reDistributeError)
				delete(instance.IdToDataNodes, k)
			}
		}
	}
}

func newRaft(master bool, follow, myID, myAddress string, fsm raft.FSM) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	isLeader := make(chan bool, 1)
	c.NotifyCh = isLeader
	c.LocalID = raft.ServerID(myID)

	baseDir := filepath.Join(config.RaftCfg.RaftDataDir, myID)

	//重启的时候如果存在该文件夹会报错，需要注意是否要删除文件夹再重新运行，为了考虑数据安全问题，未在代码里强制删除
	err := os.MkdirAll(baseDir, 0755)
	if err != nil {
		return nil, nil, err
	}
	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err)
	}

	tm := transport.New(raft.ServerAddress(myAddress), []grpc.DialOption{grpc.WithInsecure()})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, fmt.Errorf("raft.NewRaft: %v", err)
	}

	log.Println("master", master)
	if master {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(myID),
					Address:  raft.ServerAddress(myAddress),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, nil, fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
		}
	} else if follow != "" {
		leader, err := FindLeader(follow)
		if err != nil {
			return nil, nil, err
		}
		conn, err := grpc.Dial(leader, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		resp, err := namenode_pb.NewNameNodeServiceClient(conn).JoinCluster(context.Background(), &namenode_pb.JoinClusterReq{
			Addr:          myAddress,
			Id:            myID,
			PreviousIndex: 0,
		})
		if err != nil {
			return nil, nil, err
		}
		if resp.Success {
			log.Println("join the cluster success")
		}
	}
	return r, tm, nil
}

// FindLeader 查找NameNode的Raft集群的Leader
func FindLeader(addrList string) (string, error) {
	nameNodes := strings.Split(addrList, ",")
	var res = ""
	for _, n := range nameNodes {
		conn, err := grpc.Dial(n, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			//表明连接不上，继续遍历节点
			continue
		}
		resp, err := namenode_pb.NewNameNodeServiceClient(conn).FindLeader(context.Background(), &namenode_pb.FindLeaderReq{})
		if err != nil {
			continue
		}
		res = resp.Addr
		break
	}
	if res == "" {
		return "", errors.New("there is no alive name node")
	}
	return res, nil
}
