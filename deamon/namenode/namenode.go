package namenode

import (
	"context"
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
	"syscall"
	"time"
)

var RaftCur *raft.Raft

func InitializeNameNodeUtil(follow string, raftId string, serverPort int, blockSize int, replicationFactor int) {

	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	log.Printf("BlockSize is %d\n", blockSize)
	log.Printf("Replication Factor is %d\n", replicationFactor)
	log.Printf("NameNode port is %d\n", serverPort)

	addr := ":" + strconv.Itoa(serverPort)

	listener, err := net.Listen("tcp", addr)
	util.Check(err)

	var fsm RaftMessage
	raftNode, tm, err := NewRaft(follow, context.Background(), raftId, hostname+addr, &fsm)
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
			log.Println("节点名称:", hostname, "raftNode:", raftNode)
		}
	}(raftNode)

	// graceful shutdown
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGKILL)

	<-sig

	server.GracefulStop()

}

func NewRaft(follow string, ctx context.Context, myID, myAddress string, fsm raft.FSM) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	isLeader := make(chan bool, 1)
	c.NotifyCh = isLeader
	c.HeartbeatTimeout = time.Second * 5
	c.ElectionTimeout = time.Second * 10
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

	log.Println("follow:", follow)
	if follow == "" {
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
	} else {
		conn, err := grpc.Dial(follow, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		resp, err := namenode_pb.NewNameNodeServiceClient(conn).JoinCluster(context.Background(), &namenode_pb.JoinClusterReq{
			Id:            myID,
			Addr:          myAddress,
			PreviousIndex: 0,
		})
		if err != nil {
			return nil, nil, err
		}
		if resp.Success {
			log.Println("join the raft cluster success")
		}
	}
	return r, tm, nil
}
