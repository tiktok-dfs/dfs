package datanode

import (
	"bufio"
	"context"
	"errors"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	"go-fs/pkg/util"
	dn "go-fs/proto/datanode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io/ioutil"
	"log"
	"os"
	"time"
)

type Server struct {
	dn.DataNodeServer
	DataDirectory string
	ServicePort   uint32
	NameNodeHost  string
	NameNodePort  uint32
}

func (s *Server) Ping(c context.Context, req *dn.PingReq) (*dn.PingResp, error) {
	//接收到NameNode的Ping请求
	s.NameNodeHost = req.Host
	s.NameNodePort = req.Port
	log.Println("I am alive")
	return &dn.PingResp{Success: true}, nil
}

func (s *Server) HeartBeat(c context.Context, req *dn.HeartBeatReq) (*dn.HeartBeatResp, error) {
	if req.Request {
		log.Println("receive heart beat success")
		diskPercent, err := GetDiskPercent()
		if err != nil {
			log.Println("cannot GetDiskPercent:", err)
		}
		memPercent, err := GetMemPercent()
		if err != nil {
			log.Println("cannot GetMemPercent:", err)
		}
		cpuPercent, err := GetCpuPercent()
		if err != nil {
			log.Println("cannot GetCpuPercent:", err)
		}
		return &dn.HeartBeatResp{
			Success:     true,
			DiskPercent: float32(diskPercent),
			MemPercent:  float32(memPercent),
			CpuPercent:  float32(cpuPercent),
		}, nil
	}
	return nil, errors.New("HeartBeatError")
}

func (s *Server) forwardForReplication(request *dn.PutReq, reply *dn.PutResp) (*dn.PutResp, error) {
	blockId := request.BlockId
	blockAddresses := request.ReplicationNodes
	if len(blockAddresses) == 0 {
		log.Println("backup success")
		return &dn.PutResp{Success: true}, nil
	}

	startingDataNode := blockAddresses[0]
	log.Println("已经写入的blockAddress为:", startingDataNode)
	remainingDataNodes := blockAddresses[1:]
	log.Println("剩余可用的的blockAddress为:", remainingDataNodes)

	dataNodeInstance, rpcErr := grpc.Dial(startingDataNode.Host+":"+startingDataNode.ServicePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	util.Check(rpcErr)
	defer dataNodeInstance.Close()

	log.Println("backup data to another datanode:", startingDataNode.Host, startingDataNode.ServicePort)
	payloadRequest := dn.PutReq{
		Path:             request.Path,
		BlockId:          blockId,
		Data:             request.Data,
		ReplicationNodes: remainingDataNodes,
	}
	resp, rpcErr := dn.NewDataNodeClient(dataNodeInstance).Put(context.Background(), &payloadRequest)
	util.Check(rpcErr)
	return resp, nil
}

func (s *Server) Put(c context.Context, req *dn.PutReq) (*dn.PutResp, error) {
	log.Println("写入blockId为", req.BlockId)
	fileWriteHandler, err := os.Create(s.DataDirectory + req.BlockId)
	if err != nil {
		log.Println("data directory create error:", err)
	}
	log.Println("data directory create success")
	util.Check(err)
	defer fileWriteHandler.Close()

	fileWriter := bufio.NewWriter(fileWriteHandler)
	_, err = fileWriter.WriteString(req.Data)
	util.Check(err)
	fileWriter.Flush()
	resp := dn.PutResp{Success: true}
	replication, err := s.forwardForReplication(req, &resp)
	return replication, nil
}

func (s *Server) Get(c context.Context, req *dn.GetReq) (*dn.GetResp, error) {
	log.Println("读取的BlockId为：", req.BlockId)
	dataBytes, err := ioutil.ReadFile(s.DataDirectory + req.BlockId)
	if err != nil {
		return &dn.GetResp{}, err
	}
	return &dn.GetResp{Data: string(dataBytes)}, nil
}

func (s *Server) Delete(c context.Context, req *dn.DeleteReq) (*dn.DeleteResp, error) {
	_, err := os.Open(s.DataDirectory + req.BlockId)
	if err != nil {
		log.Println("文件已经被删掉")
		return &dn.DeleteResp{Success: true}, nil
	}
	err = os.Remove(s.DataDirectory + req.BlockId)
	if err != nil {
		return &dn.DeleteResp{}, err
	}
	log.Println("成功删除文件")
	return &dn.DeleteResp{Success: true}, nil
}

func (s *Server) Stat(c context.Context, req *dn.StatReq) (*dn.StatResp, error) {
	stat, err := os.Stat(s.DataDirectory + req.BlockId)
	if err != nil {
		log.Println("cannot stat the file:", err)
		return &dn.StatResp{}, err
	}
	return &dn.StatResp{
		Size:    stat.Size(),
		ModTime: stat.ModTime().Unix(),
	}, nil
}

// List 索引目录下的所有文件和文件夹
func (s *Server) List(c context.Context, req *dn.ListReq) (*dn.ListResp, error) {
	files, err := os.ReadDir(req.Path)
	if err != nil {
		log.Println("cannot list the file:", err)
		return &dn.ListResp{}, err
	}
	var blocks, dirs []string
	for _, file := range files {
		// 判断是否为文件夹
		if file.IsDir() {
			dirs = append(dirs, file.Name())
		} else {
			blocks = append(blocks, file.Name())
		}
	}
	return &dn.ListResp{FileList: blocks, DirList: dirs}, nil
}

// Mkdir 创建目录
func (s *Server) Mkdir(c context.Context, req *dn.MkdirReq) (*dn.MkdirResp, error) {
	err := os.Mkdir(req.Path, 0755)
	if err != nil {
		log.Println("cannot mkdir the file:", err)
		return &dn.MkdirResp{}, err
	}
	log.Println("成功创建目录")
	return &dn.MkdirResp{Success: true}, nil
}

// Rename 重命名文件
func (s *Server) Rename(c context.Context, req *dn.RenameReq) (*dn.RenameResp, error) {
	err := os.Rename(req.OldPath, req.NewPath)
	if err != nil {
		log.Println("cannot rename the file:", err)
		return &dn.RenameResp{}, err
	}
	log.Println("成功重命名")
	return &dn.RenameResp{Success: true}, nil
}

// GetCpuPercent 以下三个方法可以用于给NameNode决定选取哪一个datanode作为写入节点，已测试过，和linux命令行输出的结果相差无几
// GetCpuPercent 获取CPU使用率
func GetCpuPercent() (float64, error) {
	percent, err := cpu.Percent(time.Second, false)
	if err != nil {
		log.Println("Cannot Read CPU Message:", err)
		return 0, err
	}
	return percent[0], nil
}

// GetMemPercent 获取内存使用率
func GetMemPercent() (float64, error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		log.Println("Cannot Get Memory Percent:", err)
		return 0, err
	}
	return memInfo.UsedPercent, nil
}

// GetDiskPercent 获取当前程序所在目录的硬盘使用率
func GetDiskPercent() (float64, error) {
	pwd, err := os.Getwd()
	if err != nil {
		log.Println("cannot get pwd:", err)
		return 0, err
	}
	usage, err := disk.Usage(pwd)
	if err != nil {
		log.Println("Cannot Usage Disk Usage:", err)
		return 0, err
	}
	return usage.UsedPercent, nil
}
