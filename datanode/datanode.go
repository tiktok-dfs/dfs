package datanode

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	"go-fs/pkg/util"
	dn "go-fs/proto/datanode"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
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
		//以下可改成协程进行，作用不大，heartbeat已经是五秒一请求
		diskPercent, err := GetUsedDisk()
		if err != nil {
			log.Println("cannot GetUsedDisk:", err)
		}
		memPercent, err := GetUsedMem()
		if err != nil {
			log.Println("cannot GetUsedMem:", err)
		}
		cpuPercent, err := GetCpuPercent()
		if err != nil {
			log.Println("cannot GetCpuPercent:", err)
		}
		totalDisk, err := GetTotalDisk()
		if err != nil {
			log.Println("cannot GetTotalDisk:", err)
		}
		totalMem, err := GetTotalMem()
		if err != nil {
			log.Println("cannot GetTotalMem:", err)
		}
		return &dn.HeartBeatResp{
			Success:    true,
			UsedDisk:   diskPercent,
			UsedMem:    memPercent,
			CpuPercent: float32(cpuPercent),
			TotalDisk:  totalDisk,
			TotalMem:   totalMem,
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
	zap.S().Debug("写入blockId为", req.BlockId)
	fileWriteHandler, err := os.Create(s.DataDirectory + req.Path + req.BlockId)
	if err != nil {
		zap.S().Debug("data directory create error: ", err)
		return nil, err
	}
	defer fileWriteHandler.Close()
	zap.S().Debug("data directory create success")

	fileWriter := bufio.NewWriter(fileWriteHandler)
	_, err = fileWriter.WriteString(string(req.Data))
	if err != nil {
		return nil, errors.New("写文件失败")
	}
	fileWriter.Flush()
	resp := dn.PutResp{Success: true}
	replication, err := s.forwardForReplication(req, &resp)
	return replication, nil
}

func (s *Server) Get(c context.Context, req *dn.GetReq) (*dn.GetResp, error) {
	zap.S().Debug("读取的BlockId为：", req.BlockId)
	dataBytes, err := ioutil.ReadFile(s.DataDirectory + req.PrePath + req.BlockId)
	if err != nil {
		return &dn.GetResp{}, err
	}
	return &dn.GetResp{Data: dataBytes}, nil
}

func (s *Server) Delete(c context.Context, req *dn.DeleteReq) (*dn.DeleteResp, error) {
	_, err := os.Open(s.DataDirectory + req.PrePath + req.BlockId)
	zap.S().Debug("will open: "+s.DataDirectory+req.PrePath+req.BlockId, " required: ", req.PrePath, req.BlockId)
	if err != nil {
		zap.S().Debug("文件已经被删掉")
		return &dn.DeleteResp{Success: true}, nil
	}
	err = os.Remove(s.DataDirectory + req.PrePath + req.BlockId)
	if err != nil {
		return &dn.DeleteResp{}, err
	}
	zap.S().Debug("成功删除文件")
	return &dn.DeleteResp{Success: true}, nil
}

func (s *Server) Stat(c context.Context, req *dn.StatReq) (*dn.StatResp, error) {
	p := path.Join(s.DataDirectory + req.PrePath + req.BlockId)
	zap.S().Debug("Stat: ", p)
	stat, err := os.Stat(p)
	if err != nil {
		zap.S().Error("cannot stat the file:", err)
		return &dn.StatResp{}, err
	}
	_, name := filepath.Split(stat.Name())
	mod := fmt.Sprintf("%s", stat.Mode())
	return &dn.StatResp{
		Size:    stat.Size(),
		ModTime: stat.ModTime().Unix(),
		Name:    name,
		Mode:    mod,
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
	//判断用户是否携带/
	path := req.Path
	if strings.HasPrefix(path, "/") {
		path = strings.TrimPrefix(path, "/")
	}
	err := os.MkdirAll(s.DataDirectory+path, 0755)
	if err != nil {
		zap.S().Error("cannot mkdir the file:", err)
		return &dn.MkdirResp{}, err
	}
	zap.S().Info("成功创建目录")
	return &dn.MkdirResp{Success: true}, nil
}

// Rename 重命名文件
func (s *Server) Rename(c context.Context, req *dn.RenameReq) (*dn.RenameResp, error) {
	err := os.Rename(s.DataDirectory+req.OldPath, s.DataDirectory+req.NewPath)
	if err != nil {
		log.Println("cannot rename the file:", err)
		return &dn.RenameResp{}, err
	}
	log.Println("成功重命名")
	return &dn.RenameResp{Success: true}, nil
}

// GetCpuPercent 以下方法可以用于给NameNode决定选取哪一个datanode作为写入节点，已测试过，和linux命令行输出的结果相差无几
// GetCpuPercent 获取CPU使用率
func GetCpuPercent() (float64, error) {
	percent, err := cpu.Percent(time.Second, false)
	if err != nil {
		log.Println("Cannot Read CPU Message:", err)
		return 0, err
	}
	return percent[0], nil
}

// GetUsedMem 获取内存已经使用量
func GetUsedMem() (uint64, error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		log.Println("Cannot Get Memory Percent:", err)
		return 0, err
	}
	return memInfo.Used, nil
}

// GetUsedDisk 获取当前程序所在目录的硬盘已使用字节数量
func GetUsedDisk() (uint64, error) {
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
	return usage.Used, nil
}

// GetTotalMem 获取总内存，方便计算内存占用率
func GetTotalMem() (uint64, error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		log.Println("Cannot Get Memory Percent:", err)
		return 0, err
	}
	return memInfo.Total, nil
}

// GetTotalDisk 获取磁盘大小，方便计算磁盘利用率
func GetTotalDisk() (uint64, error) {
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
	return usage.Total, nil
}
