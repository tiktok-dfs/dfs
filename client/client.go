package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/klauspost/reedsolomon"
	"go-fs/pkg/converter"
	"go-fs/pkg/e"
	"go-fs/pkg/util"
	dn "go-fs/proto/datanode"
	namenode_pb "go-fs/proto/namenode"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type StatResp struct {
	FileName string
	FileSize int64
	ModTime  time.Time
}

type ListResp struct {
	DirName  []string
	FileName []string
}

const MaxShardsNum = 25

func Put(nameNodeConn *grpc.ClientConn, sourceFilePath string, destFilePath string) (bool, error) {
	nameNodeInstance := namenode_pb.NewNameNodeServiceClient(nameNodeConn)

	// -------------- Step1:获取文件大小 ---------------
	fileSizeHandler, err := os.Stat(sourceFilePath)
	if err != nil {
		log.Println("文件不存在")
		return false, e.ErrFileDoesNotExist
	}

	var blockSize uint64
	namenodeGetBlockSizeRequest := &namenode_pb.GetBlockSizeRequest{Request: true}
	blockSizeResponse, err := nameNodeInstance.GetBlockSize(context.Background(), namenodeGetBlockSizeRequest)
	if err != nil {
		return false, e.ErrInternalBusy
	}
	zap.S().Debugf("NameNode里获取到的块大小： %d", blockSizeResponse.BlockSize)
	blockSize = blockSizeResponse.BlockSize

	// 拿到size为了给文件分片(block), 每个block会被分配到不同的data node中
	fileSize := uint64(fileSizeHandler.Size())
	fileName := destFilePath
	prePath := util.GetPrePath(fileName)

	numberOfBlocksToAllocate := uint64(math.Ceil(float64(fileSize) / float64(blockSize)))
	zap.S().Debugf("分配块的数量: %d", numberOfBlocksToAllocate)

	//分割filename
	namenodeWriteRequest := &namenode_pb.WriteRequest{
		FileName:    fileName,
		BlockNumber: numberOfBlocksToAllocate,
	}

	// ---------------- Step2: 从 namenode 获取初始化的元数据 ----------------
	// namenode 的 writeData并不是真的写入, 返回的reply包含每一个文件的block应该被写入的data node 的地址
	writeResponse, err := nameNodeInstance.WriteData(context.Background(), namenodeWriteRequest)
	if err != nil {
		zap.S().Debugf("err: %s, e: %s", err.Error(), e.ErrDuplicatedWrite.Error())
		zap.S().Debugf("%p, %p", err, e.ErrDuplicatedWrite)
		zap.S().Debugf("%p, %p", err, e.ErrSubDirTree)
		switch err.Error() {
		case "文件重复写":
			log.Println("文件已存在")
		case "子目录不存在":
			log.Println("请确认你创建的文件目录是否存在")
		}
		return false, err
	}
	zap.S().Debugf("NameNode里的写数据信息： %v", writeResponse.NameNodeMetaDataList)

	// ---------------- Step3: 开始读取文件，分片后发送给 datanode ----------------
	fileHandler, err := os.Open(sourceFilePath)
	if err != nil {
		return false, e.ErrInternalBusy
	}
	defer fileHandler.Close()
	zap.S().Debugf("打开文件： %s", sourceFilePath)

	// buffer 每次只读全局设定的block size 或 更少的数据
	dataStagingBytes := make([]byte, blockSize) // 用于保存当前批次的数据
	putStatus := false

	// 被切成多少个 block 就有多少组 matdata，然后每个 matdata 记录这批相同的 block 被分配给哪些 datanode
	for _, pbMetaData := range writeResponse.NameNodeMetaDataList {
		metaData := converter.Pb2NameNodeMetaData(pbMetaData) // 将元数据从 pb 格式解析到结构体中

		// n代表着实际读到的byte数
		n, err := fileHandler.Read(dataStagingBytes)
		if err != nil {
			if err != io.EOF {
				zap.S().Error("File read n bytes failed")
				return false, e.ErrInternalBusy
			}
		}
		dataStagingBytes = dataStagingBytes[:n] // 截取实际读到的数据（最后一次可能不会正好覆盖完整个 dataStagingBytes）

		blockId := metaData.BlockId
		blockAddresses := metaData.BlockAddresses

		startingDataNode := blockAddresses[0]    // 头 datanode
		remainingDataNodes := blockAddresses[1:] // 备份的 datanode，由头节点往后复制，无需客户端亲自传递数据

		var datanodes []*dn.DataNodeInstance
		for _, dni := range remainingDataNodes {
			datanodes = append(datanodes, &dn.DataNodeInstance{
				Host:        dni.Host,
				ServicePort: dni.ServicePort,
			})
		}
		// data1 node 此时真正的准备写入数据
		dataNodeInstance, rpcErr := grpc.Dial(startingDataNode.Host+":"+startingDataNode.ServicePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if rpcErr != nil {
			zap.S().Error(rpcErr)
			return false, e.ErrInternalBusy
		}
		defer dataNodeInstance.Close()

		request := dn.PutReq{
			Path:             prePath,
			BlockId:          blockId,
			Data:             dataStagingBytes, // 负载的数据分片
			ReplicationNodes: datanodes,        // 需要往后传递，用于备份的 datanode 列表
		}
		// 写入数据
		zap.S().Debugf("已经写入的BLockId为：%s", blockId)
		resp, rpcErr := dn.NewDataNodeClient(dataNodeInstance).Put(context.Background(), &request)
		if rpcErr != nil {
			zap.S().Error(rpcErr)
			return false, e.ErrInternalBusy
		}
		zap.S().Debugf("put data success: %v", resp)
		if resp.Success {
			putStatus = true
			continue
		} else {
			putStatus = false
		}
	}
	return putStatus, nil
}

func Get(nameNodeConn *grpc.ClientConn, sourceFilePath string) (fileContents string, getStatus bool, err error) {
	nameNodeInstance := namenode_pb.NewNameNodeServiceClient(nameNodeConn)

	fileName := sourceFilePath
	prePath := util.GetPrePath(fileName)
	fileName = util.ModPath(fileName)
	nameNodeReadRequest := &namenode_pb.ReadRequst{FileName: fileName}

	// name node 并不是真的读数据, 返回的reply包含每一个文件的block 存放在data node 的地址
	readResponse, err := nameNodeInstance.ReadData(context.Background(), nameNodeReadRequest)
	if err != nil {
		zap.S().Debugf("%s", err.Error())
		zap.S().Debugf("%p, %p", err, e.ErrDuplicatedWrite)
		zap.S().Debugf("%p, %p", err, e.ErrSubDirTree)
		switch err.Error() {
		case "z文件不存在":
			log.Println("请检查文件路径")
		}
		zap.S().Error(err)
		return "", false, err
	}

	zap.S().Debug("调用NameNode读取数据的信息为：", readResponse.NameNodeMetaDataList)
	fileContents = ""

	for _, pbMetaData := range readResponse.NameNodeMetaDataList {

		//blockId := metaData.BlockId
		blockAddresses := pbMetaData.BlockAddresses
		// block被获取的标志位
		blockFetchStatus := false

		// 每一个block, 都被备份了x次, 但只需要拿一次.
		for _, selectedDataNode := range blockAddresses {
			// data1 node 此时真正的准备读数据
			dataNodeInstance, rpcErr := grpc.Dial(selectedDataNode.Host+":"+selectedDataNode.ServicePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if rpcErr != nil {
				continue
			}

			defer func(dataNodeInstance *grpc.ClientConn) {
				err := dataNodeInstance.Close()
				if err != nil {
					zap.S().Error("cannot close connection,please check:", err)
				}
			}(dataNodeInstance)

			request := dn.GetReq{
				PrePath: prePath,
				BlockId: pbMetaData.BlockId,
			}

			// 读数据
			resp, rpcErr := dn.NewDataNodeClient(dataNodeInstance).Get(context.Background(), &request)
			if err != nil {
				zap.S().Error(err)
				continue
			}
			// 追加内容
			fileContents += string(resp.Data)
			// 读取成功后, 将标志位置为true, 此block不再获取
			blockFetchStatus = true
			break
		}

		// 如果一个block重试x次都没有拿到数据, 则返回文件没有get到
		if !blockFetchStatus {
			getStatus = false
			return fileContents, getStatus, nil
		}
	}

	// 所有block被拿到, 返回文件成功get到
	getStatus = true
	return fileContents, getStatus, nil
}

func Delete(nameNodeConn *grpc.ClientConn, filename string) bool {
	zap.S().Debug("filename: ", filename)

	filename = util.ModPath(filename)
	resp, err := namenode_pb.NewNameNodeServiceClient(nameNodeConn).DeleteData(context.Background(), &namenode_pb.DeleteDataReq{
		FileName: filename,
	})
	if err != nil {
		zap.S().Debugf("%s", err.Error())
		zap.S().Debugf("%p, %p", err, e.ErrDuplicatedWrite)
		zap.S().Debugf("%p, %p", err, e.ErrSubDirTree)
		switch err {
		case e.ErrFileDoesNotExist:
			log.Println("文件不存在")
		}
		zap.S().Error("调用NameNode的Delete请求发生错误:", err)

		return false
	}
	zap.S().Debug("调用NameNode的Delete请求读取数据的信息为：", resp.NameNodeMetaDataList)
	deleteStatus := false
	prePath := util.GetPrePath(util.ModFilePath(filename))

	zap.S().Debug(" prePath: ", prePath)

	//datanode开始删除数据
	for _, mdi := range resp.NameNodeMetaDataList {
		for _, dni := range mdi.BlockAddresses {
			conn, err := grpc.Dial(dni.Host+":"+dni.ServicePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				zap.S().Error("cannot connect datanode:", dni.Host, ":", dni.ServicePort)
				return false
			}
			defer conn.Close()

			deleteResp, err := dn.NewDataNodeClient(conn).Delete(context.Background(), &dn.DeleteReq{
				BlockId: mdi.BlockId,
				PrePath: prePath,
			})
			if err != nil {
				zap.S().Error("cannot dial method from datanode:", dni.Host, ":", dni.ServicePort)
				return false
			}
			if deleteResp.Success {
				deleteStatus = true
			} else {
				deleteStatus = false
			}
		}
	}
	return deleteStatus
}

func Stat(nameNodeConn *grpc.ClientConn, filename string) (string, error) {
	filename = util.ModFilePath(filename)
	resp, err := namenode_pb.NewNameNodeServiceClient(nameNodeConn).StatData(context.Background(), &namenode_pb.StatDataReq{
		FileName: filename,
	})
	prePath := util.GetPrePath(filename)
	if err != nil {
		zap.S().Error("NameNode Stat Data Error:", err)
		return "", err
	}
	zap.S().Debug("NameNode Stat Resp:", resp.NameNodeMetaDataList)
	var modTime int64
	var size int64
	var mod string
	n := 0
	// block被获取的标志位
	blockFetchStatus := false
	for _, mdi := range resp.NameNodeMetaDataList {
		for _, dni := range mdi.BlockAddresses {
			conn, err := grpc.Dial(dni.Host+":"+dni.ServicePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				zap.S().Error("cannot connect datanode:", dni.Host, ":", dni.ServicePort)
				return "", err
			}
			resp, err := dn.NewDataNodeClient(conn).Stat(context.Background(), &dn.StatReq{
				BlockId: mdi.BlockId,
				PrePath: prePath,
			})
			if err != nil {
				zap.S().Error("cannot dial method from datanode:", dni.Host, ":", dni.ServicePort)
				return "", err
			}
			size += resp.Size
			modTime += resp.ModTime
			n += 1
			if mod == "" {
				mod = resp.Mode
			}

			blockFetchStatus = true
			break
		}

		if !blockFetchStatus {
			return "", errors.New("cannot Stat BlockId From DataNode" + mdi.BlockId)
		}
	}
	_, name := filepath.Split(filename)
	time := time.Unix(modTime, 0).Format("2006-01-02 15:04:05")

	prompt := fmt.Sprintln("Permissions\tSize\tDate Modified\tName")
	info := fmt.Sprintf("%s\t%d\t%s\t%s", mod, size, time, name)
	stat := fmt.Sprintf("\n%s%s", prompt, info)
	return stat, nil

}

func Mkdir(nameNodeConn *grpc.ClientConn, filename string) bool {
	//先判断用户传入的filename是否以/号结尾
	suffix := strings.HasSuffix(filename, "/")
	if !suffix {
		filename = filename + "/"
	}
	client := namenode_pb.NewNameNodeServiceClient(nameNodeConn)
	_, err := client.Mkdir(context.Background(), &namenode_pb.MkdirReq{Path: filename})
	if err != nil {
		zap.S().Error("NameNode Mkdir Error:", err)
		return false
	}
	nodes, err := client.GetDataNodes(context.Background(), &namenode_pb.GetDataNodesReq{})
	if err != nil {
		zap.S().Error("NameNode Get DataNodes Error:", err)
		return false
	}
	for _, dni := range nodes.DataNodeList {
		conn, err := grpc.Dial(dni, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			zap.S().Error("Dial DataNodes Error:", err)
			return false
		}
		resp, err := dn.NewDataNodeClient(conn).Mkdir(context.Background(), &dn.MkdirReq{
			Path: filename,
		})
		if err != nil {
			zap.S().Error("cannot dial method from datanode:", dni)
			return false
		}
		if resp.Success {
		} else {
			return false
		}
	}
	return true
}

func Rename(nameNodeConn *grpc.ClientConn, remoteFilePath string, renameDestPath string) bool {
	//判断文件类型，目录交给datanode处理，文件交给NameNode处理
	//先将remoteFilePath交给NameNode判断是否是文件
	client := namenode_pb.NewNameNodeServiceClient(nameNodeConn)
	resp, err := client.IsDir(context.Background(), &namenode_pb.IsDirReq{
		Filename: remoteFilePath,
	})
	if err != nil {
		zap.S().Error("NameNode IsDir Error:", err)
		return false
	}
	zap.S().Debug("isdir: ", resp.Ok)
	if resp.Ok {
		//是文件夹格式，告诉datanode更改目录名称，也要告诉NameNode更新tree
		_, err := client.ReDirTree(context.Background(), &namenode_pb.ReDirTreeReq{
			OldPath: remoteFilePath,
			NewPath: renameDestPath,
		})
		if err != nil {
			zap.S().Error("NameNode ReDirTree Error:", err)
			return false
		}
		nodes, err := client.GetDataNodes(context.Background(), &namenode_pb.GetDataNodesReq{})
		if err != nil {
			zap.S().Error("NameNode Get DataNodes Error:", err)
			return false
		}
		for _, dni := range nodes.DataNodeList {
			conn, err := grpc.Dial(dni, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				zap.S().Error("Dial DataNodes Error:", err)
				return false
			}
			renameResp, err := dn.NewDataNodeClient(conn).Rename(context.Background(), &dn.RenameReq{
				OldPath: remoteFilePath,
				NewPath: renameDestPath,
			})
			if err != nil {
				zap.S().Error("cannot dial method from datanode:", dni)
				return false
			}
			if renameResp.Success {
			} else {
				return false
			}
		}
	} else {
		//是文件形式，告诉NameNode更改文件名称
		renameResp, err := client.Rename(context.Background(), &namenode_pb.RenameReq{
			OldFileName: remoteFilePath,
			NewFileName: renameDestPath,
		})
		zap.S().Debug("file rename: ", resp.Ok)
		if err != nil {
			zap.S().Error("NameNode Rename Error:", err)
			return false
		}
		if !renameResp.Success {
			return false
		}
	}
	return true
}

func List(nameNodeConn *grpc.ClientConn, parentPath string) (string, error) {
	resp, err := namenode_pb.NewNameNodeServiceClient(nameNodeConn).List(context.Background(), &namenode_pb.ListReq{
		ParentPath: parentPath,
	})
	if err != nil {
		log.Println("NameNode List Error:", err)
		return "", err
	}
	ls := fmt.Sprintf("dir: %s\nfile: %s", resp.DirName, resp.FileName)
	return ls, nil
}

func PutByEc(nameNodeConn *grpc.ClientConn, sourceFilePath string, destFilePath string) bool {
	//先获取可用datanode节点，并设置一个拆分切片最大值
	//目前仅对EC做个简单示例，全面覆盖多副本策略比较困难，时间有限
	file, err := ioutil.ReadFile(sourceFilePath)
	if err != nil {
		log.Println("cannot read file:", err)
		return false
	}
	prePath := util.GetPrePath(destFilePath)
	nodes, err := namenode_pb.NewNameNodeServiceClient(nameNodeConn).GetDataNodes(context.Background(), &namenode_pb.GetDataNodesReq{})
	if err != nil {
		log.Println("NameNode GetDataNodes Error:", err)
		return false
	}
	dataShardsNum := len(nodes.DataNodeList)
	if dataShardsNum >= MaxShardsNum {
		dataShardsNum = MaxShardsNum
	}
	parityShardsNum := dataShardsNum / 3
	//例如三个节点分成两个data切片和一个parity切片，暂时不考虑EC冗余配比
	encoder, err := reedsolomon.New(dataShardsNum-parityShardsNum, parityShardsNum)
	if err != nil {
		log.Println("cannot new reedsolomon:", err)
		return false
	}
	data, err := encoder.Split(file)
	if err != nil {
		log.Println("cannot Split reedsolomon:", err)
		return false
	}
	//接下来向NameNode发起请求给data分配datanode节点
	node, err := namenode_pb.NewNameNodeServiceClient(nameNodeConn).ECAssignDataNode(context.Background(), &namenode_pb.ECAssignDataNodeReq{
		Filename:       destFilePath,
		DatanodeNumber: int64(len(data)),
	})
	if err != nil {
		log.Println("NameNode ECAssignDataNode Error:", err)
		return false
	}
	//向datanode写入数据
	for i, m := range node.NameNodeMetaData {
		write := data[i]
		filename := m.BlockId
		for _, dni := range m.BlockAddresses {
			conn, err := grpc.Dial(dni.Host+":"+dni.ServicePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Println("DataNode Dial Error:", err)
				return false
			}
			resp, err := dn.NewDataNodeClient(conn).Put(context.Background(), &dn.PutReq{
				Path:             prePath,
				Data:             write,
				BlockId:          filename,
				ReplicationNodes: nil,
			})
			if err != nil {
				log.Println("DataNode Put Error:", err)
				return false
			}
			if resp.Success {
				log.Println("write data success:", filename)
			}
		}
	}
	return true
}

func GetByEc(nameNodeConn *grpc.ClientConn, filename string) (fileContents string, getStatus bool) {
	//多副本是读到一个datanode就结束，而EC需要读取所有的DataNodes
	fileContents = ""
	getStatus = false
	data, err := namenode_pb.NewNameNodeServiceClient(nameNodeConn).ReadData(context.Background(), &namenode_pb.ReadRequst{
		FileName: filename,
	})
	if err != nil {
		log.Println("NameNode ReadData Error:", err)
		return
	}
	prePath := util.GetPrePath(filename)
	for _, m := range data.NameNodeMetaDataList {
		for _, b := range m.BlockAddresses {
			conn, err := grpc.Dial(b.Host+":"+b.ServicePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Println("DataNode Dial Error:", err)
				return
			}
			resp, err := dn.NewDataNodeClient(conn).Get(context.Background(), &dn.GetReq{
				BlockId: m.BlockId,
				PrePath: prePath,
			})
			if err != nil {
				log.Println("DataNode Get Error:", err)
				return
			}
			fileContents += string(resp.Data)
		}
	}
	getStatus = true
	return
}

func RecoverDataByEC(nameNodeConn *grpc.ClientConn, filename string, deadDataNodeAddr string) (string, bool) {
	//暂时采用挂掉DataNode节点然后直接告诉NameNode哪个节点挂掉，然后观察能否恢复数据的方式来测试
	//先向NameNode获取blockId等信息
	resp, err := namenode_pb.NewNameNodeServiceClient(nameNodeConn).ReadData(context.Background(), &namenode_pb.ReadRequst{
		FileName: filename,
	})
	if err != nil {
		log.Println("NameNode ReadData Error:", err)
		return "", false
	}
	prePath := util.GetPrePath(filename)
	var data [][]byte
	for _, m := range resp.NameNodeMetaDataList {
		for _, b := range m.BlockAddresses {
			if b.Host+":"+b.ServicePort != deadDataNodeAddr {
				conn, err := grpc.Dial(b.Host+":"+b.ServicePort, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Println("DataNode Dial Error:", err)
					return "", false
				}
				getResp, err := dn.NewDataNodeClient(conn).Get(context.Background(), &dn.GetReq{
					BlockId: m.BlockId,
					PrePath: prePath,
				})
				if err != nil {
					log.Println("DataNode Get Error:", err)
					return "", false
				}
				data = append(data, getResp.Data)
			}
		}
	}
	//一般采用三个节点测试，此处直接赋值了
	encoder, err := reedsolomon.New(2, 1)
	verify, err := encoder.Verify(data)
	if !verify {
		err := encoder.Reconstruct(data)
		if err != nil {
			log.Println("cannot recover data")
			return "", false
		}
		b, err := encoder.Verify(data)
		if !b {
			log.Println("cannot recover data")
			return "", false
		}
	}
	var fileContent string
	for _, d := range data {
		fileContent += string(d)
	}
	return fileContent, true
}
