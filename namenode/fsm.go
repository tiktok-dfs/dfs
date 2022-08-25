package namenode

import (
	"encoding/json"
	"github.com/hashicorp/raft"
	"go-fs/common/config"
	"go-fs/pkg/tree"
	"go-fs/pkg/util"
	"io"
	"io/ioutil"
	"log"
	"path/filepath"
	"time"
)

var _ raft.FSM = &Service{}

func (nameNode *Service) Apply(l *raft.Log) interface{} {
	log.Println("用于单机测试判断是否同步数据")
	err := ioutil.WriteFile(filepath.Join(config.RaftCfg.RaftDataDir, "metadata.dat"), l.Data, 0755)
	if err != nil {
		panic(err)
	}
	return nil
}

// Snapshot 生成快照
func (nameNode *Service) Snapshot() (raft.FSMSnapshot, error) {
	//TODO implement me
	return Snap{
		Port:               nameNode.Port,
		BlockSize:          nameNode.BlockSize,
		ReplicationFactor:  nameNode.ReplicationFactor,
		IdToDataNodes:      nameNode.IdToDataNodes,
		DirTree:            nameNode.DirTree,
		DataNodeMessageMap: nameNode.DataNodeMessageMap,
		BlockToDataNodeIds: nameNode.BlockToDataNodeIds,
		FileNameToBlocks:   nameNode.FileNameToBlocks,
		DataNodeHeartBeat:  nameNode.DataNodeHeartBeat,
	}, nil
}

// Restore 从快照中恢复数据
func (nameNode *Service) Restore(snapshot io.ReadCloser) error {
	//TODO implement me
	var s Snap
	err := json.NewDecoder(snapshot).Decode(&s)
	if err != nil {
		panic(err)
	}
	nameNode.IdToDataNodes = s.IdToDataNodes
	nameNode.FileNameToBlocks = s.FileNameToBlocks
	nameNode.DataNodeHeartBeat = s.DataNodeHeartBeat
	nameNode.BlockToDataNodeIds = s.BlockToDataNodeIds
	nameNode.DataNodeHeartBeat = s.DataNodeHeartBeat
	nameNode.ReplicationFactor = s.ReplicationFactor
	nameNode.BlockSize = s.BlockSize
	nameNode.DirTree = s.DirTree
	nameNode.DataNodeMessageMap = s.DataNodeMessageMap
	return nil
}

// Snap 用于生成快照，服务重启的时候会从快照中恢复，需要实现两个接口
type Snap struct {
	Port               uint16
	BlockSize          uint64
	ReplicationFactor  uint64
	IdToDataNodes      map[int64]util.DataNodeInstance
	FileNameToBlocks   map[string][]string
	BlockToDataNodeIds map[string][]int64
	DataNodeMessageMap map[string]DataNodeMessage
	DataNodeHeartBeat  map[string]time.Time
	DirTree            *tree.DirTree
}

func (s Snap) Persist(sink raft.SnapshotSink) error {
	//TODO implement me
	bytes, err := json.Marshal(s)
	if err != nil {
		return err
	}
	_, err = sink.Write(bytes)
	if err != nil {
		return err
	}
	err = sink.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s Snap) Release() {
}
