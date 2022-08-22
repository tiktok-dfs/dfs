package namenode

import (
	"github.com/hashicorp/raft"
	"go-fs/common/config"
	"go-fs/namenode"
	"go-fs/pkg/tree"
	"go-fs/pkg/util"
	"io"
	"io/ioutil"
	"time"
)

var _ raft.FSM = &RaftMessage{}

type RaftMessage struct {
	nameNodeInstance *namenode.Service
}

type Metadata struct {
	NameNodeHost          string
	NameNodePort          string
	BlockSize             uint64
	ReplicationFactor     uint64
	IdToDataNodes         map[int64]util.DataNodeInstance
	FileNameToBlocks      map[string][]string
	BlockToDataNodeIds    map[string][]int64
	DataNodeMessageMap    map[string]namenode.DataNodeMessage
	DataNodeLastHeartTime map[string]time.Time
	DirTree               *tree.DirTree
}

func (r *RaftMessage) Apply(log *raft.Log) interface{} {
	err := ioutil.WriteFile(config.RaftCfg.RaftDataDir, log.Data, 0755)
	if err != nil {
		panic(err)
	}
	return nil
}

func (r *RaftMessage) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (r *RaftMessage) Restore(snapshot io.ReadCloser) error {
	//TODO implement me
	return nil
}
