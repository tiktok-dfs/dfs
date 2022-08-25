package namenode

import (
	"github.com/hashicorp/raft"
	"go-fs/common/config"
	"io"
	"io/ioutil"
	"log"
	"path/filepath"
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

func (nameNode *Service) Snapshot() (raft.FSMSnapshot, error) {
	//TODO implement me
	return nil, nil
}

func (nameNode *Service) Restore(snapshot io.ReadCloser) error {
	//TODO implement me
	return nil
}
