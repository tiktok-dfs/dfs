package config

import (
	"github.com/sirupsen/logrus"
)

type NameNode struct {
	Port              uint32
	BlockSize         uint64
	ReplicationFactor uint64
}

type Raft struct {
	RaftDataDir string
	RaftId      string
}

type DataNode struct {
	Port uint32
	Path string
}

var (
	RaftCfg     Raft
	NameNodeCfg NameNode
	DataNodeCfg DataNode
)

func Init() {
	var err error

	if err = conf.UnmarshalKey("namenode", &NameNodeCfg); err != nil {
		logrus.Panicf("parse config err, app: %v", err)
	}

	if err = conf.UnmarshalKey("datanode", &DataNodeCfg); err != nil {
		logrus.Panicf("parse config err, app: %v", err)
	}

	if err = conf.UnmarshalKey("raft", &RaftCfg); err != nil {
		logrus.Panicf("parse config err, app: %v", err)
	}

	logrus.Debug("parse config success")
}
