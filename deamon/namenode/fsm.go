package namenode

import (
	"github.com/hashicorp/raft"
	"go-fs/namenode"
)

var _ raft.FSM = &namenode.Service{}
