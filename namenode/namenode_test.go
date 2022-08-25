package namenode

import (
	boltdb "github.com/hashicorp/raft-boltdb"
	"go-fs/common/config"
	"path/filepath"
	"testing"
)

/*
import (
	"context"
	"go-fs/pkg/util"
	namenode_pb "go-fs/proto/namenode"
	"testing"
)

// Test creating a NameNode Service
func TestNameNodeCreation(t *testing.T) {
	testNameNodeService := Service{
		BlockSize:          4,
		ReplicationFactor:  2,
		FileNameToBlocks:   make(map[string][]string),
		IdToDataNodes:      make(map[int64]util.DataNodeInstance),
		BlockToDataNodeIds: make(map[string][]int64),
	}

	testDataNodeInstance1 := util.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := util.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2

	if len(testNameNodeService.IdToDataNodes) != 2 || testNameNodeService.BlockSize != 4 || testNameNodeService.ReplicationFactor != 2 {
		t.Errorf("Unable to initialize NameNode correctly; Expected: %d, %d, %d, found: %v, %d %d.", 2, 4, 2, testNameNodeService.IdToDataNodes, testNameNodeService.BlockSize, testNameNodeService.ReplicationFactor)
	}
}

// Test write process
func TestNameNodeServiceWrite(t *testing.T) {
	testNameNodeService := Service{
		BlockSize:          4,
		ReplicationFactor:  2,
		FileNameToBlocks:   make(map[string][]string),
		IdToDataNodes:      make(map[int64]util.DataNodeInstance),
		BlockToDataNodeIds: make(map[string][]int64),
		DataNodeMessageMap: make(map[string]DataNodeMessage),
		DirTree:            initDirTree(),
	}

	testDataNodeInstance1 := util.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := util.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2

	writeDataPayload := &namenode_pb.WriteRequest{
		FileName: "foo",
	}

	response, err := testNameNodeService.WriteData(context.Background(), writeDataPayload)
	util.Check(err)
	if len(response.NameNodeMetaDataList) != 3 {
		t.Errorf("Unable to set metadata correctly; Expected: %d, found: %d.", 3, len(response.NameNodeMetaDataList))
	}
}
*/

func TestService_RegisterDataNode(t *testing.T) {

	config.ReadCfg()
	config.Init()

	baseDir := filepath.Join(config.RaftCfg.RaftDataDir, "node1")
	join := filepath.Join(baseDir, "logs.dat")
	store, err := boltdb.NewBoltStore(join)
	if err != nil {
		panic(err)
	}
	err = store.Set([]byte("update"), []byte("update"))
	if err != nil {
		panic(err)
	}
	err = store.Set([]byte("update"), []byte("test"))
	if err != nil {
		panic(err)
	}
	get, err := store.Get([]byte("update"))
	if err != nil {
		panic(err)
	}
	t.Log(string(get))
}
