package datanode

import (
	"go-fs/pkg/util"
	"os"
	"testing"
)

// Test creating a DataNode Service
func TestDataNodeServiceCreation(t *testing.T) {
	testDataNodeService := new(Service)
	testDataNodeService.DataDirectory = "./"
	testDataNodeService.ServicePort = 8000

	if testDataNodeService.DataDirectory != "./" {
		t.Errorf("Unable to set DataDirectory correctly; Expected: %s, found: %s", "./", testDataNodeService.DataDirectory)
	}
	if testDataNodeService.ServicePort != 8000 {
		t.Errorf("Unable to set ServicePort correctly; Expected: %d, found: %d", 8000, testDataNodeService.ServicePort)
	}
}

// Test writing data in single file without dir paths within DataNode
func TestDataNodeServiceWriteFile(t *testing.T) {
	testDataNodeService := new(Service)
	testDataNodeService.DataDirectory = "./workdir/"
	testDataNodeService.ServicePort = 8000

	putRequestPayload := DataNodePutRequest{
		FilePath:         "./fileWithoutDir.txt",
		BlockId:          "1",
		Data:             "Hello world",
		ReplicationNodes: nil,
	}

	var replyPayload DataNodeWriteStatus
	testDataNodeService.PutData(&putRequestPayload, &replyPayload)

	if !replyPayload.Status {
		t.Errorf("Unable to write data correctly; Expected: %t, found: %t", true, replyPayload.Status)
	}

	// clean up
	filePathExist, err := util.PathExist("./workdir/fileWithoutDir.txt")
	if err != nil {
		t.Error("Unexpected Error")
	}

	if !filePathExist {
		t.Error("Unable to write data correctly; No file putted")
	}

	err = os.RemoveAll("./workdir")
	if err != nil {
		t.Errorf("Unexpected Error! Please Clean up the test risidual file manually")
	}
}

// Test writing data in single file with dir paths within DataNode
func TestDataNodeServiceWriteFileWithDirPaths(t *testing.T) {
	testDataNodeService := new(Service)
	testDataNodeService.DataDirectory = "./workdir/"
	testDataNodeService.ServicePort = 8000

	putRequestPayload := DataNodePutRequest{
		FilePath:         "testfolder/testfolder2/fileWithDir.txt",
		BlockId:          "1",
		Data:             "Hello world",
		ReplicationNodes: nil,
	}

	var replyPayload DataNodeWriteStatus
	testDataNodeService.PutData(&putRequestPayload, &replyPayload)

	if !replyPayload.Status {
		t.Errorf("Unable to write data correctly; Expected: %t, found: %t", true, replyPayload.Status)
	}

	// clean up
	filePathExist, err := util.PathExist("./workdir/testfolder/testfolder2/fileWithDir.txt")
	if err != nil {
		t.Error("Unexpected Error")
	}

	if !filePathExist {
		t.Error("Unable to write data correctly; No file putted")
	}

	err = os.RemoveAll("./workdir")
	if err != nil {
		t.Errorf("Unexpected Error! Please Clean up the test risidual file manually")
	}
}

// Test reading data within DataNode
//func TestDataNodeServiceRead(t *testing.T) {
//testDataNodeService := new(Service)
//testDataNodeService.DataDirectory = "./"
//testDataNodeService.ServicePort = 8000

//getRequestPayload := DataNodeGetRequest{BlockId: "1"}
//var replyPayload DataNodeData
//testDataNodeService.GetData(&getRequestPayload, &replyPayload)

//if strings.Compare(replyPayload.Data, "Hello world") != 0 {
//t.Errorf("Unable to read data correctly; Expected: %s, found: %s.", "Hello world", replyPayload.Data)
//}
//}
