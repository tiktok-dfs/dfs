package main

import (
	"flag"
	"go-fs/common/config"
	"go-fs/deamon/client"
	"go-fs/deamon/datanode"
	"go-fs/deamon/namenode"
	"log"
	"os"
	"sync"
)

var once sync.Once

func init() {
	once.Do(func() {
		config.ReadCfg()
		config.Init()
	})
}

func main() {
	WorkByCli()
}

func WorkByCli() {
	dataNodeCommand := flag.NewFlagSet("datanode", flag.ExitOnError)
	nameNodeCommand := flag.NewFlagSet("namenode", flag.ExitOnError)
	clientCommand := flag.NewFlagSet("client", flag.ExitOnError)

	nameNodeAddr := dataNodeCommand.String("namenode", "", "NameNode communication port")

	master := nameNodeCommand.Bool("master", false, "start by boostrap")

	clientNameNodePortPtr := clientCommand.String("namenode", "localhost:9000", "NameNode communication port")
	clientOperationPtr := clientCommand.String("operation", "", "Operation to perform")
	clientSourcePathPtr := clientCommand.String("source-path", "", "Source path of the file")
	clientFilenamePtr := clientCommand.String("filename", "", "File name")
	clientOldFilenamePtr := clientCommand.String("old", "", "Old File Name")
	clientNewFilenamePtr := clientCommand.String("new", "", "New File Name")

	if len(os.Args) < 2 {
		log.Println("sub-command is required")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "datanode":
		_ = dataNodeCommand.Parse(os.Args[2:])
		dataNodePortPtr := config.DataNodeCfg.Port
		dataNodeDataLocationPtr := config.DataNodeCfg.Path
		leader, err := namenode.FindLeader(*nameNodeAddr)
		if err != nil {
			panic(err)
		}
		datanode.InitializeDataNodeUtil(leader, int(dataNodePortPtr), dataNodeDataLocationPtr)

	case "namenode":
		_ = nameNodeCommand.Parse(os.Args[2:])
		nameNodePort := config.NameNodeCfg.Port
		nameNodeBlockSize := config.NameNodeCfg.BlockSize
		nameNodeReplicationFactor := config.NameNodeCfg.ReplicationFactor
		namenode.InitializeNameNodeUtil(*master, config.RaftCfg.RaftId, int(nameNodePort), int(nameNodeBlockSize), int(nameNodeReplicationFactor))

	case "client":
		_ = clientCommand.Parse(os.Args[2:])

		leader, err := namenode.FindLeader(*clientNameNodePortPtr)
		if err != nil {
			panic(err)
		}
		if *clientOperationPtr == "put" {
			status := client.PutHandler(leader, *clientSourcePathPtr, *clientFilenamePtr)
			log.Printf("Put status: %t\n", status)

		} else if *clientOperationPtr == "get" {
			contents, status := client.GetHandler(leader, *clientFilenamePtr)
			log.Printf("Get status: %t\n", status)
			if status {
				log.Println(contents)
			}

		} else if *clientOperationPtr == "delete" {
			status := client.DeleteHandler(leader, *clientFilenamePtr)
			log.Println("Delete Status:", status)

		} else if *clientOperationPtr == "stat" {
			resp, err := client.StatHandler(leader, *clientFilenamePtr)
			if err != nil {
				log.Println("Stat Error:", err)
			}
			log.Println("Stat Data Message:\n", "FileName:", *clientFilenamePtr, "FileSize:", resp.FileSize, "FileModTime:", resp.ModTime)

		} else if *clientOperationPtr == "mkdir" {
			status := client.MkdirHandler(leader, *clientFilenamePtr)
			log.Println("Mkdir Status:", status)

		} else if *clientOperationPtr == "mv" {
			status := client.RenameHandle(leader, *clientOldFilenamePtr, *clientNewFilenamePtr)
			log.Println("mv Status:", status)

		} else if *clientOperationPtr == "ls" {
			resp, err := client.ListHandler(leader, *clientFilenamePtr)
			if err != nil {
				log.Println("Ls Error:", err)
			}
			log.Println("ls Data:", resp)
		}
	}
}
