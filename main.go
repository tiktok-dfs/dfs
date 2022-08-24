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
	dataport := dataNodeCommand.Int("port", 7000, "")
	dataLocation := dataNodeCommand.String("path", "", "")
	datanodeHost := dataNodeCommand.String("host", "", "")

	master := nameNodeCommand.Bool("master", false, "start by boostrap")
	follow := nameNodeCommand.String("follow", "", "")
	port := nameNodeCommand.Int("port", 0, "")
	raftid := nameNodeCommand.String("raftid", "", "")
	host := nameNodeCommand.String("host", "", "")

	clientNameNodePortPtr := clientCommand.String("namenode", "localhost:9000", "NameNode communication port")
	clientOperationPtr := clientCommand.String("operation", "", "Operation to perform")
	clientSourcePathPtr := clientCommand.String("source-path", "", "Source path of the file")
	clientFilenamePtr := clientCommand.String("filename", "", "File name")
	clientOldFilenamePtr := clientCommand.String("old", "", "Old File Name")
	clientNewFilenamePtr := clientCommand.String("new", "", "New File Name")
	ec := clientCommand.Bool("ec", false, "put data by EC")
	dead := clientCommand.String("dead", "", "dead datanode addr")

	if len(os.Args) < 2 {
		log.Println("sub-command is required")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "datanode":
		_ = dataNodeCommand.Parse(os.Args[2:])
		dataNodePortPtr := int(config.DataNodeCfg.Port)
		if *dataport != 7000 {
			//cmd上指定了优先使用指定的,后面亦是如此
			dataNodePortPtr = *dataport
		}
		dataNodeDataLocationPtr := config.DataNodeCfg.Path
		if *dataLocation != "" {
			dataNodeDataLocationPtr = *dataLocation
		}
		datanode.InitializeDataNodeUtil(*datanodeHost, *nameNodeAddr, dataNodePortPtr, dataNodeDataLocationPtr)

	case "namenode":
		_ = nameNodeCommand.Parse(os.Args[2:])
		nameNodePort := int(config.NameNodeCfg.Port)
		raftId := config.RaftCfg.RaftId
		if *port != 0 {
			nameNodePort = *port
		}
		if *raftid != "" {
			raftId = *raftid
		}
		nameNodeBlockSize := config.NameNodeCfg.BlockSize
		nameNodeReplicationFactor := config.NameNodeCfg.ReplicationFactor
		namenode.InitializeNameNodeUtil(*host, *master, *follow, raftId, nameNodePort, int(nameNodeBlockSize), int(nameNodeReplicationFactor))

	case "client":
		_ = clientCommand.Parse(os.Args[2:])

		if *clientOperationPtr == "put" {
			status := client.PutHandler(*clientNameNodePortPtr, *clientSourcePathPtr, *clientFilenamePtr)
			log.Printf("Put status: %t\n", status)

		} else if *clientOperationPtr == "get" {
			contents, status := client.GetHandler(*clientNameNodePortPtr, *clientFilenamePtr)
			log.Printf("Get status: %t\n", status)
			if status {
				log.Println(contents)
			}

		} else if *clientOperationPtr == "delete" {
			status := client.DeleteHandler(*clientNameNodePortPtr, *clientFilenamePtr)
			log.Println("Delete Status:", status)

		} else if *clientOperationPtr == "stat" {
			resp, err := client.StatHandler(*clientNameNodePortPtr, *clientFilenamePtr)
			if err != nil {
				log.Println("Stat Error:", err)
			}
			log.Println("Stat Data Message:\n", "FileName:", *clientFilenamePtr, "FileSize:", resp.FileSize, "FileModTime:", resp.ModTime)

		} else if *clientOperationPtr == "mkdir" {
			status := client.MkdirHandler(*clientNameNodePortPtr, *clientFilenamePtr)
			log.Println("Mkdir Status:", status)

		} else if *clientOperationPtr == "mv" {
			status := client.RenameHandle(*clientNameNodePortPtr, *clientOldFilenamePtr, *clientNewFilenamePtr)
			log.Println("mv Status:", status)

		} else if *clientOperationPtr == "ls" {
			resp, err := client.ListHandler(*clientNameNodePortPtr, *clientFilenamePtr)
			if err != nil {
				log.Println("Ls Error:", err)
			}
			log.Println("ls Data:", resp)
		} else if *ec {
			if *clientOperationPtr == "put" {
				status := client.PutByEcHandler(*clientNameNodePortPtr, *clientSourcePathPtr, *clientFilenamePtr)
				log.Printf("PutByEc status: %t\n", status)
			} else if *clientOperationPtr == "get" {
				contents, status := client.GetByEcHandler(*clientNameNodePortPtr, *clientFilenamePtr)
				log.Printf("GetByEc status: %t\n", status)
				if status {
					log.Println(contents)
				}
			} else if *clientOperationPtr == "recover" {
				contents, status := client.RecoverDataHandler(*clientNameNodePortPtr, *clientFilenamePtr, *dead)
				log.Printf("Recover status: %t\n", status)
				if status {
					log.Println(contents)
				}
			}
		}
	}
}
