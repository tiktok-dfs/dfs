package main

import (
	"flag"
	"go-fs/deamon/client"
	"go-fs/deamon/datanode"
	"go-fs/deamon/namenode"
	"log"
	"os"
	"strings"
)

func main() {
	WorkByCli()
}

func WorkByCli() {
	dataNodeCommand := flag.NewFlagSet("datanode", flag.ExitOnError)
	nameNodeCommand := flag.NewFlagSet("namenode", flag.ExitOnError)
	clientCommand := flag.NewFlagSet("client", flag.ExitOnError)

	dataNodePortPtr := dataNodeCommand.Int("port", 7000, "DataNode communication port")
	dataNodeDataLocationPtr := dataNodeCommand.String("data-location", ".", "DataNode data storage location")

	nameNodePortPtr := nameNodeCommand.Int("port", 9000, "NameNode communication port")
	nameNodeListPtr := nameNodeCommand.String("datanodes", "", "Comma-separated list of DataNodes to connect to")
	nameNodeBlockSizePtr := nameNodeCommand.Int("block-size", 32, "Block size to store")
	nameNodeReplicationFactorPtr := nameNodeCommand.Int("replication-factor", 1, "Replication factor of the system")

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
		datanode.InitializeDataNodeUtil(*dataNodePortPtr, *dataNodeDataLocationPtr)

	case "namenode":
		_ = nameNodeCommand.Parse(os.Args[2:])
		var listOfDataNodes []string
		if len(*nameNodeListPtr) > 1 {
			listOfDataNodes = strings.Split(*nameNodeListPtr, ",")
		} else {
			listOfDataNodes = []string{}
		}
		namenode.InitializeNameNodeUtil(*nameNodePortPtr, *nameNodeBlockSizePtr, *nameNodeReplicationFactorPtr, listOfDataNodes)

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
			log.Println("Delete Status:", status)

		} else if *clientOperationPtr == "mv" {
			status := client.RenameHandle(*clientNameNodePortPtr, *clientOldFilenamePtr, *clientNewFilenamePtr)
			log.Println("mv Status:", status)

		}
	}
}
