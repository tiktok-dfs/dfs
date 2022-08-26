package main

import (
	"bufio"
	"flag"
	cli "github.com/opentrx/seata-golang/v2/pkg/client"
	"github.com/opentrx/seata-golang/v2/pkg/client/config"
	l "github.com/opentrx/seata-golang/v2/pkg/util/log"
	c "go-fs/common/config"
	"go-fs/deamon/client"
	"go-fs/deamon/datanode"
	"go-fs/deamon/namenode"
	"go-fs/pkg/logger"
	"go.uber.org/zap"
	"log"
	"os"
	"sync"
)

var once sync.Once

func init() {
	once.Do(func() {
		c.ReadCfg()
		c.Init()
		configuration := config.InitConfiguration("D:\\goWorkSpace\\src\\godfs-dev\\dfs\\namenode\\config\\config.yml")
		cli.Init(configuration)
		l.Init(configuration.Log.LogPath, configuration.Log.LogLevel)
	})
}

func main() {
	WorkByCli()
}

func WorkByCli() {

	logger.InitLogger()

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
	clientDestPathPtr := clientCommand.String("dest-path", "", "destination path of the file")
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
		dataNodePortPtr := int(c.DataNodeCfg.Port)
		if *dataport != 7000 {
			//cmd上指定了优先使用指定的,后面亦是如此
			dataNodePortPtr = *dataport
		}
		dataNodeDataLocationPtr := c.DataNodeCfg.Path
		if *dataLocation != "" {
			dataNodeDataLocationPtr = *dataLocation
		}
		datanode.InitializeDataNodeUtil(*datanodeHost, *nameNodeAddr, dataNodePortPtr, dataNodeDataLocationPtr)

	case "namenode":
		_ = nameNodeCommand.Parse(os.Args[2:])
		nameNodePort := int(c.NameNodeCfg.Port)
		raftId := c.RaftCfg.RaftId
		if *port != 0 {
			nameNodePort = *port
		}
		if *raftid != "" {
			raftId = *raftid
		}
		nameNodeBlockSize := c.NameNodeCfg.BlockSize
		nameNodeReplicationFactor := c.NameNodeCfg.ReplicationFactor
		namenode.InitializeNameNodeUtil(*host, *master, *follow, raftId, nameNodePort, int(nameNodeBlockSize), int(nameNodeReplicationFactor))

	case "client":
		_ = clientCommand.Parse(os.Args[2:])

		if !*ec {
			if *clientOperationPtr == "put" {
				status, err := client.PutHandler(*clientNameNodePortPtr, *clientSourcePathPtr, *clientFilenamePtr)
				if err != nil {
					log.Println(err.Error())
				}
				log.Printf("Put status: %t\n", status)

			} else if *clientOperationPtr == "get" {
				contents, status, err := client.GetHandler(*clientNameNodePortPtr, *clientFilenamePtr)
				if err != nil {
					log.Println(err.Error())
				}
				log.Printf("Get status: %t\n", status)
				if !status {
					return
				}

				log.Println(contents)

				fileWriteHandler, err := os.Create(*clientDestPathPtr)
				defer fileWriteHandler.Close()
				if err != nil {
					log.Println("请检查路径")
					return
				}

				fileWriter := bufio.NewWriter(fileWriteHandler)
				fileWriter.WriteString(contents)
				fileWriter.Flush()

			} else if *clientOperationPtr == "delete" {
				status := client.DeleteHandler(*clientNameNodePortPtr, *clientFilenamePtr)
				log.Println("Delete Status:", status)

			} else if *clientOperationPtr == "stat" {
				resp, err := client.StatHandler(*clientNameNodePortPtr, *clientFilenamePtr)
				if err != nil {
					zap.S().Debug("Stat Error:", err)
					log.Println("state file failed")
					return
				}
				log.Println(resp)

			} else if *clientOperationPtr == "mkdir" {
				status := client.MkdirHandler(*clientNameNodePortPtr, *clientFilenamePtr)
				log.Println("Mkdir Status:", status)

			} else if *clientOperationPtr == "mv" {
				status := client.RenameHandle(*clientNameNodePortPtr, *clientOldFilenamePtr, *clientNewFilenamePtr)
				log.Println("mv Status:", status)

			} else if *clientOperationPtr == "ls" {
				resp, err := client.ListHandler(*clientNameNodePortPtr, *clientFilenamePtr)
				if err != nil {
					zap.S().Debug("Ls Error:", err)
					return
				}
				log.Println(resp)
			}
		} else {
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
