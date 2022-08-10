# 学习指南

## 学习顺序

namenode -> datanode -> deamon/namenode -> deamon/datanode -> client and (deamon/client) -> main

## 使用

在 goDFS 目录: `make build`

创建三个隐藏文件夹

```bash
mkdir .dndata1/ .dndata2/ .dndata3
```

<div align=center><img src="https://tvax1.sinaimg.cn/large/006cK6rNly1h4llje9urkj30vt02d41l.jpg"></div>

在不同的 terminal 下, 启动三个 data node 端口为 7001, 7002, 7003

```bash
./godfs datanode --port 7001 --data1-location .dndata1/
./godfs datanode --port 7002 --data1-location .dndata2/
./godfs datanode --port 7003 --data1-location .dndata3/
```

<div align=center><img src="https://tva1.sinaimg.cn/large/006cK6rNly1h4llm29fzoj30pp0fagwr.jpg"></div>

在另一个 terminal 下, 启动 name node, 端口为 7000

```bash
./godfs namenode --port 7000 --datanodes localhost:7001,localhost:7002,localhost:7003 --block-size 10 --replication-factor 2
```

<div align=center><img src="https://tvax3.sinaimg.cn/large/006cK6rNly1h4llob6845j311303dq6v.jpg"></div>

创建一个文件 hello.txt

<div align=center><img src="https://tvax2.sinaimg.cn/large/006cK6rNly1h4llp0a5o1j30qy030q5t.jpg"></div>

在另一个 terminal 下使用客户端 :

PUT

```bash
./godfs client --namenode localhost:7000 --operation put --source-path ./ --filename hello.txt
```

<div align=center><img src="https://tva4.sinaimg.cn/large/006cK6rNly1h4llpzrh49j30w506yjxd.jpg"></div>

GET:

```bash
./godfs client --namenode localhost:7000 --operation get --filename hello.txt
```

<div align=center><img src="https://tvax1.sinaimg.cn/large/006cK6rNly1h4llqw28wqj30rs043jv8.jpg"></div>
