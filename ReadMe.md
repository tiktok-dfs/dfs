# 一、项目介绍

项目名称： 简易分布式存储系统

Github 地址：https://github.com/tiktok-dfs/dfs

针对数据的可用性, 可靠性, 稳定性, 实现可以处理节点异常, 资源扩容以及一定数据一致性等场景的分布式存储系统.

## 基础功能

| API    | 输入                                         | 输出                                                                | 说明                                                                                                                                                         |
| ------ | -------------------------------------------- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| PUT    | 本地文件 source-path 远程文件路径 filename   | 是否成功 status && 错误信息(如果有)                                 | 1. 不允许重复 PUT 相同路径的文件 (一定程度缓解了并发写) 2. datanode 会优先根据 CPU, 内存, 磁盘占用, 总内存, 总磁盘大小等因素进行一定计算, 选取 data node PUT |
| GET    | 远程文件路径 filename 写入本地路径 dest-path | 是否成功 status && 错误信息(如果有)                                 | 如有多个远程副本, 默认顺序进行读写                                                                                                                           |
| DELETE | 远程文件路径 filename                        | 是否成功 status                                                     | 结果对用户意义不大,但对系统而言 namenode 的记录被删除即已删除.                                                                                               |
| Stat   | 远程文件路径 filename                        | 文件信息: 权限 文件长度 修改日期 文件名 or 不进行输出(如有错误发生) |                                                                                                                                                              |
| MKDIR  | 远程文件路径 filename                        | 是否成功 status                                                     | 在指定多个 namenode 创建目标目录路径                                                                                                                         |
| RENAME | 远程文件路径 old 新路径 new                  | 是否成功 status                                                     | 重命名                                                                                                                                                       |
| LIST   | 远程文件路径                                 | 文件与文件夹名称                                                    | 获取文件与文件夹名称                                                                                                                                         |

# 二、项目分工

| 团队成员 | 主要贡献 |
| -------- | -------- |
| 邵佳成   |          |
| 雷永腾   |          |
| 徐皓     |          |
|          |          |

# 三、项目实现

## 技术选型

- RPC 通信：https://github.com/grpc/grpc-go
- 元数据一致性：https://github.com/hashicorp/raft
- 日志模块：https://github.com/uber-go/zap

## 系统架构

<div align=center><img src="https://tva4.sinaimg.cn/large/006cK6rNly1h5k05rssw7j30k10n4q6n.jpg"></div>

## 设计思路

- 确定 client、name node、data node 分工
- 确定元数据信息的内容
- 确定元数据一致性方案

## 组件分析

- #### name node

  name node 服务由有多个 name node 节点组成`raft集群`提供服务. name node 服务主要负责`存储相关元数据`, `接收客户端请求并发送必要的元数据`. 元数据的持久化与多个 name node 节点的元数据的状态统一由 raft 算法来保证.

  **启动流程**：

  1. 注册 grpc 服务端以及 raft 节点
  2. 等待 data node 注册进来
  3. 定期检查 data node 心跳，如若未发送心跳，进行下线和迁移数据处理
  4. 监听主节点元数据变化（可优化为监听自身成为 leader 时，从磁盘读取元数据信息）

- #### data node

  data node 服务负责`存储数据`. 可以由多个 data node 节点组成, 做到无感的扩容以及因节点异常导致的数据迁移.

  **启动流程**：

  1. 注册 grpc 服务端
  2. 向 name node 注册直到成功
  3. 定期给 name node 发送心跳，并设有计数器，超过某个数值，向 name node 更新自己的内存、磁盘等信息

- #### client

  client 与`用户直接交互`, 接收用户请求并返回结果. 目前提供命令行的方式进行交互.

## 多副本策略与 EC 纠删码策略

- 多副本：指定最少副本数量, PUT 时, datanode 会优先根据 CPU, 内存, 磁盘占用, 总内存, 总磁盘大小等因素进行一定计算, 找到对应数量的 datanode 进行 PUT. 当 data node 有异常时, 数据重定向到新的 datanode
- EC 纠删码：写入数据时，将数据切分为 N 个数据块（N 为偶数），通过 EC 编码算法计算得到 M 个校验块（M 取值 2、3 或 4），将 N+M 个数据块和校验块存储于不同的节点中，故障 M 个节点或 M 块硬盘，系统仍可正常读写数据，业务不中断，数据不丢失。EC 冗余方式的空间利用率约为 N/(N+M)，N 越大，空间利用率越高，数据的可靠性由 M 值的大小决定，M 越大可靠性越高。
- 多副本（N）与 纠删码（M+N）对比
  - 可用容量：多副本 1/N 较低，EC M/(M+N)，较高
  - 读写性能：多副本较高，EC 较低，小块 IO 尤其明显
  - 重构性能：副本无校验计算，较快 EC 有校验计算，较慢
  - 容忍节点故障数量： N-1 N
  - 适用场景： 块存储，小文件 大文件 综合来看，如果用户更关注性能，尤其是小 IO 的场景，多副本往往是更好的选择，如果用户更关注可用容量，而且是大文件场景的话，纠删码会更合适

## 元数据一致性|分布式共识算法

- 第一个节点以 boostrap 方式启动 raft 集群
- 后面的节点依次加入 raft 集群
- 主节点发起 apply 调用将元数据信息写入磁盘，产生 Log Entries，从节点依次执行该 log entry 并落盘，由此实现元数据一致性
- 切换 leader 对 data node、client 均无感

## 代码架构

```
.
├── Makefile
├── ReadMe.md
├── app.yaml
├── client // 发起 RPC 请求
│   └── client.go
├── common // 读取配置文件
│   └── config
│       ├── config.go
│       └── settings.go
├── datanode // 处理 DN 实际业务
│   ├── datanode.go
│   └── datanode_test.go
├── deamon // 初始化、心跳、高可用相关
│   ├── client
│   │   └── client.go
│   ├── datanode
│   │   └── datanode.go
│   └── namenode
│       └── namenode.go
├── go.mod
├── go.sum
├── hello.txt
├── img
│   ├── image-20220811104512231.png
│   ├── image-20220811104841134.png
│   ├── image-20220811110356434.png
│   ├── image-20220811111016123.png
│   ├── image-20220811111357983.png
│   └── image-20220811111415759.png
├── main.go
├── namenode // 处理 NN 实际业务
│   ├── fsm.go
│   ├── namenode.go
│   └── namenode_test.go
├── pkg // 全局工具
│   ├── converter
│   │   └── converter.go
│   ├── e
│   │   └── e.go
│   ├── logger
│   │   ├── logger.go
│   │   └── logger_test.go
│   ├── tree
│   │   ├── dir_tree.go
│   │   └── dir_tree_test.go
│   └── util
│       ├── util.go
│       └── util_test.go
├── proto // RPC 相关
│   ├── datanode
│   │   ├── datanode.pb.go
│   │   ├── datanode.proto
│   │   └── datanode_grpc.pb.go
│   └── namenode
│       ├── namenode.pb.go
│       ├── namenode.proto
│       └── namenode_grpc.pb.go
├── todo.md
├── 开发文档.md
├── 新测试文档.md
└── 测试文档.md
```

# 四、监控与性能测试

# 五、demo 展示

# 六、项目总结与展望

- 待接入分布式事务框架，name node 执行过程发生 error 的时候元数据信息会发生变化

