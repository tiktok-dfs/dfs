# go-fs

## master

两个表单：

1. file name -> chunk ids

2. chunk id -> data node

需要记录

    - 每个Chunk存储在哪些服务器上，所以这部分是Chunk服务器的列表

    以下未实现
    - 每个Chunk当前的版本号，所以Master节点必须记住每个Chunk对应的版本号。
    - 所有对于Chunk的写操作都必须在主Chunk（Primary Chunk）上顺序处理，主Chunk是Chunk的多个副本之一。所以，Master节点必须记住哪个Chunk服务器持有主Chunk。
    - 并且，主Chunk只能在特定的租约时间内担任主Chunk，所以，Master节点要记住主Chunk的租约过期时间。

我们的实现:

    1. chunk id -> data node ids

    2. data node ids -> data node address

数据都存在内存里, 如果 master 故障, 为了重启不丢失数据, 在写时也会将数据落盘(未实现)

需要落盘的数据:

    - Chunk Handle的数组（第一个表单, file name -> chunk ids）要保存在磁盘上。我给它标记成NV（non-volatile, 非易失），这个标记表示对应的数据会写入到磁盘上。
    - 版本号要不要落盘取决于实现
    - 每次文件到达64MB就会新增chunk或指定新的主chunk, 都需要追加一条log. 但要尽可能的少追加

不需要落盘的数据：

    - 第二张表单 (chunk id -> data node)， 因为重启后会自动与chunk server (data node)通讯
    - 主chunk的id. 重启后忘记谁是主chunk, 等待60s租约到期, 没有主chunk则会自动指定新的主chunk.
    - 租约到期时间

为什么使用磁盘而不是数据库?

这里在磁盘中维护 log 而不是数据库的原因是，数据库本质上来说是某种 B 树（b-tree）或者 hash table，相比之下，追加 log 会非常的高效，因为你可以将最近的多个 log 记录一次性的写入磁盘。因为这些数据都是向同一个地址追加，这样只需要等待磁盘的磁碟旋转一次。而对于 B 树来说，每一份数据都需要在磁盘中随机找个位置写入。所以使用 Log 可以使得磁盘写入更快一些。

多个 check point 也是有必要, 使 master 恢复时不用从头开始恢复.

## 读文件

1. 客户端发文件名与偏移量给 master
2. master 传给客户端 chunk ids 与 data nodes address
3. 客户端选择最近的 chunk 进行读取, 因为客户端每次可能只读取 1MB 或者 64KB 数据，所以，客户端可能会连续多次读取同一个 Chunk 的不同位置. 所以，客户端会缓存 Chunk 和服务器的对应关系，这样，当再次读取相同 Chunk 数据时，就不用一次次的去向 Master 请求相同的信息.
4. chunk 把数据给客户端.

## 写文件

写文件应该要干的事情: 当客户端发送写请求时, 告诉客户端在哪里追加, 即文件最后一个 chunk 的位置.

需要考虑不存在主副本的情况。所以 master 需要知道对于一个特定的 chunk, 哪个版本号是最新的, 需要落盘！.

如果 master 不知道哪个 chunk 是最新的, 会等所有最新的 chunk 服务器完成后, 挑一个主 chunk. 然后 master 也落盘. 发送消息给 chunk 说, 谁是主谁是副, 版本号是什么. 然后 chunk 服务器将这些信息落盘.

需要给主副本一个 lease.

写的过程: 客户端将数据发送给主副节点后, 先将数据写入临时文件中。当所有服务器返回消息说有数据要追加, 然后主副本会在大量并发请求中以某种顺序确保磁盘空间充足然后追加到文件末尾, 之后通知副节点追加。但是副节点可能会失败, 当追加成功后会给主节点发送 yes, 然后主节点给客户端发送写入成功, 一旦副节点写入失败, 主节点就给客户端返回写入失败。如果写入失败, 客户端应该重新发起整个追加请求.

再考虑发送给节点时, 应该写给主节点发, 然后主节点传递下去。

由网络分区引起,比如说，Master 无法与 Primary 通信，但是 Primary 又可以与客户端通信，这就是一种网络分区问题. 这就是脑裂. 处理脑裂的方式, 给主节点一个 lease, 当 master 不能和 primary 通信, 又想指定一个新的 primary 时, 只会等待 lease 失效, 而什么也不做.
