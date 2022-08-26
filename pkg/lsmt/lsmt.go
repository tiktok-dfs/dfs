package lsmt

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	SET_METHOD = iota
	DELETE_METHOD
)

var db Db

func init() {
	lsm := &LsmKvStore{
		index:          make(map[string]Entry),
		immutableIndex: make(map[string]Entry),
		storeThreshold: 10,
		ssTables:       make([]*SsTable, 0),
		Wal:            &Wal{WalFileName: "wal", TempWalFileName: "temp_wal"},
		tableName:      ".table",
		partSize:       3,
	}
	lsm.Wal.InitWal()

	lsm.RestoreFromFiles()
	lsm.RestoreFromWal()
	db = lsm
}

func Set(key, value string) error {
	return db.Set(key, value)
}

func Get(key string) (string, error) {
	return db.Get(key)
}

func Delete(key string) error {
	return db.Delete(key)
}

type Db interface {
	Set(key, value string) error
	Get(key string) (string, error)
	Delete(key string) error
}

type TableMetaInfo struct {
	version    int64
	dataStart  int64
	dataLen    int64
	indexStart int64
	indexLen   int64
	partSize   int64
}

type Position struct {
	Start  int64
	Length int64
}

func OpenFile(fileName string) *os.File {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(" error open file ", err.Error())
		os.Exit(-1)
	}
	return f
}

type SsTable struct {
	f             *os.File
	filePath      string
	tableMetaInfo *TableMetaInfo
	sparseIndex   map[string]Position
	partSize      int64
}

func (s *SsTable) Query(key string) *Entry {
	// 先排序
	var keys []string
	for k := range s.sparseIndex {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 使用二分查找
	i, j := 0, len(keys)
	foundKey := ""
	for i < j {
		mid := int((i + j) / 2)
		if mid == i || mid == j {
			break
		}
		if keys[mid] > key {
			j = mid - 1
		} else if keys[mid] < key {
			i = mid + 1
		} else {
			foundKey = key
		}
	}

	var minIndex int
	if len(foundKey) > 0 {
		minIndex = int((i + j) / 2)
	} else {
		foundIndex := len(keys)
		if i > j {
			minIndex = j
		} else {
			minIndex = i
		}
		if minIndex < 0 {
			minIndex = 0
		} else if minIndex >= foundIndex {
			minIndex = foundIndex - 1
		}
	}

	pos, ok := s.sparseIndex[keys[minIndex]]
	if ok == false {
		fmt.Println(" found sparse index error")
		os.Exit(1)
	}

	s.f.Seek(pos.Start, 0)

	bytes := make([]byte, pos.Length)
	fmt.Println("  found sparse  read  ", pos.Length, pos.Start, keys[minIndex])
	_, err := s.f.Read(bytes)
	if err != nil {
		fmt.Println(" found sparse Read error ", err.Error())
		os.Exit(1)
	}

	partData := make(map[string]Entry)
	err = json.Unmarshal(bytes, &partData)
	if err != nil {
		fmt.Println(" found sparse unmarshal error ", err.Error())
		os.Exit(1)
	}
	e, ok := partData[key]
	if ok == false {
		return nil
	}

	return &e
}

func CreateSsTableFromIndex(fileName string, partSize int64, immutableIndex map[string]Entry) *SsTable {
	f := OpenFile(fileName)
	f.Seek(0, 0)

	partData := make(map[string]Entry)
	tableMetaMap := make(map[string]Position)
	tableMetaInfo := &TableMetaInfo{}

	// 先排序
	var keys []string
	for k := range immutableIndex {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	startKey := ""
	startPos := 0
	tableMetaInfo.version = 0
	tableMetaInfo.partSize = partSize
	tableMetaInfo.dataStart = int64(startPos)

	for _, k := range keys {
		if len(startKey) == 0 {
			startKey = k
		}
		partData[k] = immutableIndex[k]
		if len(partData) == int(partSize) {
			bytes, err := json.Marshal(partData)
			if err != nil {
				fmt.Println(" marshal json error ", err)
				os.Exit(-1)
			}
			f.Write(bytes)
			// 保存当前的part信息
			tableMetaMap[startKey] = Position{Start: int64(startPos), Length: int64(len(bytes))}
			startPos += len(bytes)
			startKey = ""
			partData = make(map[string]Entry)
		}
	}
	if len(partData) > 0 {
		bytes, err := json.Marshal(partData)
		if err != nil {
			fmt.Println(" marshal json error ", err)
			os.Exit(-1)
		}
		f.Write(bytes)
		// 保存当前的part信息
		tableMetaMap[startKey] = Position{Start: int64(startPos), Length: int64(len(bytes))}
		startPos += len(bytes)
		startKey = ""
		partData = make(map[string]Entry)
	}

	tableMetaInfo.dataLen = int64(startPos)
	tableMetaInfo.indexStart = int64(startPos)
	// 将稀疏信息也写入
	bytes, err := json.Marshal(tableMetaMap)
	if err != nil {
		fmt.Println(" marshal json error ", err)
		os.Exit(-1)
	}
	f.Write(bytes)
	tableMetaInfo.indexLen = int64(len(bytes))

	// 写入文件元信息
	binary.Write(f, binary.LittleEndian, &tableMetaInfo.version)
	binary.Write(f, binary.LittleEndian, &tableMetaInfo.dataStart)
	binary.Write(f, binary.LittleEndian, &tableMetaInfo.dataLen)
	binary.Write(f, binary.LittleEndian, &tableMetaInfo.indexStart)
	binary.Write(f, binary.LittleEndian, &tableMetaInfo.indexLen)
	binary.Write(f, binary.LittleEndian, &tableMetaInfo.partSize)

	return &SsTable{
		f:             f,
		filePath:      fileName,
		tableMetaInfo: tableMetaInfo,
		sparseIndex:   tableMetaMap,
		partSize:      partSize,
	}
}

func RestoreFromFile(fileName string) *SsTable {
	f := OpenFile(fileName)

	// 因为是int64 占八个字节 从文件尾部开始
	tableMetaInfo := &TableMetaInfo{}
	f.Seek(0, 0)
	fileInfo, err := f.Stat()
	if err != nil {
		fmt.Println(" RestoreFromFile file stat error ", err.Error())
		os.Exit(1)
	}
	fileSize := fileInfo.Size()
	f.Seek(fileSize-8*6, 0)
	err = binary.Read(f, binary.LittleEndian, &tableMetaInfo.version)
	if err != nil {
		fmt.Println(" RestoreFromFile binary.Read error ", err.Error())
		os.Exit(1)
	}
	f.Seek(fileSize-8*5, 0)
	err = binary.Read(f, binary.LittleEndian, &tableMetaInfo.dataStart)
	if err != nil {
		fmt.Println(" RestoreFromFile binary.Read error ", err.Error())
		os.Exit(1)
	}
	f.Seek(fileSize-8*4, 0)
	err = binary.Read(f, binary.LittleEndian, &tableMetaInfo.dataLen)
	if err != nil {
		fmt.Println(" RestoreFromFile binary.Read error ", err.Error())
		os.Exit(1)
	}
	f.Seek(fileSize-8*3, 0)
	err = binary.Read(f, binary.LittleEndian, &tableMetaInfo.indexStart)
	if err != nil {
		fmt.Println(" RestoreFromFile binary.Read error ", err.Error())
		os.Exit(1)
	}
	f.Seek(fileSize-8*2, 0)
	err = binary.Read(f, binary.LittleEndian, &tableMetaInfo.indexLen)
	if err != nil {
		fmt.Println(" RestoreFromFile binary.Read error ", err.Error())
		os.Exit(1)
	}
	f.Seek(fileSize-8*1, 0)
	err = binary.Read(f, binary.LittleEndian, &tableMetaInfo.partSize)
	if err != nil {
		fmt.Println(" RestoreFromFile binary.Read error ", err.Error())
		os.Exit(1)
	}

	fmt.Println(" restore from file ", tableMetaInfo)
	f.Seek(0, 0)

	f.Seek(tableMetaInfo.indexStart, 0)

	bytes := make([]byte, tableMetaInfo.indexLen)

	_, err = f.Read(bytes)
	if err != nil {
		fmt.Println(" restore from file error ", err.Error())
		os.Exit(-1)
	}
	sparseIndex := make(map[string]Position)

	err = json.Unmarshal(bytes, &sparseIndex)
	if err != nil {
		fmt.Println(" restore from file Unmarshal error ", err.Error())
		os.Exit(-1)
	}

	return &SsTable{
		f:             f,
		tableMetaInfo: tableMetaInfo,
		partSize:      tableMetaInfo.partSize,
		filePath:      fileName,
		sparseIndex:   sparseIndex,
	}
}

type Wal struct {
	f               *os.File
	WalFileName     string
	TempWalFileName string
}

func (w *Wal) InitWal() {
	w.f = OpenFile(w.WalFileName)
}

func (w *Wal) WriteInt(length int) error {
	return binary.Write(w.f, binary.LittleEndian, int64(length))
}

func (w *Wal) ReadInt() (int, error) {
	var r int64

	return int(r), binary.Read(w.f, binary.LittleEndian, &r)
}

func (w *Wal) Write(bytes []byte) error {
	totalLen := len(bytes)
	writeLen := 0
	for writeLen < totalLen {
		n, err := w.f.Write(bytes)
		if err != nil {
			return err
		}
		writeLen += n
	}
	return nil
}

func (w *Wal) Read(l int) ([]byte, error) {
	bytes := make([]byte, l)
	_, err := w.f.Read(bytes)
	return bytes, err
}

type Entry struct {
	Method int    `json:"method"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

func (e *Entry) Encode() ([]byte, error) {
	bytes, err := json.Marshal(e)
	if err != nil {
		return []byte{}, err
	}
	return bytes, nil
}

func DecodeEntry(bytes []byte) (Entry, error) {
	var e Entry
	err := json.Unmarshal(bytes, &e)
	return e, err
}

type LsmKvStore struct {
	// 当前的索引地址
	index map[string]Entry
	// 不可变的索引地址
	immutableIndex map[string]Entry

	lock sync.RWMutex

	ssTables []*SsTable
	// 触发同步到sstable的阈值
	storeThreshold int

	tableName string
	partSize  int64
	Wal       *Wal
}

func (l *LsmKvStore) SwitchIndex() {
	l.immutableIndex = l.index
	l.index = make(map[string]Entry)
	// 关闭文件
	l.Wal.f.Close()
	// 重命名文件
	os.Rename(l.Wal.WalFileName, l.Wal.TempWalFileName)
	l.Wal.InitWal()
}

func (l *LsmKvStore) StoreToSsTable() {
	timeStr := time.Now().Format("2006-01-02_15:04:05.000000")
	fileName := fmt.Sprintf("%s%s", timeStr, l.tableName)
	s := CreateSsTableFromIndex(fileName, l.partSize, l.immutableIndex)
	l.ssTables = append(l.ssTables, s)
}

func (l *LsmKvStore) RestoreFromFiles() {
	files, _ := ioutil.ReadDir("./")
	for _, f := range files {
		if strings.HasSuffix(f.Name(), l.tableName) {
			s := RestoreFromFile(f.Name())
			l.ssTables = append(l.ssTables, s)
		}
	}
}

func (l *LsmKvStore) RestoreFromWal() {
	for {
		length, err := l.Wal.ReadInt()
		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println(" error read wal int ", err.Error())
			return
		}
		bytes, err := l.Wal.Read(length)
		if err != nil {
			fmt.Println(" error read wal unmarshal ", err.Error())
			os.Exit(1)
		}
		e := Entry{}
		err = json.Unmarshal(bytes, &e)
		if err != nil {
			fmt.Println(" error read wal unmarshal ", err.Error())
			os.Exit(1)
		}
		l.index[e.Key] = e
	}

}

func (l *LsmKvStore) Get(key string) (string, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()

	v, ok := l.index[key]
	fmt.Println(" index  ", l.index)
	if ok == true {
		if v.Method == DELETE_METHOD {
			return "", nil
		}
		return v.Value, nil
	}

	// 查找不可变的index
	v, ok = l.immutableIndex[key]
	if ok == true {
		if v.Method == DELETE_METHOD {
			return "", nil
		}
		return v.Value, nil
	}

	// 查找文件
	for _, s := range l.ssTables {
		e := s.Query(key)
		if e != nil {
			if e.Method == SET_METHOD {
				return e.Value, nil
			}
		}
	}
	return "", errors.New(" not found key")
}

func (l *LsmKvStore) Set(key, value string) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	e := Entry{Key: key, Value: value, Method: SET_METHOD}
	bytes, err := e.Encode()
	if err != nil {
		fmt.Println("error set  ", key, value)
		return err
	}
	err = l.Wal.WriteInt(len(bytes))
	if err != nil {
		fmt.Println(" error write int  ", err.Error())
		return err
	}
	l.Wal.Write(bytes)
	l.index[key] = e

	if len(l.index) >= l.storeThreshold {
		fmt.Println(" threshold  ", len(l.index))
		l.SwitchIndex()
		l.StoreToSsTable()
	}
	return nil
}

func (l *LsmKvStore) Delete(key string) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	e := Entry{Key: key, Value: "", Method: DELETE_METHOD}

	bytes, err := e.Encode()
	if err != nil {
		fmt.Println("error delete  ", key)
		return err
	}

	err = l.Wal.WriteInt(len(bytes))
	if err != nil {
		fmt.Println(" error write int  ", err.Error(), len(bytes))
		return err
	}
	l.Wal.Write(bytes)
	l.index[key] = e

	if len(l.index) >= l.storeThreshold {
		fmt.Println(" threshold  ", len(l.index))
		l.SwitchIndex()
		l.StoreToSsTable()
	}
	return nil
}
