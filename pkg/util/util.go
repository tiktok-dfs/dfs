package util

import (
	"github.com/go-redis/redis"
	"go.uber.org/zap"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type DataNodeInstance struct {
	Host        string
	ServicePort string
}

func Check(e error) {
	if e != nil {
		zap.L().Error(e.Error())
	}
}

func CheckStatus(e bool) {
	if !e {
		log.Println(e)
		//panic(e)
	}
}

// PathExist 判断文件或目录是否存在
// 如果 error != nil, 则出现了不是不存在的错误, 此时无法判断文件是否存在
// bool 当error == nil 时, 用来判断文件或目录是否存在
// 所以你总是想:
// filePathExist, err := util.PathExist(filePath)
// if err != nil {
//   panic(err)
// }
// if !filePathExist {
// // Do something
//  err := os.MkdirAll(filePath, 0750)
//  if err != nil {
//  panic(err)
//  }
// }
func PathExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	//isnotexist来判断，是不是不存在的错误
	if os.IsNotExist(err) { //如果返回的错误类型使用os.isNotExist()判断为true，说明文件或者文件夹不存在
		return false, nil
	}
	return false, err //如果有错误了，但是不是不存在的错误，所以把这个错误原封不动的返回
}

// ModPath 修改path格式
func ModPath(path string) string {
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

// ModFilePath 修改path格式
func ModFilePath(path string) string {
	if strings.HasSuffix(path, "/") {
		path = strings.TrimRight(path, "/")
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

// GetPrePath 获取文件名的前缀路径
func GetPrePath(filename string) string {
	dir, _ := filepath.Split(filename)
	return dir
}

//// LockFile 文件加锁
//func LockFile(filename string) error {
//
//	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
//// UnlockFile 解锁文件
//func UnlockFile(filename string) error {
//	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//	err = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
//	if err != nil {
//		return err
//	}
//	return nil
//}

var RedisDB *redis.Client

// InitRedis 初始化redis，用于分布式锁，理论依据：https://blog.51cto.com/u_13460811/5262747
func InitRedis() {
	RedisDB = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	_, err := RedisDB.Ping().Result()

	Check(err)
}

// Lock 分布式锁
func Lock(key string) bool {
	var result bool

	for {
		retryTimes := 0
		retryTimes++
		success, err := RedisDB.SetNX(key, "1", 0).Result()
		// TODO:解决潜在的死锁问题，目前非主动不会释放

		Check(err)
		if success {
			result = true
			break
		}
		if retryTimes >= 1000 {
			log.Println("retryTimes>=1000")
			break
		}

		time.Sleep(time.Millisecond)
	}
	return result
}

// Unlock 分布式锁解锁
func Unlock(key string) {
	RedisDB.Del(key)
}
