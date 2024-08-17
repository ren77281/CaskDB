package http

import (
	"encoding/json"
	"fmt"
	bitcask "kv-go/db"
	"log"
	"net/http"
	"os"
)

var db *bitcask.DB

func init() {
	// 初始化DB实例
	opts := bitcask.DefaultDBOptions
	// TODO!!!这里放在tmp目录下是否合适？
	dir, _ := os.MkdirTemp("", "bitcask-http")
	opts.DirPath = dir
	var err error
	db, err = bitcask.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("fail to create db, %v", err))
	}
}

func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// 解析http请求为k-v
	var datas map[string]string
	if err := json.NewDecoder(request.Body).Decode(&datas); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	for k, v := range datas {
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			http.Error(writer, "Method not allowed", http.StatusInternalServerError)
			log.Printf("failed to put kv, %v\n", err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// 获取key对应的value
	key := request.URL.Query().Get("key")
	val, err := db.Get([]byte(key))
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to get value, %v\n", err)
			return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(val))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// 获取要删除的key并删除
	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))
	if err != nil{
		http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to delete key, %v\n", err)
			return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// 获取迭代的方向TODO!!!
	// reverse := request.URL.Query().Get("reverse")
	keys := db.ListKeys(true)
	writer.Header().Set("Content-Type", "application/json")
	var result []string
	for _, k := range keys {
		result = append(result, string(k))
	}
	_ = json.NewEncoder(writer).Encode(result)
}

func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	stat, err := db.Stat()
	if err != nil{
		http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to get stat, %v\n", err)
			return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)
}

func main() {
	// 注册http处理方法
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/listkeys", handleListKeys)
	http.HandleFunc("/bitcask/stat", handleStat)
	// 启动http服务
	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
		panic(fmt.Sprintf("fail to start http server, %v", err))
	}
}