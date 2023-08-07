package main

import (
	"context"
	"fmt"
	mysql_storage "github.com/storage-lock/go-mysql-storage"
)

func main() {

	// 数据库连接不是DSN的形式，就是一堆零散的属性，则依次设置，可以得到一个连接管理器
	host := "127.0.0.1"
	port := uint(3306)
	username := "root"
	passwd := "UeGqAm8CxYGldMDLoNNt"
	database := "storage_lock_test"
	connectionManager := mysql_storage.NewMySQLConnectionManager(host, port, username, passwd, database)

	// 然后从这个连接管理器创建MySQL Storage
	options := mysql_storage.NewMySQLStorageOptions().SetConnectionManager(connectionManager)
	storage, err := mysql_storage.NewMySQLStorage(context.Background(), options)
	if err != nil {
		panic(err)
	}
	fmt.Println(storage.GetName())

}
