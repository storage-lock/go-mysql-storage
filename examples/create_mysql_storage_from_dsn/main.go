package main

import (
	"context"
	"fmt"
	mysql_storage "github.com/storage-lock/go-mysql-storage"
)

func main() {

	// 使用一个DSN形式的数据库连接字符串创建ConnectionManager
	testDsn := "root:UeGqAm8CxYGldMDLoNNt@tcp(127.0.0.1:3306)/storage_lock_test"
	connectionManager := mysql_storage.NewMysqlConnectionManagerFromDSN(testDsn)

	// 然后从这个ConnectionManager创建MySQL Storage
	options := mysql_storage.NewMySQLStorageOptions().SetConnectionManager(connectionManager)
	storage, err := mysql_storage.NewMysqlStorage(context.Background(), options)
	if err != nil {
		panic(err)
	}
	fmt.Println(storage.GetName())

}
