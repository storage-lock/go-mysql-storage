package main

import (
	"context"
	"fmt"

	mysql_storage "github.com/storage-lock/go-mysql-storage"
)

func main() {
	// 数据库连接不是DSN的形式，就是一堆零散的属性，则依次设置，可以得到一个连接管理器
	storage, err := newMysqlStorageFromProperties(context.Background(), "127.0.0.1", 3306, "root", "UeGqAm8CxYGldMDLoNNt", "storage_lock_test")
	if err != nil {
		panic(err)
	}
	fmt.Println(storage.GetName())
}

func newMysqlStorageFromProperties(ctx context.Context, host string, port uint, username, passwd, database string) (*mysql_storage.MysqlStorage, error) {
	connectionManager := mysql_storage.NewMysqlConnectionManager(host, port, username, passwd, database)
	options := mysql_storage.NewMysqlStorageOptions().SetConnectionManager(connectionManager)
	return mysql_storage.NewMysqlStorage(ctx, options)
}
