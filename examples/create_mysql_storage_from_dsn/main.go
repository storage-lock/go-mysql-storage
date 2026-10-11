package main

import (
	"context"
	"fmt"

	mysql_storage "github.com/storage-lock/go-mysql-storage"
)

func main() {
	// 使用一个DSN形式的数据库连接字符串创建ConnectionManager
	testDsn := "root:UeGqAm8CxYGldMDLoNNt@tcp(127.0.0.1:3306)/storage_lock_test"
	storage, err := newMysqlStorageFromDSN(context.Background(), testDsn)
	if err != nil {
		panic(err)
	}
	fmt.Println(storage.GetName())
}

func newMysqlStorageFromDSN(ctx context.Context, dsn string) (*mysql_storage.MysqlStorage, error) {
	connectionManager := mysql_storage.NewMysqlConnectionManagerFromDsn(dsn)
	options := mysql_storage.NewMysqlStorageOptions().SetConnectionManager(connectionManager)
	return mysql_storage.NewMysqlStorage(ctx, options)
}
