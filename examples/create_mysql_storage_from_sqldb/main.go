package main

import (
	"context"
	"database/sql"
	"fmt"

	mysql_storage "github.com/storage-lock/go-mysql-storage"
	storage "github.com/storage-lock/go-storage"
)

var sqlOpen = sql.Open

func main() {
	// 假设已经在其它地方初始化数据库连接得到了一个*sql.DB
	testDsn := "root:UeGqAm8CxYGldMDLoNNt@tcp(127.0.0.1:3306)/storage_lock_test"
	db, err := sqlOpen("mysql", testDsn)
	if err != nil {
		panic(err)
	}
	storage, err := newMysqlStorageFromDB(context.Background(), db)
	if err != nil {
		panic(err)
	}
	fmt.Println(storage.GetName())
}

func newMysqlStorageFromDB(ctx context.Context, db *sql.DB) (*mysql_storage.MysqlStorage, error) {
	connectionManager := storage.NewFixedSqlDBConnectionManager(db)
	options := mysql_storage.NewMysqlStorageOptions().SetConnectionManager(connectionManager)
	return mysql_storage.NewMysqlStorage(ctx, options)
}
