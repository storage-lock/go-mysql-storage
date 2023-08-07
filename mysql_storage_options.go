package mysql_storage

import (
	"database/sql"
	"github.com/storage-lock/go-storage"
)

// MySQLStorageOptions 基于MySQL为存储引擎时的选项
type MySQLStorageOptions struct {

	// 存放锁的表的名字，如果未指定的话则使用默认的表
	TableName string

	// 用于获取数据库连接
	ConnectionManager storage.ConnectionManager[*sql.DB]
}

func NewMySQLStorageOptions() *MySQLStorageOptions {
	return &MySQLStorageOptions{
		TableName: storage.DefaultStorageTableName,
	}
}

func (x *MySQLStorageOptions) SetConnectionManager(connManager storage.ConnectionManager[*sql.DB]) *MySQLStorageOptions {
	x.ConnectionManager = connManager
	return x
}

func (x *MySQLStorageOptions) SetTableName(tableName string) *MySQLStorageOptions {
	x.TableName = tableName
	return x
}
