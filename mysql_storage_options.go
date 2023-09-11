package mysql_storage

import (
	"database/sql"
	"github.com/storage-lock/go-storage"
)

// MysqlStorageOptions 基于MySQL为存储引擎时的选项
type MysqlStorageOptions struct {

	// 存放锁的表的名字，如果未指定的话则使用默认的表
	TableName string

	// 用于获取数据库连接
	ConnectionManager storage.ConnectionManager[*sql.DB]
}

func NewMySQLStorageOptions() *MysqlStorageOptions {
	return &MysqlStorageOptions{
		TableName: storage.DefaultStorageTableName,
	}
}

func (x *MysqlStorageOptions) SetConnectionManager(connManager storage.ConnectionManager[*sql.DB]) *MysqlStorageOptions {
	x.ConnectionManager = connManager
	return x
}

func (x *MysqlStorageOptions) SetTableName(tableName string) *MysqlStorageOptions {
	x.TableName = tableName
	return x
}

func (x *MysqlStorageOptions) Check() error {

	if x.TableName == "" {
		x.TableName = storage.DefaultStorageDatabaseName
	}

	if x.ConnectionManager == nil {
		return ErrMysqlStorageOptionsConnectionManagerNil
	}

	return nil
}
