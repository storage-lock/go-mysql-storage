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

func NewMysqlStorageOptions() *MysqlStorageOptions {
	return &MysqlStorageOptions{
		TableName: storage.DefaultStorageTableName,
	}
}

// NewMySQLStorageOptions 兼容别名：2023 年 b093d38 把 NewMySQLStorageOptions 改名为
// NewMysqlStorageOptions，但已发布的旧版本对外暴露的一直是 NewMySQLStorageOptions，
// go-mysql-locks/go-mariadb-locks/storage-lock-cli 及 README 全部沿用旧名。补上别名后
// 新旧两种写法都能编译，避免下游仅因大小写改名而构建失败。
func NewMySQLStorageOptions() *MysqlStorageOptions {
	return NewMysqlStorageOptions()
}

func (x *MysqlStorageOptions) SetTableName(tableName string) *MysqlStorageOptions {
	x.TableName = tableName
	return x
}

func (x *MysqlStorageOptions) SetConnectionManager(connManager storage.ConnectionManager[*sql.DB]) *MysqlStorageOptions {
	x.ConnectionManager = connManager
	return x
}

func (x *MysqlStorageOptions) Check() error {
	if x.TableName == "" {
		x.TableName = storage.DefaultStorageTableName
	}
	if x.ConnectionManager == nil {
		return ErrMysqlStorageOptionsConnectionManagerNil
	}
	return nil
}
