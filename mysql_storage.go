package mysql_storage

import (
	"context"
	_ "github.com/go-sql-driver/mysql"
	sql_based_storage "github.com/storage-lock/go-sql-based-storage"
	"github.com/storage-lock/go-storage"
)

// MysqlStorage 基于Mysql的存储
type MysqlStorage struct {
	// mysql的操作基本都是支持sql92语法的
	*sql_based_storage.SqlBasedStorage

	options *MysqlStorageOptions
}

var _ storage.Storage = &MysqlStorage{}

func NewMysqlStorage(ctx context.Context, options *MysqlStorageOptions) (*MysqlStorage, error) {

	// 参数检查
	if err := options.Check(); err != nil {
		return nil, err
	}

	// sql storage的基础Storage
	baseStorageOption := sql_based_storage.NewSqlBasedStorageOptions().
		SetConnectionManager(options.ConnectionManager).
		SetSqlProvider(sql_based_storage.NewSql92Provider()).
		SetTableFullName(options.TableName)
	baseStorage, err := sql_based_storage.NewSqlBasedStorage(baseStorageOption)
	if err != nil {
		return nil, err
	}

	s := &MysqlStorage{
		SqlBasedStorage: baseStorage,
		options:         options,
	}

	err = s.Init(ctx)
	if err != nil {
		return nil, err
	}

	return s, nil
}

const StorageName = "mysql-storage"

func (x *MysqlStorage) GetName() string {
	return StorageName
}
