package mysql_storage

import (
	"context"
	_ "github.com/go-sql-driver/mysql"
	sql_based_storage "github.com/storage-lock/go-sql-based-storage"
	"github.com/storage-lock/go-storage"
)

type MySQLStorage struct {
	*sql_based_storage.SqlBasedStorage
	options *MySQLStorageOptions
}

var _ storage.Storage = &MySQLStorage{}

func NewMySQLStorage(ctx context.Context, options *MySQLStorageOptions) (*MySQLStorage, error) {

	// 参数检查
	if err := options.Check(); err != nil {
		return nil, err
	}

	// sql storage的基础Storage
	baseStorageOption := sql_based_storage.NewSqlBasedStorageOptions().
		SetConnectionManager(options.ConnectionManager).
		SetSqlProvider(sql_based_storage.NewSql97Provider()).
		SetTableFullName(options.TableName)
	baseStorage, err := sql_based_storage.NewSqlBasedStorage(baseStorageOption)
	if err != nil {
		return nil, err
	}

	s := &MySQLStorage{
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

func (x *MySQLStorage) GetName() string {
	return StorageName
}
