package mysql_storage

import (
	"context"
	_ "github.com/go-sql-driver/mysql"
	sql_based_storage "github.com/storage-lock/go-sql-based-storage"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"strings"
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

func (x *MysqlStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) (returnError error) {
	err := x.SqlBasedStorage.CreateWithVersion(ctx, lockId, version, lockInformation)
	if err != nil {
		msg := err.Error()
		// 不同的版本报错信息可能略有差异，这里就只使用错误码来区分
		// panic: Error 1062 (23000): Duplicate entry '2b690ef6ed8e442d99aaa58147829c89' for key 'PRIMARY'
		// panic: Error 1062 (23000): Duplicate entry 'db0904fe9c3e4b7ab72476cd8a16bd86' for key 'storage_lock.PRIMARY'
		if strings.Contains(msg, "Error 1062 (23000)") {
			return storage_lock.ErrVersionMiss
		}
	}
	return err
}
