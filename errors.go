package mysql_storage

import "fmt"

var (
	ErrMysqlStorageOptionsConnectionManagerNil = fmt.Errorf("MysqlStorageOptions.ConnectionManager can not nil")
)
