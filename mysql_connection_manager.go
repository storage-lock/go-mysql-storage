package mysql_storage

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/storage-lock/go-storage"
	"sync"
)

// MysqlConnectionManager 创建一个MySQL的连接管理器
type MysqlConnectionManager struct {

	// 主机的名字
	Host string

	// 主机的端口
	Port uint

	// 用户名
	User string

	// 密码
	Passwd string

	DatabaseName string

	DSN string

	// 初始化好的数据库实例
	db   *sql.DB
	err  error
	once sync.Once
}

var _ storage.ConnectionManager[*sql.DB] = &MysqlConnectionManager{}

// NewMysqlConnectionManagerFromDSN 从DSN创建MySQL连接管理器
func NewMysqlConnectionManagerFromDSN(dsn string) *MysqlConnectionManager {
	return &MysqlConnectionManager{
		DSN: dsn,
	}
}

// NewMysqlConnectionManager 从连接属性创建数据库连接
func NewMysqlConnectionManager(host string, port uint, user, passwd, database string) *MysqlConnectionManager {
	return &MysqlConnectionManager{
		Host:         host,
		Port:         port,
		User:         user,
		Passwd:       passwd,
		DatabaseName: database,
	}
}

func (x *MysqlConnectionManager) SetHost(host string) *MysqlConnectionManager {
	x.Host = host
	return x
}

func (x *MysqlConnectionManager) SetPort(port uint) *MysqlConnectionManager {
	x.Port = port
	return x
}

func (x *MysqlConnectionManager) SetUser(user string) *MysqlConnectionManager {
	x.User = user
	return x
}

func (x *MysqlConnectionManager) SetPasswd(passwd string) *MysqlConnectionManager {
	x.Passwd = passwd
	return x
}

func (x *MysqlConnectionManager) SetDatabaseName(databaseName string) *MysqlConnectionManager {
	x.DatabaseName = databaseName
	return x
}

const MysqlConnectionManagerName = "mysql-connection-manager"

func (x *MysqlConnectionManager) Name() string {
	return MysqlConnectionManagerName
}

// Take 获取到数据库的连接
func (x *MysqlConnectionManager) Take(ctx context.Context) (*sql.DB, error) {
	x.once.Do(func() {
		db, err := sql.Open("mysql", x.GetDSN())
		if err != nil {
			x.err = err
			return
		}
		x.db = db
	})
	return x.db, x.err
}

func (x *MysqlConnectionManager) GetDSN() string {
	if x.DSN != "" {
		return x.DSN
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", x.User, x.Passwd, x.Host, x.Port, x.DatabaseName)
}

func (x *MysqlConnectionManager) Return(ctx context.Context, db *sql.DB) error {
	return nil
}

func (x *MysqlConnectionManager) Shutdown(ctx context.Context) error {
	if x.db != nil {
		return x.db.Close()
	}
	return nil
}
