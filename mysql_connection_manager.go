package mysql_storage

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/storage-lock/go-storage"
	"sync"
)

// MySQLConnectionManager 创建一个MySQL的连接管理器
type MySQLConnectionManager struct {

	// 主机的名字
	Host string

	// 主机的端口
	Port uint

	// 用户名
	User string

	// 密码
	Passwd string

	DatabaseName string

	// 初始化好的数据库实例
	db   *sql.DB
	err  error
	once sync.Once
}

var _ storage.ConnectionManager[*sql.DB] = &MySQLConnectionManager{}

// NewMySQLConnectionManagerFromDSN 从DSN创建MySQL连接管理器
func NewMySQLConnectionManagerFromDSN(dsn string) storage.ConnectionManager[*sql.DB] {
	return storage.NewDsnConnectionManager("mysql", dsn)
}

// NewMySQLConnectionProvider 从连接属性创建数据库连接
func NewMySQLConnectionProvider(host string, port uint, user, passwd, database string) *MySQLConnectionManager {
	return &MySQLConnectionManager{
		Host:         host,
		Port:         port,
		User:         user,
		Passwd:       passwd,
		DatabaseName: database,
	}
}

func (x *MySQLConnectionManager) SetHost(host string) *MySQLConnectionManager {
	x.Host = host
	return x
}

func (x *MySQLConnectionManager) SetPort(port uint) *MySQLConnectionManager {
	x.Port = port
	return x
}

func (x *MySQLConnectionManager) SetUser(user string) *MySQLConnectionManager {
	x.User = user
	return x
}

func (x *MySQLConnectionManager) SetPasswd(passwd string) *MySQLConnectionManager {
	x.Passwd = passwd
	return x
}

func (x *MySQLConnectionManager) SetDatabaseName(databaseName string) *MySQLConnectionManager {
	x.DatabaseName = databaseName
	return x
}

func (x *MySQLConnectionManager) Name() string {
	return "mysql-connection-manager"
}

// Take 获取到数据库的连接
func (x *MySQLConnectionManager) Take(ctx context.Context) (*sql.DB, error) {
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

func (x *MySQLConnectionManager) GetDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", x.User, x.Passwd, x.Host, x.Port, x.DatabaseName)
}

func (x *MySQLConnectionManager) Return(ctx context.Context, db *sql.DB) error {
	return nil
}

func (x *MySQLConnectionManager) Shutdown(ctx context.Context) error {
	if x.db != nil {
		return x.db.Close()
	}
	return nil
}
