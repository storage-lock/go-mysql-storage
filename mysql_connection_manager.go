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
	// initMu 保护初始化状态。原用 sync.Once，sql.Open 首次失败后 err 永久缓存无法重试
	// （漏洞4/liveness，不破互斥性——拿不到 db 则 CAS 无法发起）。改为 mutex + initialized：
	// 成功后缓存 db 不重复 Open；失败不缓存 err 可重试。
	db          *sql.DB
	initMu      sync.Mutex
	initialized bool
}

var _ storage.ConnectionManager[*sql.DB] = &MysqlConnectionManager{}

// NewMysqlConnectionManagerFromDsn 从DSN创建MySQL连接管理器
func NewMysqlConnectionManagerFromDsn(dsn string) *MysqlConnectionManager {
	return &MysqlConnectionManager{
		DSN: dsn,
	}
}

func NewMysqlConnectionManagerFromSqlDb(db *sql.DB) *MysqlConnectionManager {
	return &MysqlConnectionManager{
		db: db,
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
//
// 漏洞4 修复：原 sync.Once 在 sql.Open 失败时永久缓存 err 无法重试（liveness，不破互斥性）。
// 改为 mutex + initialized：成功后缓存 db 不重复 Open；失败不缓存 err 可重试。
func (x *MysqlConnectionManager) Take(ctx context.Context) (*sql.DB, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	if x.initialized {
		return x.db, nil
	}
	db, err := sql.Open("mysql", x.GetDSN())
	if err != nil {
		// 失败不缓存，下次 Take 可重试
		return nil, err
	}
	x.db = db
	x.initialized = true
	return x.db, nil
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
	x.initMu.Lock()
	db := x.db
	x.initMu.Unlock()
	if db != nil {
		return db.Close()
	}
	return nil
}
