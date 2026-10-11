package mysql_storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

type takeErrorManager struct{}

func (takeErrorManager) Name() string { return "take-error" }
func (takeErrorManager) Take(context.Context) (*sql.DB, error) {
	return nil, errors.New("take failed")
}
func (takeErrorManager) Return(context.Context, *sql.DB) error { return nil }
func (takeErrorManager) Shutdown(context.Context) error        { return nil }

type returnErrorManager struct{ db *sql.DB }

func (returnErrorManager) Name() string { return "return-error" }
func (x returnErrorManager) Take(context.Context) (*sql.DB, error) {
	return x.db, nil
}
func (returnErrorManager) Return(context.Context, *sql.DB) error {
	return errors.New("return failed")
}
func (returnErrorManager) Shutdown(context.Context) error { return nil }

type defaultDriver struct{}

func (defaultDriver) Open(string) (driver.Conn, error) { return defaultConn{}, nil }

type defaultConn struct{}

func (defaultConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (defaultConn) Close() error                        { return nil }
func (defaultConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }

func init() {
	sql.Register("mysql-coverage", defaultDriver{})
}

func TestMysqlConnectionManager(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectClose()

	fromDSN := NewMysqlConnectionManagerFromDsn("user:pass@tcp(h:1)/db")
	assert.Equal(t, MysqlConnectionManagerName, fromDSN.Name())
	assert.Equal(t, "user:pass@tcp(h:1)/db", fromDSN.GetDSN())
	fromDSN.open = func(string, string) (*sql.DB, error) { return nil, errors.New("open failed") }
	_, err = fromDSN.Take(context.Background())
	assert.EqualError(t, err, "open failed")
	fromDSN.open = func(string, string) (*sql.DB, error) { return db, nil }
	got, err := fromDSN.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, db, got)
	again, err := fromDSN.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, got, again)
	assert.NoError(t, fromDSN.Return(context.Background(), got))
	assert.NoError(t, fromDSN.Shutdown(context.Background()))

	fromDB := NewMysqlConnectionManagerFromSqlDb(db)
	got, err = fromDB.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, db, got)

	fromConfig := NewMysqlConnectionManager("h", 3306, "u", "p", "db").SetHost("db.local")
	assert.Equal(t, "db.local", fromConfig.Host)
	assert.Equal(t, "u:p@tcp(db.local:3306)/db", fromConfig.GetDSN())
	assert.NoError(t, (&MysqlConnectionManager{}).Shutdown(context.Background()))

	defaultManager := NewMysqlConnectionManagerFromDsn("u:p@tcp(127.0.0.1:1)/db")
	opened, err := defaultManager.Take(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, opened)
	assert.NoError(t, defaultManager.Shutdown(context.Background()))

	hooked := NewMysqlConnectionManagerFromDsn("u:p@tcp(127.0.0.1:1)/db")
	hooked.open = func(driverName, dsn string) (*sql.DB, error) {
		return sql.Open("mysql-coverage", dsn)
	}
	opened, err = hooked.Take(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, opened)
	assert.NoError(t, hooked.Shutdown(context.Background()))
}

func TestNewMysqlStorage_Branches(t *testing.T) {
	_, err := NewMysqlStorage(context.Background(), NewMysqlStorageOptions())
	assert.ErrorIs(t, err, ErrMysqlStorageOptionsConnectionManagerNil)

	_, err = NewMysqlStorage(context.Background(), &MysqlStorageOptions{ConnectionManager: takeErrorManager{}, TableName: "locks"})
	assert.EqualError(t, err, "take failed")

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mock.ExpectExec("CREATE TABLE").WillReturnError(errors.New("create failed"))
	mock.ExpectClose()
	_, err = NewMysqlStorage(context.Background(), &MysqlStorageOptions{
		ConnectionManager: returnErrorManager{db: db},
		TableName:         "locks",
	})
	assert.Error(t, err)

	db2, mock2, err := sqlmock.New()
	assert.NoError(t, err)
	defer db2.Close()
	mock2.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	s, err := NewMysqlStorage(context.Background(), &MysqlStorageOptions{
		ConnectionManager: storage.NewFixedSqlDBConnectionManager(db2),
		TableName:         "locks",
	})
	assert.NoError(t, err)
	info := &storage.LockInformation{OwnerId: "owner"}
	mock2.ExpectExec("INSERT INTO").WillReturnError(errors.New("Error 1062 (23000): Duplicate entry"))
	assert.ErrorIs(t, s.CreateWithVersion(context.Background(), "lock", 1, info), storage_lock.ErrLockAlreadyExists)
	mock2.ExpectExec("INSERT INTO").WillReturnError(errors.New("other"))
	assert.EqualError(t, s.CreateWithVersion(context.Background(), "lock", 1, info), "other")
	mock2.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
	assert.NoError(t, s.CreateWithVersion(context.Background(), "lock", 1, info))

	_, err = (&MysqlStorage{}).Watch(context.Background(), "")
	assert.Error(t, err)

	empty := &MysqlStorageOptions{ConnectionManager: storage.NewFixedSqlDBConnectionManager(&sql.DB{})}
	assert.NoError(t, empty.Check())
	assert.Equal(t, storage.DefaultStorageTableName, empty.TableName)
	assert.Equal(t, storage.DefaultStorageTableName, NewMySQLStorageOptions().TableName)
}
