package main

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	mysql_storage "github.com/storage-lock/go-mysql-storage"
	"github.com/stretchr/testify/assert"
)

func TestNewMysqlStorageFromDSN_InitFailsWithoutDatabase(t *testing.T) {
	_, err := newMysqlStorageFromDSN(context.Background(), "user:pass@tcp(127.0.0.1:1)/db")
	assert.Error(t, err)
}

func TestMain_PrintsStorageName(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mock.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	orig := mysql_storage.MysqlOpen
	mysql_storage.MysqlOpen = func(string, string) (*sql.DB, error) { return db, nil }
	defer func() { mysql_storage.MysqlOpen = orig }()
	assert.NotPanics(t, main)
}

func TestMain_PanicsWhenInitFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mock.ExpectExec(".").WillReturnError(assert.AnError)
	orig := mysql_storage.MysqlOpen
	mysql_storage.MysqlOpen = func(string, string) (*sql.DB, error) { return db, nil }
	defer func() { mysql_storage.MysqlOpen = orig }()
	assert.Panics(t, main)
}
