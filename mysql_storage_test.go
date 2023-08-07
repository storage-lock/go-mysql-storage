package mysql_storage

import (
	"context"
	"github.com/storage-lock/go-storage"
	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewMySQLStorage(t *testing.T) {
	envName := "STORAGE_LOCK_MYSQL_DSN"
	dsn := os.Getenv(envName)
	assert.NotEmpty(t, dsn)
	connectionGetter := NewMySQLConnectionManagerFromDSN(dsn)
	s, err := NewMySQLStorage(context.Background(), &MySQLStorageOptions{
		ConnectionManager: connectionGetter,
		TableName:         storage.DefaultStorageTableName,
	})
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}
