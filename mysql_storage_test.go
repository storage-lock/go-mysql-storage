package mysql_storage

import (
	"context"
	"database/sql"
	"testing"

	"github.com/storage-lock/go-storage"
	"github.com/stretchr/testify/assert"
)

func TestNewMysqlStorageOptions(t *testing.T) {
	opts := NewMysqlStorageOptions()
	assert.NotNil(t, opts)
	assert.Equal(t, storage.DefaultStorageTableName, opts.TableName)
	assert.Nil(t, opts.ConnectionManager)
}

func TestMysqlStorageOptions_SetConnectionManager(t *testing.T) {
	opts := NewMysqlStorageOptions()
	assert.NotNil(t, opts)
	db := &sql.DB{}
	connManager := storage.NewFixedSqlDBConnectionManager(db)
	opts.SetConnectionManager(connManager)
	assert.NotNil(t, opts.ConnectionManager)
}

func TestMysqlStorageOptions_SetTableName(t *testing.T) {
	opts := NewMysqlStorageOptions()
	assert.NotNil(t, opts)
	opts.SetTableName("custom_table")
	assert.Equal(t, "custom_table", opts.TableName)
}

func TestMysqlStorageOptions_Check_NilConnectionManager(t *testing.T) {
	opts := NewMysqlStorageOptions()
	assert.NotNil(t, opts)
	err := opts.Check()
	assert.Error(t, err)
	assert.Equal(t, ErrMysqlStorageOptionsConnectionManagerNil, err)
}

func TestMysqlStorageOptions_Check_ValidOptions(t *testing.T) {
	opts := NewMysqlStorageOptions()
	assert.NotNil(t, opts)
	db := &sql.DB{}
	connManager := storage.NewFixedSqlDBConnectionManager(db)
	opts.SetConnectionManager(connManager)
	err := opts.Check()
	assert.NoError(t, err)
}

func TestNewMysqlStorage_NilOptions(t *testing.T) {
	assert.Panics(t, func() {
		NewMysqlStorage(context.Background(), nil)
	})
}

func TestMysqlStorage_GetName(t *testing.T) {
	s := &MysqlStorage{}
	name := s.GetName()
	assert.Equal(t, StorageName, name)
}

func TestMysqlStorage_Capabilities(t *testing.T) {
	s := &MysqlStorage{}
	caps := s.Capabilities()
	assert.NotNil(t, caps)
	assert.Len(t, caps, 3)
	assert.Equal(t, storage.CapabilityCAS, caps[0])
	assert.Equal(t, storage.CapabilityReliableTime, caps[1])
	assert.Equal(t, storage.CapabilityWatch, caps[2])
}
