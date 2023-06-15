package config

import (
	"errors"
	"fmt"
	"github.com/spf13/viper"
)

func resolveMetaConfig(v *viper.Viper) (*MetaConfig, error) {

	metaMode := v.GetString("meta.mode")
	if metaMode == "" {
		return nil, fmt.Errorf("[meta.mode] can not empty")
	}

	switch metaMode {
	case "mock":
		return resolveMetaInMock(v)
	case "local":
		return resolveMetaInLocal(v)
	case "sqlite":
		return resolveMetaInSqlite(v)
	case "mysql":
		return resolveMetaInMysql(v)
	case "remote":
		return resolveMetaInRemote(v)
	default:
		msg := fmt.Sprintf("invalid [meta.mode], %s", metaMode)
		return nil, errors.New(msg)
	}
}

func resolveMetaInRemote(v *viper.Viper) (*MetaConfig, error) {
	metaFile := v.GetString("meta.remoteMetaFile")
	if metaFile == "" {
		return nil, errors.New("empty [meta.remoteMetaFile], pls check config")
	}
	return &MetaConfig{
		MetaMode:       "remote",
		RemoteMetaFile: metaFile,
	}, nil
}

func resolveMetaInMock(v *viper.Viper) (*MetaConfig, error) {
	mockFile := v.GetString("meta.mockFile")
	if mockFile == "" {
		return nil, errors.New("empty [meta.mockFile], pls check config")
	}

	return &MetaConfig{
		MetaMode:      "mock",
		LocalMockFile: mockFile,
	}, nil
}

func resolveMetaInLocal(v *viper.Viper) (*MetaConfig, error) {
	mockFile := v.GetString("meta.localFile")
	if mockFile == "" {
		return nil, errors.New("empty [meta.localFile], pls check config")
	}

	return &MetaConfig{
		MetaMode:      "local",
		LocalMockFile: mockFile,
	}, nil
}

func resolveMetaInSqlite(v *viper.Viper) (*MetaConfig, error) {
	sqliteFile := v.GetString("meta.sqliteFile")
	if sqliteFile == "" {
		return nil, errors.New("empty [meta.sqliteFile], pls check config")
	}

	return &MetaConfig{
		MetaMode:        "sqlite",
		LocalSqliteFile: sqliteFile,
	}, nil
}

func resolveMetaInMysql(v *viper.Viper) (*MetaConfig, error) {
	url := v.GetString("meta.mysqlUrl")
	if url == "" {
		return nil, errors.New("empty [meta.mysqlUrl], pls check config")
	}

	return &MetaConfig{
		MetaMode:      "mysql",
		LocalMysqlURL: url,
	}, nil
}
