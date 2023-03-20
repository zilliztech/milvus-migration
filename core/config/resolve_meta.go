package config

import (
	"errors"
	"fmt"
	"github.com/spf13/viper"
)

func resolveMetaConfig(sourceMode string, v *viper.Viper) (*MetaConfig, error) {
	switch sourceMode {
	case "remote":
		return resolveMetaConfigInRemote(v)
	case "local":
		return resolveMetaConfigInLocal(v)
	default:
		msg := fmt.Sprintf("invalid [source.mode], %s", sourceMode)
		return nil, errors.New(msg)
	}
}

func resolveMetaConfigInRemote(v *viper.Viper) (*MetaConfig, error) {
	metaFile := v.GetString("source.remote.metaFile")
	if metaFile == "" {
		return nil, errors.New("meta file is empty, please check config [source.remote.metaFile]")
	}
	return &MetaConfig{
		MetaMode:       "remote",
		RemoteMetaFile: metaFile,
	}, nil
}

func resolveMetaConfigInLocal(v *viper.Viper) (*MetaConfig, error) {
	metaMode := v.GetString("source.local.meta.mode")
	switch metaMode {
	case "mock":
		return resolveLocalMetaInMock(metaMode, v)
	case "sqlite":
		return resolveLocalMetaInSqlite(metaMode, v)
	case "mysql":
		return resolveLocalMetaInMysql(metaMode, v)
	default:
		msg := fmt.Sprintf("invalid [source.local.meta.mode], %s", metaMode)
		return nil, errors.New(msg)
	}

}

func resolveLocalMetaInMock(metaMode string, v *viper.Viper) (*MetaConfig, error) {
	mockFile := v.GetString("source.local.meta.mockFile")
	if mockFile == "" {
		return nil, errors.New("empty [source.local.meta.mockFile], pls check config")
	}

	return &MetaConfig{
		MetaMode:      metaMode,
		LocalMockFile: mockFile,
	}, nil
}

func resolveLocalMetaInSqlite(metaMode string, v *viper.Viper) (*MetaConfig, error) {
	sqliteFile := v.GetString("source.local.meta.sqliteFile")
	if sqliteFile == "" {
		return nil, errors.New("empty [source.local.meta.sqliteFile], pls check config")
	}

	return &MetaConfig{
		MetaMode:        metaMode,
		LocalSqliteFile: sqliteFile,
	}, nil
}

func resolveLocalMetaInMysql(metaMode string, v *viper.Viper) (*MetaConfig, error) {
	url := v.GetString("source.local.meta.mysql.url")
	if url == "" {
		return nil, errors.New("empty [source.local.meta.mysql.url], pls check config")
	}

	return &MetaConfig{
		MetaMode:      metaMode,
		LocalMysqlURL: url,
	}, nil
}
