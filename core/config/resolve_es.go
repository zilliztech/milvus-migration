package config

import (
	"errors"
	"github.com/spf13/viper"
	"github.com/zilliztech/milvus-migration/core/common"
	"strings"
)

func getSourceESConfig(v *viper.Viper) (*ESConfig, error) {
	security := strings.TrimSpace(v.GetString("source.es.security"))
	var username string
	var password string
	var cert string
	var fingerprint string
	urls := v.GetStringSlice("source.es.urls")
	if urls == nil || len(urls) == 0 {
		return nil, errors.New("ES urls is empty!")
	}
	switch security {
	case common.User:
		username = strings.TrimSpace(v.GetString("source.es.username"))
		password = strings.TrimSpace(v.GetString("source.es.password"))
		if username == "" || password == "" {
			return nil, errors.New("ES username and password cannot be empty!")
		}
	case common.Cert:
		cert = strings.TrimSpace(v.GetString("source.es.cert"))
		if cert == "" {
			return nil, errors.New("ES cert param is empty!")
		}
	case common.FingerPrint:
		fingerprint = strings.TrimSpace(v.GetString("source.es.fingerprint"))
		if fingerprint == "" {
			return nil, errors.New("ES fingerprint param is empty!")
		}
	}
	return &ESConfig{
		Security:    security,
		Username:    username,
		Password:    password,
		Urls:        urls,
		Cert:        cert,
		FingerPrint: fingerprint,
	}, nil
}
