package config

import (
	"errors"
	"github.com/spf13/viper"
	"strings"
)

func getSourceESConfig(v *viper.Viper) (*ESConfig, error) {

	var username string
	var password string
	var cert string
	var fingerprint string
	var serviceToken string
	var cloudId string
	var apiKey string
	var urls []string

	cloudId = strings.TrimSpace(v.GetString("source.es.cloudId"))
	if len(cloudId) > 0 {
		//use elastic cloud case:
		apiKey = strings.TrimSpace(v.GetString("source.es.apiKey"))
		if len(apiKey) == 0 {
			return nil, errors.New("ES Cloud apiKey is empty!")
		}
	} else {
		//use self es server
		urls = v.GetStringSlice("source.es.urls")
		if urls == nil || len(urls) == 0 {
			return nil, errors.New("ES urls and cloudId cannot both be empty!")
		}
		serviceToken = strings.TrimSpace(v.GetString("source.es.serviceToken"))
		if len(serviceToken) == 0 {
			//if serviceToken is empty then use user/pwd login to es, else use serviceToken.
			username = strings.TrimSpace(v.GetString("source.es.username"))
			password = strings.TrimSpace(v.GetString("source.es.password"))
			cert = strings.TrimSpace(v.GetString("source.es.cert"))
			fingerprint = strings.TrimSpace(v.GetString("source.es.fingerprint"))
		}
	}
	return &ESConfig{
		CloudId:      cloudId,
		ApiKey:       apiKey,
		ServiceToken: serviceToken,
		Username:     username,
		Password:     password,
		Urls:         urls,
		Cert:         cert,
		FingerPrint:  fingerprint,
	}, nil
}
