package config

import (
	"errors"
	"github.com/spf13/viper"
)

func notEmpty(key string, v *viper.Viper) error {
	val := v.GetString(key)
	if val == "" {
		return errors.New(key + " can not be empty")
	}

	return nil
}
