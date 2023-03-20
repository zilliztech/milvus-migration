package data

import "github.com/spf13/viper"

type TempConfig struct {
	YamlPath string
	ViperV   *viper.Viper

	CollectionDim int
}
