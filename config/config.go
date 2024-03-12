package config

import (
	"github.com/spf13/viper"
)

const defaultFilePath string = "./config/config.toml"

func Init() error {
	viper.SetConfigFile(defaultFilePath)
	err := viper.ReadInConfig()
	if err != nil {
		return err
	}
	return nil
}
