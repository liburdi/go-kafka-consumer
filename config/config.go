package config

import (
	"github.com/spf13/viper"
)

func Init() error {
	viper.SetConfigFile("./config/config.toml")
	err := viper.ReadInConfig()
	if err != nil {
		return err
	}
	return nil
}
