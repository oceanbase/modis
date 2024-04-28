/*-
 * #%L
 * Modis
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * Modis is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

type LogConfig struct {
	// log file dir
	FilePath string `mapstructure:"filepath" json:"filepath" yaml:"filepath"`
	// log file size（MB）
	SingleFileMaxSize int `mapstructure:"single-file-max-size" json:"single-file-max-size" yaml:"single-file-max-size"`
	// Maximum number of old files to keep
	MaxBackupFileSize int `mapstructure:"max-backup-file-size" json:"max-backup-file-size" yaml:"max-backup-file-size"`
	// Maximum number of days to keep old files
	MaxAgeFileRem int `mapstructure:"max-age-file-rem" json:"max-age-file-rem" yaml:"max-age-file-rem"`
	// Whether to compress/archive old files
	Compress bool `mapstructure:"compress" json:"compress" yaml:"compress"`
	// log level
	Level string `mapstructure:"level" json:"level" yaml:"level"`
}

type ObkvStorageConfig struct {
	ConfigServerUrl    string `mapstructure:"config-server-url" json:"config-server-url" yaml:"config-server-url"`
	FullUserName       string `mapstructure:"full-user-name" json:"full-user-name" yaml:"full-user-name"`
	Password           string `mapstructure:"password" json:"password" yaml:"password"`
	SysUserName        string `mapstructure:"sys-user-name" json:"sys-user-name" yaml:"sys-user-name"`
	SysPassword        string `mapstructure:"sys-password" json:"sys-password" yaml:"sys-password"`
	ConnectionPoolSize int    `mapstructure:"connection-pool-size" json:"connection-pool-size" yaml:"connection-pool-size"`
}

type ServerConfig struct {
	Listen        string `mapstructure:"listen" json:"listen" yaml:"listen"`
	MaxConnection int    `mapstructure:"max-connection" json:"max-connection" yaml:"max-connection"`
	Password      string `mapstructure:"password" json:"password" yaml:"password"`
	TLS
}

type StorageConfig struct {
	Backend    string            `mapstructure:"backend" json:"backend" yaml:"backend"`
	ObkvConfig ObkvStorageConfig `mapstructure:"obkv" json:"obkv" yaml:"obkv"`
}

type TLS struct {
	SSLCertFile string `mapstructure:"ssl-cert-file" json:"ssl-cert-file" yaml:"ssl-cert-file"`
	SSLKeyFile  string `mapstructure:"ssl-key-file" json:"ssl-key-file" yaml:"ssl-key-file"`
}

type Config struct {
	Server  ServerConfig  `mapstructure:"server" json:"server" yaml:"server"`
	Log     LogConfig     `mapstructure:"log" json:"log" yaml:"log"`
	Storage StorageConfig `mapstructure:"storage" json:"storage" yaml:"storage"`
}

const (
	// DefaultConfigEnv config settings config env
	defaultConfigEnv = "modisConfig"
	// DefaultConfigFilePath config settings config file
	defaultConfigFilePath = "config/config.yaml"
)

var (
	// defaultGlobalConfig global config object
	DefaultGlobalConfig Config
)

func LoadConfig(path ...string) (*viper.Viper, error) {
	var config string
	// Command line > Environment variable > Default values
	if len(path) == 0 {
		if configEnv := os.Getenv(defaultConfigEnv); configEnv == "" {
			config = defaultConfigFilePath
			fmt.Printf("Using the default value for config, config path: %v\n", config)
		} else {
			config = configEnv
			fmt.Printf("Using the defaultGlobalConfig environment variable, config path: %v\n", config)
		}
	} else {
		config = path[0]
		fmt.Printf("Using the specified variable, config path: %v\n", config)
	}

	v := viper.New()
	v.SetConfigFile(config)
	err := v.ReadInConfig()
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	v.WatchConfig()
	v.OnConfigChange(func(e fsnotify.Event) {
		fmt.Println("config file changed:", e.Name)
		if err := v.Unmarshal(&DefaultGlobalConfig); err != nil {
			fmt.Println(err)
		}
	})
	if err := v.Unmarshal(&DefaultGlobalConfig); err != nil {
		fmt.Println(err)
		return nil, err
	}

	fmt.Println("init config finished")
	var out bytes.Buffer
	c, err := json.Marshal(DefaultGlobalConfig)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	err = json.Indent(&out, c, "", "  ")
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	fmt.Printf("your config is:%s\n", out.String())

	return v, nil
}
