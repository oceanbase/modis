/*
 * Copyright (c) 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
	ChannelSize   int    `mapstructure:"channel-size" json:"channel-size" yaml:"channel-size"`
	Password      string `mapstructure:"password" json:"password" yaml:"password"`
	DBNum         int64  `mapstructure:"databases" json:"databases" yaml:"databases"`
	Supervised    string `mapstructure:"supervised" json:"supervised" yaml:"supervised"`
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

func printConfig() error {
	var out bytes.Buffer
	c, err := json.Marshal(DefaultGlobalConfig)
	if err != nil {
		fmt.Println(err)
		return err
	}
	err = json.Indent(&out, c, "", "  ")
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Printf("your config is:%s\n", out.String())
	return nil
}

func LoadConfig(path ...string) (*viper.Viper, error) {
	fmt.Println("start to load config...")
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
		fmt.Println("config file changed:", e.String())
		if err := v.Unmarshal(&DefaultGlobalConfig); err != nil {
			fmt.Println(err)
			return
		}
		// printConfig()
	})
	if err := v.Unmarshal(&DefaultGlobalConfig); err != nil {
		fmt.Println(err)
		return nil, err
	}

	fmt.Println("load config finished")
	// err = printConfig()
	// if err != nil {
	// 	return nil, err
	// }
	return v, nil
}
