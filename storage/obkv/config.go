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

package obkv

import (
	obkvcfg "github.com/oceanbase/obkv-table-client-go/config"

	"github.com/oceanbase/modis/config"
)

type Config struct {
	cliCfg *ClientConfig
}

func NewConfig(cfg *config.ObkvStorageConfig) *Config {
	return &Config{
		cliCfg: NewClientConfig(cfg),
	}
}

type ClientConfig struct {
	configUrl    string
	fullUserName string
	password     string
	sysUserName  string
	sysPassword  string
	cfg          *obkvcfg.ClientConfig
}

func NewClientConfig(cfg *config.ObkvStorageConfig) *ClientConfig {
	cliCfg := obkvcfg.NewDefaultClientConfig()
	cliCfg.ConnPoolMaxConnSize = cfg.ConnectionPoolSize
	cliCfg.NeedCalculateGenerateColumn = false
	return &ClientConfig{
		configUrl:    cfg.ConfigServerUrl,
		fullUserName: cfg.FullUserName,
		password:     cfg.Password,
		sysUserName:  cfg.SysUserName,
		sysPassword:  cfg.SysPassword,
		cfg:          cliCfg,
	}
}
