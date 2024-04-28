/*-
 * #%L
 * Modis
 * %%
 * Copyright (C) 2024 OceanBase
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
	return &ClientConfig{
		configUrl:    cfg.ConfigServerUrl,
		fullUserName: cfg.FullUserName,
		password:     cfg.Password,
		sysUserName:  cfg.SysUserName,
		sysPassword:  cfg.SysPassword,
		cfg:          cliCfg,
	}
}
