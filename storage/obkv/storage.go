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
	"github.com/oceanbase/obkv-table-client-go/client"
)

const (
	dbColumnName     = "db"
	keyColumnName    = "rkey"
	valueColumnName  = "value"
	expireColumnName = "expire_ts"
)

type Storage struct {
	cli client.Client
	cfg *Config
}

func NewStorage(cfg *Config) *Storage {
	return &Storage{
		cfg: cfg,
	}
}

// Initialize init obkv storage
func (s *Storage) Initialize() error {
	cli, err := client.NewClient(
		s.cfg.cliCfg.configUrl,
		s.cfg.cliCfg.fullUserName,
		s.cfg.cliCfg.password,
		s.cfg.cliCfg.sysUserName,
		s.cfg.cliCfg.sysPassword,
		s.cfg.cliCfg.cfg)
	if err != nil {
		return err
	}

	s.cli = cli
	return nil
}

// Close obkv storage
func (s *Storage) Close() error {
	s.cli.Close()
	return nil
}
