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
	"database/sql"
	"fmt"

	"github.com/oceanbase/modis/util"
	"github.com/oceanbase/obkv-table-client-go/client"
	"github.com/oceanbase/obkv-table-client-go/protocol"
	"github.com/pkg/errors"
)

const (
	driver         = "mysql"
	dsnFormat      = "root@%s:%s@tcp(%s:%d)/oceanbase"
	table_sys_name = "DBA_OB_KV_REDIS_TABLE"
)

type Storage struct {
	cli    client.Client
	cfg    *Config
	tables map[string]string
}

func NewStorage(cfg *Config) *Storage {
	return &Storage{
		cfg:    cfg,
		tables: make(map[string]string),
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
	cli.SetEntityType(protocol.ObTableEntityTypeRedis)
	s.cli = cli
	return s.getTableNames()
}

func (s *Storage) getTableNameByCmdName(cmd string) (string, error) {
	val, ok := s.tables[cmd]
	if !ok {
		return "", fmt.Errorf("%s not support", cmd)
	}
	return val, nil
}

func (s *Storage) getTableNames() error {
	serverAddr := s.cli.GetRouteInfo().GetTenantServer()

	tenantName := util.GetTenantName(s.cfg.cliCfg.fullUserName)
	if len(tenantName) == 0 {
		return errors.Errorf("fullUserName not invalid %s", s.cfg.cliCfg.fullUserName)
	}

	dsn := fmt.Sprintf(dsnFormat, tenantName, s.cfg.cliCfg.password, serverAddr.Ip(), serverAddr.SqlPort())
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err
	}

	rows, err := db.Query("select command_name, table_name from DBA_OB_KV_REDIS_TABLE")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var commandName string
		var tableName string
		err := rows.Scan(&commandName, &tableName)
		if err != nil {
			return err
		}
		s.tables[commandName] = tableName
	}
	if err = rows.Err(); err != nil {
		return err
	}
	return nil
}

// Close obkv storage
func (s *Storage) Close() error {
	s.cli.Close()
	return nil
}
