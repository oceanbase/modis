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
	"context"
	"fmt"
	"net/url"

	"github.com/oceanbase/obkv-table-client-go/client"
	"github.com/oceanbase/obkv-table-client-go/client/option"
	"github.com/oceanbase/obkv-table-client-go/protocol"
	"github.com/oceanbase/obkv-table-client-go/table"
)

const (
	dbColumnName     = "db"
	keyColumnName    = "rkey"
	valueColumnName  = "value"
	expireColumnName = "expire_ts"
	indexColumnName  = "index"
	isDataColumnName = "is_data"

	jdbc_database  = "oceanbase"
	table_sys_name = "__all_obkv_redis_command_to_tablename"
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

func (s *Storage) getJDBCUrl() (client.Client, error) {
	u, err := url.Parse(s.cfg.cliCfg.configUrl)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("database", jdbc_database)
	u.RawQuery = q.Encode()

	fmt.Println(u)
	cli, err := client.NewClient(
		u.String(),
		s.cfg.cliCfg.fullUserName,
		s.cfg.cliCfg.password,
		s.cfg.cliCfg.sysUserName,
		s.cfg.cliCfg.sysPassword,
		s.cfg.cliCfg.cfg)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func (s *Storage) getTableNames() error {
	jdbcHandler, err := s.getJDBCUrl()
	if err != nil {
		return err
	}
	startRowKey := []*table.Column{table.NewColumn("command_name", table.Min), table.NewColumn("table_name", table.Min)}
	endRowKey := []*table.Column{table.NewColumn("command_name", table.Max), table.NewColumn("table_name", table.Max)}
	keyRanges := []*table.RangePair{table.NewRangePair(startRowKey, endRowKey)}
	resultIter, err := jdbcHandler.Query(
		context.TODO(),
		table_sys_name,
		keyRanges,
		option.WithQuerySelectColumns([]string{"command_name", "table_name"}),
		option.WithQueryOffset(0),
	)
	if err != nil {
		return err
	}
	iter, err := resultIter.Next()
	for ; iter != nil && err == nil; iter, err = resultIter.Next() {
		commandName := iter.Value("command_name").(string)
		tableName := iter.Value("table_name").(string)
		if _, ok := s.tables[commandName]; !ok {
			s.tables[commandName] = tableName
		}
	}
	fmt.Println(s.tables)
	if err != nil {
		return err
	}
	return nil
}

// Close obkv storage
func (s *Storage) Close() error {
	s.cli.Close()
	return nil
}
