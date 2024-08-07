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
	"strconv"
	"strings"

	"github.com/oceanbase/modis/util"
	"github.com/oceanbase/obkv-table-client-go/client"
	"github.com/oceanbase/obkv-table-client-go/protocol"
	"github.com/pkg/errors"
)

const (
	dbColumnName     = "db"
	keyColumnName    = "rkey"
	valueColumnName  = "value"
	expireColumnName = "expire_ts"
	indexColumnName  = "index"
	isDataColumnName = "is_data"

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

func (s *Storage) getServerAddr() (*util.ObServerAddr, error) {
	var resp util.ObHttpRslistResponse
	err := util.GetConfigServerResponseOrNull(s.cfg.cliCfg.configUrl,
		s.cfg.cliCfg.cfg.RsListHttpGetTimeout,
		s.cfg.cliCfg.cfg.RsListHttpGetRetryTimes,
		s.cfg.cliCfg.cfg.RsListHttpGetRetryInterval,
		&resp)
	if err != nil {
		return nil, errors.WithMessagef(err, "get remote ocp response, url:%s", s.cfg.cliCfg.configUrl)
	}
	rslist := util.NewRslist()

	for _, server := range resp.Data.RsList {
		// split ip and port, server.Address(xx.xx.xx.xx:xx)
		res := strings.Split(server.Address, ":")
		if len(res) != 2 {
			return nil, errors.Errorf("fail to split ip and port, server:%s", server.Address)
		}
		ip := res[0]
		if ip == "172.16.46.180" {
			ip = "115.29.212.38"
			println(ip)
		}
		svrPort, err := strconv.Atoi(res[1])
		if err != nil {
			return nil, errors.Errorf("fail to convert server port to int, port:%s", res[1])
		}
		serverAddr := util.NewObServerAddr(ip, server.SqlPort, svrPort)
		rslist.Append(serverAddr)
	}

	if rslist.Size() == 0 {
		return nil, errors.Errorf("failed to load Rslist, url:%s", s.cfg.cliCfg.configUrl)
	}
	return rslist.Get(), nil
}

func (s *Storage) getTableNames() error {
	serverAddr, err := s.getServerAddr()
	if err != nil {
		return errors.New("fail to get server addr")
	}

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
