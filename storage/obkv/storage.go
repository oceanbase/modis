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
	"time"

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
)

// for prefetch route
const (
	routeTimeout = time.Second * 20
	mockDB       = int64(0)
)

var (
	tbNames = []string{
		"modis_string_table",
		"modis_set_table",
		"modis_list_table",
		"modis_hash_table",
		"modis_zset_table",
	}
	getRouteCommand = []byte("*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n")
	mockKey         = []byte("k")
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

func (s *Storage) tryPrefetchRoute() error {
	mutateColumns := []*table.Column{
		table.NewColumn("REDIS_CODE_STR", getRouteCommand),
	}
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, mockDB),
		table.NewColumn(keyColumnName, mockKey),
	}

	for _, tableName := range tbNames {
		ctx, _ := context.WithTimeout(context.Background(), routeTimeout)
		_, err := s.cli.Redis(
			ctx,
			tableName,
			rowKey,
			mutateColumns,
			option.WithReturnAffectedEntity(true),
		)
		if err != nil {
			fmt.Println(err.Error())
			fmt.Println(tableName)
			return err
		}
	}

	return nil
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
	err = s.tryPrefetchRoute()
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
