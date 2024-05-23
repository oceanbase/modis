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
	"math"
	"time"

	"github.com/oceanbase/modis/protocol/resp"
	"github.com/oceanbase/obkv-table-client-go/table"
)

const (
	listTableName = "modis_list_table"
)

// listExists check the number of keys that exist in list table
func (s *Storage) listExists(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	var exist_key_count int64 = 0

	for i := 0; i < len(keys); i++ {
		len_cmd := [][]byte{[]byte("llen")}
		len_cmd = append(len_cmd, keys[i])
		len_cmd_str := resp.EncArray(len_cmd)

		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, keys[i]),
			table.NewColumn(indexColumnName, int64(math.MinInt64)),
		}
		list_len, err := s.ObServerCmd(ctx, listTableName, rowKey, []byte(len_cmd_str))
		if err != nil {
			return exist_key_count, err
		}

		len, err := resp.DecInteger(list_len)
		if err != nil {
			return exist_key_count, err
		}
		if len > 0 {
			exist_key_count++
		}

	}

	return exist_key_count, nil
}

// deleteList delete list table
func (s *Storage) deleteList(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	var delete_key_count int64 = 0

	for i := 0; i < len(keys); i++ {
		trim_cmd := [][]byte{[]byte("ledl")}
		trim_cmd = append(trim_cmd, keys[i])
		trim_cmd_str := resp.EncArray(trim_cmd)

		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, keys[i]),
			table.NewColumn(indexColumnName, int64(math.MinInt64)),
		}
		res, err := s.ObServerCmd(ctx, listTableName, rowKey, []byte(trim_cmd_str))
		if err != nil {
			return delete_key_count, err
		}
		if res == resp.ResponsesOk {
			delete_key_count++
		}
	}

	return delete_key_count, nil
}

// expireList expire list table
func (s *Storage) expireList(ctx context.Context, db int64, key []byte, expire_ts table.TimeStamp) (int, error) {
	return 0, nil
}

// persistList persist list table
func (s *Storage) persistList(ctx context.Context, db int64, key []byte) (int, error) {
	return 0, nil
}

// ttlList get expire time of list table
func (s *Storage) ttlList(ctx context.Context, db int64, key []byte) (time.Duration, error) {
	return -2, nil
}
