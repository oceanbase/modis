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
	"time"

	"github.com/oceanbase/modis/protocol/resp"
	"github.com/oceanbase/obkv-table-client-go/table"
)

// zsetExists check the number of keys that exist in zset table
func (s *Storage) zsetExists(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	var existNum int64 = 0
	plainArray := make([][]byte, 2)
	plainArray[0] = []byte("zcard")

	for _, key := range keys {
		plainArray[1] = key
		encodedArray := resp.EncArray(plainArray)
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
		}

		outContent, err := s.ObServerCmd(ctx, "zremrange", rowKey, []byte(encodedArray))
		if err != nil {
			return 0, err
		}

		curDelNum, err := resp.DecInteger(outContent)
		if err != nil {
			return 0, err
		}
		if curDelNum > 0 {
			existNum += 1
		}
	}

	return existNum, nil
}

// deleteZSet delete zset table
func (s *Storage) deleteZSet(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	var deleteNum int64 = 0
	plainArray := make([][]byte, 4)
	plainArray[0] = []byte("zremrangebyrank")
	plainArray[2] = []byte("0")
	plainArray[3] = []byte("-1")

	for _, key := range keys {
		plainArray[1] = key
		encodedArray := resp.EncArray(plainArray)
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
		}

		outContent, err := s.ObServerCmd(ctx, "zremrange", rowKey, []byte(encodedArray))
		if err != nil {
			return 0, err
		}

		curDelNum, err := resp.DecInteger(outContent)
		if err != nil {
			return 0, err
		}
		if curDelNum > 0 {
			deleteNum++
		}
	}

	return deleteNum, nil
}

// expireZSet expire zset table
func (s *Storage) expireZSet(ctx context.Context, db int64, key []byte, expire_ts table.TimeStamp) (int, error) {
	return 0, nil
}

// persistZSet expire zset table
func (s *Storage) persistZSet(ctx context.Context, db int64, key []byte) (int, error) {
	return 0, nil
}

// ttlZSet get expire time of zset table
func (s *Storage) ttlZSet(ctx context.Context, db int64, key []byte) (time.Duration, error) {
	return -2, nil
}
