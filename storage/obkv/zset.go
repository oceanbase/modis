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

	"github.com/oceanbase/obkv-table-client-go/table"
)

const (
	zsetTableName = "modis_zset_table"
)

// ZAdd add member to zset
func (s *Storage) ZAdd(ctx context.Context, db int64, key []byte, memberScore map[string]int64) (int, error) {
	return 0, nil
}

// ZRange get data with the range
func (s *Storage) ZRange(ctx context.Context, db int64, key []byte, start int64, end int64, withScore bool) ([][]byte, error) {
	return nil, nil
}

// ZCard get the size of the key
func (s *Storage) ZCard(ctx context.Context, db int64, key []byte) (int, error) {
	return 0, nil
}

// ZRem remove the member from the key
func (s *Storage) ZRem(ctx context.Context, db int64, key []byte, members [][]byte) (int, error) {
	return 0, nil
}

// zsetExists check the number of keys that exist in zset table
func (s *Storage) zsetExists(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	// todo:impl
	return 0, nil
}

// deleteZSet delete zset table
func (s *Storage) deleteZSet(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	// todo:impl
	return 0, nil
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
