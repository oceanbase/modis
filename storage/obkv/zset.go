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
