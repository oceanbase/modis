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

// listExists check the number of keys that exist in list table
func (s *Storage) listExists(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	// todo:impl
	return 0, nil
}

// deleteList delete list table
func (s *Storage) deleteList(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	// todo:impl
	return 0, nil
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
