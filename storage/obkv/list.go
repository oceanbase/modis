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
