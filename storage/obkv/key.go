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

	"github.com/oceanbase/obkv-table-client-go/table"
)

// Type get the type of the key
// check order: string hash list zset set
func (s *Storage) Type(ctx context.Context, db int64, key []byte) (string, error) {
	var keys [][]byte
	keys = append(keys, key)

	num, err := s.stringExists(ctx, db, keys)
	if err != nil {
		return "", err
	}
	if num != 0 {
		return "string", nil
	}

	num, err = s.hashExists(ctx, db, keys)
	if err != nil {
		return "", err
	}
	if num != 0 {
		return "hash", nil
	}

	num, err = s.listExists(ctx, db, keys)
	if err != nil {
		return "", err
	}
	if num != 0 {
		return "list", nil
	}

	num, err = s.zsetExists(ctx, db, keys)
	if err != nil {
		return "", err
	}
	if num != 0 {
		return "zset", nil
	}

	num, err = s.setExists(ctx, db, keys)
	if err != nil {
		return "", err
	}
	if num != 0 {
		return "set", nil
	}

	return "none", nil
}

// Exists check the number of keys that exist
func (s *Storage) Exists(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	var existsNum int64
	num, err := s.stringExists(ctx, db, keys)
	if err != nil {
		return 0, err
	}
	existsNum += num
	fmt.Println(num)

	num, err = s.hashExists(ctx, db, keys)
	if err != nil {
		return 0, err
	}
	existsNum += num
	fmt.Println(num)

	num, err = s.listExists(ctx, db, keys)
	if err != nil {
		return 0, err
	}
	existsNum += num
	fmt.Println(num)

	num, err = s.zsetExists(ctx, db, keys)
	if err != nil {
		return 0, err
	}
	existsNum += num

	num, err = s.setExists(ctx, db, keys)
	if err != nil {
		return 0, err
	}
	existsNum += num
	fmt.Println(num)

	return existsNum, nil
}

// Delete delete all keys
func (s *Storage) Delete(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	var deleteNum int64
	num, err := s.deleteString(ctx, db, keys)
	if err != nil {
		return 0, err
	}
	deleteNum += num

	num, err = s.deleteHash(ctx, db, keys)
	if err != nil {
		return 0, err
	}
	deleteNum += num

	num, err = s.deleteList(ctx, db, keys)
	if err != nil {
		return 0, err
	}
	deleteNum += num

	num, err = s.deleteZSet(ctx, db, keys)
	if err != nil {
		return 0, err
	}
	deleteNum += num

	num, err = s.deleteSet(ctx, db, keys)
	if err != nil {
		return 0, err
	}
	deleteNum += num

	return deleteNum, nil
}

// Expire sets a timeout on key
func (s *Storage) Expire(ctx context.Context, db int64, key []byte, at time.Time) (int, error) {
	res := 0
	var err error

	// expire string
	res, err = s.expireString(ctx, db, key, table.TimeStamp(at))
	if err != nil {
		return 0, err
	}
	if res != 0 {
		return res, nil
	}

	// // expire hash
	// res, err = s.expireHash(ctx, db, key, table.TimeStamp(at))
	// if err != nil {
	// 	return 0, err
	// }
	// if res != 0 {
	// 	return res, nil
	// }

	// // expire list
	// res, err = s.expireList(ctx, db, key, table.TimeStamp(at))
	// if err != nil {
	// 	return 0, err
	// }
	// if res != 0 {
	// 	return res, nil
	// }

	// // expire zset
	// res, err = s.expireZSet(ctx, db, key, table.TimeStamp(at))
	// if err != nil {
	// 	return 0, err
	// }
	// if res != 0 {
	// 	return res, nil
	// }

	// // expire set
	// res, err = s.expireSet(ctx, db, key, table.TimeStamp(at))
	// if err != nil {
	// 	return 0, err
	// }
	// if res != 0 {
	// 	return res, nil
	// }

	return 0, nil
}

// Persist removes the existing timeout on key, turning the key from volatile to persistent
func (s *Storage) Persist(ctx context.Context, db int64, key []byte) (int, error) {
	res := 0
	var err error

	// persist string
	res, err = s.persistString(ctx, db, key)
	if err != nil {
		return 0, err
	}
	if res != 0 {
		return res, nil
	}

	// // persist hash
	// res, err = s.persistHash(ctx, db, key)
	// if err != nil {
	// 	return 0, err
	// }
	// if res != 0 {
	// 	return res, nil
	// }

	// // persist list
	// res, err = s.persistList(ctx, db, key)
	// if err != nil {
	// 	return 0, err
	// }
	// if res != 0 {
	// 	return res, nil
	// }

	// // persist zset
	// res, err = s.persistZSet(ctx, db, key)
	// if err != nil {
	// 	return 0, err
	// }
	// if res != 0 {
	// 	return res, nil
	// }

	// // persist set
	// res, err = s.persistSet(ctx, db, key)
	// if err != nil {
	// 	return 0, err
	// }
	// if res != 0 {
	// 	return res, nil
	// }

	return 0, nil
}

func (s *Storage) TTL(ctx context.Context, db int64, key []byte) (time.Duration, error) {
	// ttl string
	sub, err := s.ttlString(ctx, db, key)
	if err != nil {
		return 0, err
	}
	if sub >= -1 {
		return sub, nil
	}

	// // ttl hash
	// sub, err = s.ttlHash(ctx, db, key)
	// if err != nil {
	// 	return 0, err
	// }
	// if sub >= -1 {
	// 	return sub, nil
	// }

	// // ttl list
	// sub, err = s.ttlList(ctx, db, key)
	// if err != nil {
	// 	return 0, err
	// }
	// if sub >= -1 {
	// 	return sub, nil
	// }

	// // ttl zset
	// sub, err = s.ttlZSet(ctx, db, key)
	// if err != nil {
	// 	return 0, err
	// }
	// if sub >= -1 {
	// 	return sub, nil
	// }

	// // ttl set
	// sub, err = s.ttlSet(ctx, db, key)
	// if err != nil {
	// 	return 0, err
	// }
	// if sub >= -1 {
	// 	return sub, nil
	// }

	return -2, nil
}
