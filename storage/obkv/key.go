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
	"errors"
	"strings"
	"time"

	"github.com/oceanbase/obkv-table-client-go/table"
)

// Type get the type of the key
// check order: string hash list zset set
func (s *Storage) Type(ctx context.Context, db int64, key []byte) ([]byte, error) {
	var keys [][]byte
	var types []byte
	keys = append(keys, key)
	is_first := true

	num, err := s.stringExists(ctx, db, keys)
	if err != nil {
		return nil, err
	}
	if num != 0 {
		types = append(types, []byte("string")...)
		is_first = false
	}

	num, err = s.hashExists(ctx, db, keys)
	if err != nil {
		return nil, err
	}
	if num != 0 {
		if !is_first {
			types = append(types, []byte(", ")...)
		}
		types = append(types, []byte("hash")...)
		is_first = false
	}

	num, err = s.listExists(ctx, db, keys)
	if err != nil {
		return nil, err
	}
	if num != 0 {
		if !is_first {
			types = append(types, []byte(", ")...)
		}
		types = append(types, []byte("list")...)
		is_first = false
	}

	num, err = s.zsetExists(ctx, db, keys)
	if err != nil {
		return nil, err
	}
	if num != 0 {
		if !is_first {
			types = append(types, []byte(", ")...)
		}
		types = append(types, []byte("zset")...)
		is_first = false
	}

	num, err = s.setExists(ctx, db, keys)
	if err != nil {
		return nil, err
	}
	if num != 0 {
		if !is_first {
			types = append(types, []byte(", ")...)
		}
		types = append(types, []byte("set")...)
	}

	return types, nil
}

// Exists check the number of keys that exist
func (s *Storage) Exists(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	var existsNum int64
	num, err := s.stringExists(ctx, db, keys)
	if err != nil {
		return 0, err
	}
	existsNum += num

	num, err = s.hashExists(ctx, db, keys)
	if err != nil {
		return 0, err
	}
	existsNum += num

	num, err = s.listExists(ctx, db, keys)
	if err != nil {
		return 0, err
	}
	existsNum += num

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

	return existsNum, nil
}

// Delete delete all keys
func (s *Storage) Delete(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	var deleteNum int64
	// TODO:
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
	var expireNum int
	var err_msg string
	var err error

	val, err := s.Type(ctx, db, key)
	if err != nil {
		return 0, err
	} else if val != nil {
		if strings.Contains(string(val), "string") {
			// expire string
			_, err = s.expireString(ctx, "get", db, key, table.TimeStamp(at))
			if err != nil {
				return 0, err
			}
			expireNum += 1
			err_msg += "expire string success, "
		}
		if len(val) != 6 || string(val) != "string" {
			return expireNum, errors.New(err_msg + "expire types other than string are not supported")
		}
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

	return expireNum, nil
}

// Persist removes the existing timeout on key, turning the key from volatile to persistent
func (s *Storage) Persist(ctx context.Context, db int64, key []byte) (int, error) {
	res := 0
	var err error

	val, err := s.Type(ctx, db, key)
	if err != nil {
		return 0, err
	} else if val != nil && (len(val) != 6 || string(val) != "string") {
		return 0, errors.New("expire types other than string are not supported")
	}

	// persist string
	res, err = s.persistString(ctx, "get", db, key)
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
