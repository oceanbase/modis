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

package storage

import (
	"context"
	"time"

	"github.com/oceanbase/modis/config"
	"github.com/oceanbase/modis/storage/obkv"
)

type Storage interface {
	Initialize() error
	// key commands
	Delete(ctx context.Context, db int64, keys [][]byte) (int64, error)
	Type(ctx context.Context, db int64, key []byte) (string, error)
	Exists(ctx context.Context, db int64, keys [][]byte) (int64, error)
	Expire(ctx context.Context, db int64, key []byte, t time.Time) (int, error)
	Persist(ctx context.Context, db int64, key []byte) (int, error)
	TTL(ctx context.Context, db int64, key []byte) (time.Duration, error)

	// string commands
	Get(ctx context.Context, db int64, key []byte) ([]byte, error)
	Set(ctx context.Context, db int64, key []byte, value []byte) error
	PSetEx(ctx context.Context, db int64, key []byte, expireTime uint64, value []byte) error
	SetEx(ctx context.Context, db int64, key []byte, expireTime uint64, value []byte) error
	MGet(ctx context.Context, db int64, keys [][]byte) ([][]byte, error)
	MSet(ctx context.Context, db int64, kv map[string][]byte) (int, error)
	SetNx(ctx context.Context, db int64, key []byte, value []byte) (int, error)
	Append(ctx context.Context, db int64, key []byte, value []byte) (int, error)
	IncrBy(ctx context.Context, db int64, key []byte, value []byte) (int64, error)
	IncrByFloat(ctx context.Context, db int64, key []byte, value []byte) (float64, error)
	SetBit(ctx context.Context, db int64, key []byte, offset int, value int) (int, error)
	GetBit(ctx context.Context, db int64, key []byte, offset int) (byte, error)
	GetSet(ctx context.Context, db int64, key []byte, value []byte) ([]byte, error)

	// hash commands
	HSet(ctx context.Context, db int64, key []byte, fieldValue map[string][]byte) (int, error)
	HSetNx(ctx context.Context, db int64, key []byte, field []byte, value []byte) (int, error)
	HMGet(ctx context.Context, db int64, key []byte, fields [][]byte) ([][]byte, error)
	HGet(ctx context.Context, db int64, key []byte, field []byte) ([]byte, error)
	HDel(ctx context.Context, db int64, key []byte, fields [][]byte) (int64, error)
	HGetAll(ctx context.Context, db int64, key []byte) ([][]byte, error)
	HKeys(ctx context.Context, db int64, key []byte) ([][]byte, error)
	HVals(ctx context.Context, db int64, key []byte) ([][]byte, error)
	HLen(ctx context.Context, db int64, key []byte) (int64, error)
	HIncrBy(ctx context.Context, db int64, key []byte, field []byte, value []byte) (int64, error)
	HIncrByFloat(ctx context.Context, db int64, key []byte, field []byte, value []byte) (float64, error)

	// set commands
	SAdd(ctx context.Context, db int64, key []byte, members [][]byte) (int64, error)
	SCard(ctx context.Context, db int64, key []byte) (int64, error)
	SIsmember(ctx context.Context, db int64, key []byte, member []byte) (int, error)
	SMembers(ctx context.Context, db int64, key []byte) ([][]byte, error)
	Smove(ctx context.Context, db int64, src []byte, dst []byte, member []byte) (int, error)
	SPop(ctx context.Context, db int64, key []byte, count int64) ([][]byte, error)
	SRandMember(ctx context.Context, db int64, key []byte, count int64) ([][]byte, error)
	SRem(ctx context.Context, db int64, key []byte, members [][]byte) (int64, error)

	// zset commands
	ZAdd(ctx context.Context, db int64, key []byte, memberScore map[string]int64) (int, error)
	ZRange(ctx context.Context, db int64, key []byte, start int64, end int64, withScore bool) ([][]byte, error)
	ZCard(ctx context.Context, db int64, key []byte) (int, error)
	ZRem(ctx context.Context, db int64, key []byte, members [][]byte) (int, error)

	Close() error
}

func NewStorage(cfg Config) Storage {
	return obkv.NewStorage(cfg.(*obkv.Config))
}

// Open a storage instance
func Open(config *config.ObkvStorageConfig) (Storage, error) {
	cfg := NewConfig(config)
	storage := NewStorage(cfg)
	if err := storage.Initialize(); err != nil {
		return nil, err
	}

	return storage, nil
}
