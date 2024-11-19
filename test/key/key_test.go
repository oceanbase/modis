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

package key

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"

	"github.com/oceanbase/modis/test"
)

func TestKey_Del(t *testing.T) {
	key := "Key"
	value := "Value"
	field := "Field"
	member := "Member"
	score := 1.234
	element := "Element"
	defer test.ClearDb(0, rCli, test.TestModisStringTableName, test.TestModisHashTableName, test.TestModisSetTableName, test.TestModisZSetTableName, test.TestModisListTableName)

	// empty
	delRedis, err := rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err := mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// string
	setRedis, err := rCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	setModis, err := mCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, setRedis, setModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// hash
	hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, hsetRedis, hsetModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// set
	saddRedis, err := rCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	saddModis, err := mCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, saddRedis, saddModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// zset
	zaddRedis, err := rCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	zaddModis, err := mCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, zaddRedis, zaddModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// list
	lpushRedis, err := rCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	lpushModis, err := mCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, lpushRedis, lpushModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)
}

func TestKey_Exists(t *testing.T) {
	key := "Key"
	value := "Value"
	field := "Field"
	member := "Member"
	score := 1.234
	element := "Element"
	defer test.ClearDb(0, rCli, test.TestModisStringTableName, test.TestModisHashTableName, test.TestModisSetTableName, test.TestModisZSetTableName, test.TestModisListTableName)
	existsNum := 0

	// empty
	existsModis, err := mCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existsNum, existsModis)

	// string
	setRedis, err := rCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	setModis, err := mCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, setRedis, setModis)
	existsNum++
	existsModis, err = mCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existsNum, existsModis)

	// hash
	hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 1, hsetModis)
	existsNum++
	existsModis, err = mCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existsNum, existsModis)

	// set
	saddModis, err := mCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 1, saddModis)
	existsNum++
	existsModis, err = mCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existsNum, existsModis)

	// zset
	zaddModis, err := mCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 1, zaddModis)
	existsNum++
	existsModis, err = mCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existsNum, existsModis)

	// list
	lpushModis, err := mCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 1, lpushModis)
	existsNum++
	existsModis, err = mCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existsNum, existsModis)
}

func TestKey_Type(t *testing.T) {
	key := "Key"
	value := "Value"
	field := "Field"
	member := "Member"
	score := 1.234
	element := "Element"
	defer test.ClearDb(0, rCli, test.TestModisStringTableName, test.TestModisHashTableName, test.TestModisSetTableName, test.TestModisZSetTableName, test.TestModisListTableName)

	// empty
	typeRedis, err := rCli.Type(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	typeModis, err := mCli.Type(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, typeRedis, typeModis)

	// string
	res := "string"
	setModis, err := mCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, "OK", setModis)
	typeModis, err = mCli.Type(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, res, typeModis)

	// hash
	res += ", hash"
	hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 1, hsetModis)
	typeModis, err = mCli.Type(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, res, typeModis)

	// list
	res += ", list"
	lpushModis, err := mCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 1, lpushModis)
	typeModis, err = mCli.Type(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, res, typeModis)

	// zset
	res += ", zset"
	zaddModis, err := mCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 1, zaddModis)
	typeModis, err = mCli.Type(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, res, typeModis)

	// set
	res += ", set"
	saddModis, err := mCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 1, saddModis)
	typeModis, err = mCli.Type(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, res, typeModis)
}

func TestKey_Expire(t *testing.T) {
	key := "Key"
	value := "Value"
	field := "Field"
	member := "Member"
	score := 1.234
	element := "Element"
	expiration := time.Second * 1
	defer test.ClearDb(0, rCli, test.TestModisStringTableName, test.TestModisHashTableName, test.TestModisSetTableName, test.TestModisZSetTableName, test.TestModisListTableName)

	// empty
	expireRedis, err := rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err := mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)

	// string
	setRedis, err := rCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	setModis, err := mCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, setRedis, setModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	existRedis, err := rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err := rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)

	// hash
	hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, hsetRedis, hsetModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	hexistRedis, err := rCli.Exists(context.TODO(), key, field).Result()
	assert.Equal(t, nil, err)
	hexistModis, err := rCli.Exists(context.TODO(), key, field).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, hexistRedis, hexistModis)

	// set
	saddRedis, err := rCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	saddModis, err := mCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, saddRedis, saddModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	ismemRedis, err := rCli.Exists(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	ismemModis, err := rCli.Exists(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, ismemRedis, ismemModis)

	// zset
	zaddRedis, err := rCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	zaddModis, err := mCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, zaddRedis, zaddModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	existRedis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)

	// list
	lpushRedis, err := rCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	lpushModis, err := mCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, lpushRedis, lpushModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	existRedis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)
}

func TestKey_ExpireAt(t *testing.T) {
	key := "Key"
	value := "Value"
	field := "Field"
	member := "Member"
	score := 1.234
	element := "Element"
	expiration := 1 * time.Second
	tm := time.Now().Add(expiration)
	defer test.ClearDb(0, rCli, test.TestModisStringTableName, test.TestModisHashTableName, test.TestModisSetTableName, test.TestModisZSetTableName, test.TestModisListTableName)

	// empty
	expireRedis, err := rCli.ExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	expireModis, err := mCli.ExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)

	// string
	setRedis, err := rCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	setModis, err := mCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, setRedis, setModis)
	expireRedis, err = rCli.ExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.ExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	existRedis, err := rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err := rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)

	// hash
	hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, hsetRedis, hsetModis)
	expireRedis, err = rCli.ExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.ExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	hexistRedis, err := rCli.HExists(context.TODO(), key, field).Result()
	assert.Equal(t, nil, err)
	hexistModis, err := rCli.HExists(context.TODO(), key, field).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, hexistRedis, hexistModis)

	// set
	saddRedis, err := rCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	saddModis, err := mCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, saddRedis, saddModis)
	expireRedis, err = rCli.ExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.ExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	ismemRedis, err := rCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	ismemModis, err := rCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, ismemRedis, ismemModis)

	// zset
	zaddRedis, err := rCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	zaddModis, err := mCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, zaddRedis, zaddModis)
	expireRedis, err = rCli.ExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.ExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	existRedis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)

	// list
	lpushRedis, err := rCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	lpushModis, err := mCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, lpushRedis, lpushModis)
	expireRedis, err = rCli.ExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.ExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	existRedis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)
}

func TestKey_PExpire(t *testing.T) {
	key := "Key"
	value := "Value"
	field := "Field"
	member := "Member"
	score := 1.234
	element := "Element"
	expiration := 1000 * time.Microsecond
	defer test.ClearDb(0, rCli, test.TestModisStringTableName, test.TestModisHashTableName, test.TestModisSetTableName, test.TestModisZSetTableName, test.TestModisListTableName)

	// empty
	expireRedis, err := rCli.PExpire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err := mCli.PExpire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)

	// string
	setRedis, err := rCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	setModis, err := mCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, setRedis, setModis)
	expireRedis, err = rCli.PExpire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.PExpire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	existRedis, err := rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err := rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)

	// hash
	hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, hsetRedis, hsetModis)
	expireRedis, err = rCli.PExpire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.PExpire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	hexistRedis, err := rCli.HExists(context.TODO(), key, field).Result()
	assert.Equal(t, nil, err)
	hexistModis, err := rCli.HExists(context.TODO(), key, field).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, hexistRedis, hexistModis)

	// set
	saddRedis, err := rCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	saddModis, err := mCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, saddRedis, saddModis)
	expireRedis, err = rCli.PExpire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.PExpire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	ismemRedis, err := rCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	ismemModis, err := rCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, ismemRedis, ismemModis)

	// zset
	zaddRedis, err := rCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	zaddModis, err := mCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, zaddRedis, zaddModis)
	expireRedis, err = rCli.PExpire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.PExpire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	existRedis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)

	// list
	lpushRedis, err := rCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	lpushModis, err := mCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, lpushRedis, lpushModis)
	expireRedis, err = rCli.PExpire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.PExpire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	existRedis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)
}

func TestKey_PExpireAt(t *testing.T) {
	key := "Key"
	value := "Value"
	field := "Field"
	member := "Member"
	score := 1.234
	element := "Element"
	expiration := 1000 * time.Microsecond
	tm := time.Now().Add(expiration)
	defer test.ClearDb(0, rCli, test.TestModisStringTableName, test.TestModisHashTableName, test.TestModisSetTableName, test.TestModisZSetTableName, test.TestModisListTableName)

	// empty
	expireRedis, err := rCli.PExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	expireModis, err := mCli.PExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)

	// string
	setRedis, err := rCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	setModis, err := mCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, setRedis, setModis)
	expireRedis, err = rCli.PExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.PExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	existRedis, err := rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err := rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)

	// hash
	hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, hsetRedis, hsetModis)
	expireRedis, err = rCli.PExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.PExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	hexistRedis, err := rCli.HExists(context.TODO(), key, field).Result()
	assert.Equal(t, nil, err)
	hexistModis, err := rCli.HExists(context.TODO(), key, field).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, hexistRedis, hexistModis)

	// set
	saddRedis, err := rCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	saddModis, err := mCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, saddRedis, saddModis)
	expireRedis, err = rCli.PExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.PExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	ismemRedis, err := rCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	ismemModis, err := rCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, ismemRedis, ismemModis)

	// zset
	zaddRedis, err := rCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	zaddModis, err := mCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, zaddRedis, zaddModis)
	expireRedis, err = rCli.PExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.PExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	existRedis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)

	// list
	lpushRedis, err := rCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	lpushModis, err := mCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, lpushRedis, lpushModis)
	expireRedis, err = rCli.PExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.PExpireAt(context.TODO(), key, tm).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	time.Sleep(expiration)
	existRedis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)
}

func TestKey_Persist(t *testing.T) {
	key := "Key"
	value := "Value"
	field := "Field"
	member := "Member"
	score := 1.234
	element := "Element"
	expiration := time.Second * 1
	defer test.ClearDb(0, rCli, test.TestModisStringTableName, test.TestModisHashTableName, test.TestModisSetTableName, test.TestModisZSetTableName, test.TestModisListTableName)

	// empty
	persistRedis, err := rCli.Persist(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	persistModis, err := mCli.Persist(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, persistRedis, persistModis)

	// string
	setRedis, err := rCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	setModis, err := mCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, setRedis, setModis)
	expireRedis, err := rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err := mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	persistRedis, err = rCli.Persist(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	persistModis, err = mCli.Persist(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, persistRedis, persistModis)
	time.Sleep(expiration)
	existRedis, err := rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err := rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)
	delRedis, err := rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err := mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// hash
	hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, hsetRedis, hsetModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	persistRedis, err = rCli.Persist(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	persistModis, err = mCli.Persist(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, persistRedis, persistModis)
	time.Sleep(expiration)
	hexistRedis, err := rCli.HExists(context.TODO(), key, field).Result()
	assert.Equal(t, nil, err)
	hexistModis, err := rCli.HExists(context.TODO(), key, field).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, hexistRedis, hexistModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// set
	saddRedis, err := rCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	saddModis, err := mCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, saddRedis, saddModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	persistRedis, err = rCli.Persist(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	persistModis, err = mCli.Persist(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, persistRedis, persistModis)
	time.Sleep(expiration)
	ismemRedis, err := rCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	ismemModis, err := rCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, ismemRedis, ismemModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// zset
	zaddRedis, err := rCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	zaddModis, err := mCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, zaddRedis, zaddModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	persistRedis, err = rCli.Persist(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	persistModis, err = mCli.Persist(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, persistRedis, persistModis)
	time.Sleep(expiration)
	existRedis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// list
	lpushRedis, err := rCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	lpushModis, err := mCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, lpushRedis, lpushModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	persistRedis, err = rCli.Persist(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	persistModis, err = mCli.Persist(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, persistRedis, persistModis)
	time.Sleep(expiration)
	existRedis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existModis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existRedis, existModis)
}

func TestKey_TTL(t *testing.T) {
	key := "Key"
	value := "Value"
	field := "Field"
	member := "Member"
	score := 1.234
	element := "Element"
	expiration := time.Second * 10
	defer test.ClearDb(0, rCli, test.TestModisStringTableName, test.TestModisHashTableName, test.TestModisSetTableName, test.TestModisZSetTableName, test.TestModisListTableName)

	// empty
	ttlRedis, err := rCli.TTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	ttlModis, err := mCli.TTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, ttlRedis, ttlModis)

	// string
	setRedis, err := rCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	setModis, err := mCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, setRedis, setModis)
	expireRedis, err := rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err := mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	ttlRedis, err = rCli.TTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	ttlModis, err = mCli.TTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 0, int64(ttlRedis.Seconds()-ttlModis.Seconds()))
	delRedis, err := rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err := mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// hash
	hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, hsetRedis, hsetModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	ttlRedis, err = rCli.TTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	ttlModis, err = mCli.TTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, ttlRedis, ttlModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// set
	saddRedis, err := rCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	saddModis, err := mCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, saddRedis, saddModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	ttlRedis, err = rCli.TTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	ttlModis, err = mCli.TTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, ttlRedis, ttlModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// zset
	zaddRedis, err := rCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	zaddModis, err := mCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, zaddRedis, zaddModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	ttlRedis, err = rCli.TTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	ttlModis, err = mCli.TTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, ttlRedis, ttlModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// list
	lpushRedis, err := rCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	lpushModis, err := mCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, lpushRedis, lpushModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	ttlRedis, err = rCli.TTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	ttlModis, err = mCli.TTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, ttlRedis, ttlModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)
}

func TestKey_PTTL(t *testing.T) {
	key := "Key"
	value := "Value"
	field := "Field"
	member := "Member"
	score := 1.234
	element := "Element"
	expiration := time.Second * 10
	defer test.ClearDb(0, rCli, test.TestModisStringTableName, test.TestModisHashTableName, test.TestModisSetTableName, test.TestModisZSetTableName, test.TestModisListTableName)

	// empty
	ttlRedis, err := rCli.PTTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	ttlModis, err := mCli.PTTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, ttlRedis, ttlModis)

	// string
	setRedis, err := rCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	setModis, err := mCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, setRedis, setModis)
	expireRedis, err := rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err := mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	ttlRedis, err = rCli.PTTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	ttlModis, err = mCli.PTTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 0, int64(ttlRedis.Seconds()-ttlModis.Seconds()))
	delRedis, err := rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err := mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// hash
	hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, hsetRedis, hsetModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	ttlRedis, err = rCli.PTTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	ttlModis, err = mCli.PTTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 0, int64(ttlRedis.Seconds()-ttlModis.Seconds()))
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// set
	saddRedis, err := rCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	saddModis, err := mCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, saddRedis, saddModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	ttlRedis, err = rCli.PTTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	ttlModis, err = mCli.PTTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 0, int64(ttlRedis.Seconds()-ttlModis.Seconds()))
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// zset
	zaddRedis, err := rCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	zaddModis, err := mCli.ZAdd(context.TODO(), key, &redis.Z{Score: score, Member: member}).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, zaddRedis, zaddModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	ttlRedis, err = rCli.PTTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	ttlModis, err = mCli.PTTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 0, int64(ttlRedis.Seconds()-ttlModis.Seconds()))
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// list
	lpushRedis, err := rCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	lpushModis, err := mCli.LPush(context.TODO(), key, element).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, lpushRedis, lpushModis)
	expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expireRedis, expireModis)
	ttlRedis, err = rCli.PTTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	ttlModis, err = mCli.PTTL(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 0, int64(ttlRedis.Seconds()-ttlModis.Seconds()))
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)
}
