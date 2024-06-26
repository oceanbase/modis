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

package list

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/oceanbase/modis/test"
	"github.com/stretchr/testify/assert"
)

<<<<<<< HEAD
=======
const (
	testModisListTableName       = "modis_list_table"
	testModisListCreateStatement = " CREATE TABLE modis_list_table(   db BIGINT NOT NULL,   rkey VARBINARY(1024) NOT NULL,   `index` BIGINT NOT NULL,                value VARBINARY(1024) DEFAULT NULL,   expire_ts TIMESTAMP(6) DEFAULT NULL,    PRIMARY KEY(db, rkey, `index`)        )  PARTITION BY KEY(db, rkey)             PARTITIONS 3;"
)

>>>>>>> add redis entity_type
func generateTestData(count int) []string {
	users := make([]string, count)

	for i := 0; i < count; i++ {
		users[i] = "user" + strconv.Itoa(i+1)
	}

	return users
}

func TestLPush(t *testing.T) {
	key := "listKey"
	defer test.ClearDb(0, rCli, test.TestModisListTableName)

	members := generateTestData(10)
	for _, member := range members {
		val, err := rCli.LPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.LPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	// duplicate key
	val, err := rCli.LPush(context.TODO(), key, members[0]).Result()
	assert.Equal(t, nil, err)
	val_m, err := mCli.LPush(context.TODO(), key, members[0]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)

	membersRedis, err := rCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	membersModis, err := mCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, membersRedis, membersModis)
}

func TestLPushX(t *testing.T) {
	key := "listKey"
	defer test.ClearDb(0, rCli, test.TestModisListTableName)
	members := generateTestData(10)

	// key not exist
	keyNotExist := "listKeyNotExist"
	for _, member := range members {
		val, err := rCli.LPushX(context.TODO(), keyNotExist, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.LPushX(context.TODO(), keyNotExist, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	// fixme: should return *-1\r\n
	membersRedis, err := rCli.LRange(context.TODO(), keyNotExist, 0, -1).Result()
	assert.Equal(t, nil, err)
	membersModis, err := mCli.LRange(context.TODO(), keyNotExist, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, membersRedis, membersModis)

	// key exist
	val, err := rCli.LPush(context.TODO(), key, members[0], members[1]).Result()
	assert.Equal(t, nil, err)
	val_m, err := mCli.LPush(context.TODO(), key, members[0], members[1]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)
	for _, member := range members {
		val, err := rCli.LPushX(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.LPushX(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	membersRedis, err = rCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	membersModis, err = mCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, membersRedis, membersModis)
}

func TestRPush(t *testing.T) {
	key := "listKey"
	defer test.ClearDb(0, rCli, test.TestModisListTableName)

	members := generateTestData(10)
	for _, member := range members {
		val, err := rCli.RPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.RPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	// duplicate key
	val, err := rCli.RPush(context.TODO(), key, members[0]).Result()
	assert.Equal(t, nil, err)
	val_m, err := mCli.RPush(context.TODO(), key, members[0]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)

	membersRedis, err := rCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	membersModis, err := mCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	// assert.EqualValues(t, 0, len(difference(membersRedis, membersModis)))
	assert.Equal(t, membersRedis, membersModis)
}

func TestRPushX(t *testing.T) {
	key := "listKey"
	defer test.ClearDb(0, rCli, test.TestModisListTableName)
	members := generateTestData(10)

	// fixme: should return *-1\r\n
	// key not exist
	keyNotExist := "listKeyNotExist"
	for _, member := range members {
		val, err := rCli.LPushX(context.TODO(), keyNotExist, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.LPushX(context.TODO(), keyNotExist, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	// fixme: should return *-1\r\n
	membersRedis, err := rCli.LRange(context.TODO(), keyNotExist, 0, -1).Result()
	assert.Equal(t, nil, err)
	membersModis, err := mCli.LRange(context.TODO(), keyNotExist, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, membersRedis, membersModis)

	// key exist
	val, err := rCli.RPush(context.TODO(), key, members[0], members[1]).Result()
	assert.Equal(t, nil, err)
	val_m, err := mCli.RPush(context.TODO(), key, members[0], members[1]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)
	for _, member := range members {
		val, err := rCli.RPushX(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.RPushX(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	membersRedis, err = rCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	membersModis, err = mCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, membersRedis, membersModis)
}

func TestLPop(t *testing.T) {
	key := "listKey"
	defer test.ClearDb(0, rCli, test.TestModisListTableName)

	members := generateTestData(10)
	for _, member := range members {
		val, err := rCli.RPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.RPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	val, err := rCli.LPop(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	val_m, err := mCli.LPop(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)

	membersRedis, err := rCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	membersModis, err := mCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	// assert.EqualValues(t, 0, len(difference(membersRedis, membersModis)))
	assert.Equal(t, membersRedis, membersModis)
}

func TestRPop(t *testing.T) {
	key := "listKey"
	defer test.ClearDb(0, rCli, test.TestModisListTableName)

	members := generateTestData(10)
	for _, member := range members {
		val, err := rCli.LPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.LPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	val, err := rCli.RPop(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	val_m, err := mCli.RPop(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)

	membersRedis, err := rCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	membersModis, err := mCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, membersRedis, membersModis)
}

func TestLIndex(t *testing.T) {
	key := "listKey"
	defer test.ClearDb(0, rCli, test.TestModisListTableName)

	members := generateTestData(10)
	for _, member := range members {
		val, err := rCli.LPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.LPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	val, err := rCli.LIndex(context.TODO(), key, 0).Result()
	assert.Equal(t, nil, err)
	val_m, err := mCli.LIndex(context.TODO(), key, 0).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)

	val, err = rCli.LIndex(context.TODO(), key, -1).Result()
	assert.Equal(t, nil, err)
	val_m, err = mCli.LIndex(context.TODO(), key, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)

	val, err = rCli.LIndex(context.TODO(), key, 3).Result()
	assert.Equal(t, nil, err)
	val_m, err = mCli.LIndex(context.TODO(), key, 3).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)

	// fixme: should return $-1\r\n
	val, err = rCli.LIndex(context.TODO(), key, 18).Result()
	assert.Equal(t, redis.Nil, err)
	val_m, err = mCli.LIndex(context.TODO(), key, 18).Result()
	assert.Equal(t, redis.Nil, err)
	assert.Equal(t, val, val_m)
}

func TestLSet(t *testing.T) {
	key := "listKey"
	defer test.ClearDb(0, rCli, test.TestModisListTableName)

	members := generateTestData(10)
	for _, member := range members {
		val, err := rCli.LPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.LPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	val, err := rCli.LSet(context.TODO(), key, 0, "setMember").Result()
	assert.Equal(t, nil, err)
	val_m, err := mCli.LSet(context.TODO(), key, 0, "setMember").Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)

	val, err = rCli.LSet(context.TODO(), key, -2, "setMember").Result()
	assert.Equal(t, nil, err)
	val_m, err = mCli.LSet(context.TODO(), key, -2, "setMember").Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)

	// // fixme: should return "ERR index out of range"
	// val, err = rCli.LSet(context.TODO(), key, 12, "setMember").Result()
	// assert.Contains(t, err.Error(), "ERR index out of range")
	// val_m, err_m := mCli.LSet(context.TODO(), key, 12, "setMember").Result()
	// assert.Equal(t, err, err_m)
	// assert.Equal(t, val, val_m)

	// membersRedis, err := rCli.LRange(context.TODO(), key, 0, -1).Result()
	// assert.Equal(t, nil, err)
	// membersModis, err := mCli.LRange(context.TODO(), key, 0, -1).Result()
	// assert.Equal(t, nil, err)
	// assert.Equal(t, membersRedis, membersModis)
}

func TestLTrim(t *testing.T) {
	key := "listKey"
	defer test.ClearDb(0, rCli, test.TestModisListTableName)

	members := generateTestData(10)
	for _, member := range members {
		val, err := rCli.LPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.LPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	val, err := rCli.LTrim(context.TODO(), key, 1, -1).Result()
	assert.Equal(t, nil, err)
	val_m, err := mCli.LTrim(context.TODO(), key, 1, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)

	membersRedis, err := rCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	membersModis, err := mCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, membersRedis, membersModis)
}

func TestLInsert(t *testing.T) {
	key := "listKey"
	defer test.ClearDb(0, rCli, test.TestModisListTableName)

	members := generateTestData(10)
	for _, member := range members {
		val, err := rCli.RPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.RPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	// fixme: should return integer
	val, err := rCli.LInsert(context.TODO(), key, "Before", "b1", "b2").Result()
	assert.Equal(t, nil, err)
	val_m, err := mCli.LInsert(context.TODO(), key, "Before", "b1", "b2").Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)

	val, err = rCli.LInsert(context.TODO(), key, "After", "a1", "a2").Result()
	assert.Equal(t, nil, err)
	val_m, err = mCli.LInsert(context.TODO(), key, "After", "a1", "a2").Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, val, val_m)

	membersRedis, err := rCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	membersModis, err := mCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, membersRedis, membersModis)
}

func TestLLen(t *testing.T) {
	key := "listKey"
	defer test.ClearDb(0, rCli, test.TestModisListTableName)

	members := generateTestData(10)
	for _, member := range members {
		val, err := rCli.RPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.RPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	membersRedis, err := rCli.LLen(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	membersModis, err := mCli.LLen(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, membersRedis, membersModis)
}

func TestLRem(t *testing.T) {
	key := "listKey"
	defer test.ClearDb(0, rCli, test.TestModisListTableName)

	members := generateTestData(10)
	for _, member := range members {
		val, err := rCli.RPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		val_m, err := mCli.RPush(context.TODO(), key, member).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, val, val_m)
	}

	lremR, err := rCli.LRem(context.TODO(), key, -2, members[0]).Result()
	assert.Equal(t, nil, err)
	lremM, err := mCli.LRem(context.TODO(), key, -2, members[0]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, lremR, lremM)

	membersRedis, err := rCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	membersModis, err := mCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, membersRedis, membersModis)

	// fixme: should succ
	lremR, err = rCli.LRem(context.TODO(), key, 2, members[4]).Result()
	assert.Equal(t, nil, err)
	lremM, err = mCli.LRem(context.TODO(), key, 2, members[4]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, lremR, lremM)

	membersRedis, err = rCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	membersModis, err = mCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, membersRedis, membersModis)

	lremR, err = rCli.LRem(context.TODO(), key, -2, members[4]).Result()
	assert.Equal(t, nil, err)
	lremM, err = mCli.LRem(context.TODO(), key, -2, members[4]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, lremR, lremM)

	time.Sleep(time.Second)
	membersRedis, err = rCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	membersModis, err = mCli.LRange(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, membersRedis, membersModis)
}

// Not supported currently
// func TestRPopLPush(t *testing.T) {
// 	key1 := "listKey1"
// 	key2 := "listKey2"
// 	defer test.ClearDb(0, rCli, test.TestModisListTableName)

// 	members := generateTestData(10)
// 	for _, member := range members {
// 		val, err := rCli.RPush(context.TODO(), key1, member).Result()
// 		assert.Equal(t, nil, err)
// 		val_m, err := mCli.RPush(context.TODO(), key2, member).Result()
// 		assert.Equal(t, nil, err)
// 		assert.Equal(t, val, val_m)
// 	}

// 	rPopLPushR, err := rCli.RPopLPush(context.TODO(), key1, key2).Result()
// 	assert.Equal(t, nil, err)
// 	rPopLPushM, err := mCli.RPopLPush(context.TODO(), key1, key2).Result()
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, rPopLPushR, rPopLPushM)

// 	membersRedis, err := rCli.LRange(context.TODO(), key1, 0, -1).Result()
// 	assert.Equal(t, nil, err)
// 	membersModis, err := mCli.LRange(context.TODO(), key1, 0, -1).Result()
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, membersRedis, membersModis)

// 	membersRedis, err = rCli.LRange(context.TODO(), key2, 0, -1).Result()
// 	assert.Equal(t, nil, err)
// 	membersModis, err = mCli.LRange(context.TODO(), key2, 0, -1).Result()
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, membersRedis, membersModis)
// }
