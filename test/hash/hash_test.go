/*-
 * #%L
 * Modis
 * %%
 * Copyright (C) 2021 OceanBase
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

package hash

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oceanbase/modis/test"
)

const (
	testModisHashTableName       = "modis_hash_table"
	testModisHashCreateStatement = "create table if not exists modis_hash_table(db bigint not null, rkey varbinary(1024) not null, field varbinary(1024) not null, value varbinary(1024) not null, expire_ts timestamp(6) default null, primary key(db, rkey, field)) TTL(expire_ts + INTERVAL 0 SECOND) partition by key(db, rkey) partitions 3;"
)

// func TestHash_HSet(t *testing.T) {
// 	defer test.ClearDb(rCli, testModisHashTableName)

// 	rVal, rErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	rVal, rErr = rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr = mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	// Not Supported Yet
// 	// rVal, rErr = rCli.HSet(context.TODO(), "myhash", map[string]interface{}{"key1": "value1", "key2": "value2"}).Result()
// 	// assert.Equal(t, nil, rErr)
// 	// mVal, mErr = mCli.HSet(context.TODO(), "myhash", map[string]interface{}{"key1": "value1", "key2": "value2"}).Result()
// 	// assert.Equal(t, nil, mErr)
// 	// assert.Equal(t, rVal, mVal)
// }

func TestHash_HSetNX(t *testing.T) {
	defer test.ClearDb(rCli, testModisHashTableName)

	rVal, rErr := rCli.HSetNX(context.TODO(), "myhash", "key1", "value1").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr := mCli.HSetNX(context.TODO(), "myhash", "key1", "value1").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	rVal, rErr = rCli.HSetNX(context.TODO(), "myhash", "key1", 0).Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HSetNX(context.TODO(), "myhash", "key1", 1).Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)
}

// func TestHash_HMSet(t *testing.T) {
// 	defer test.ClearDb(rCli, testModisHashTableName)

// 	rVal, rErr := rCli.HMSet(context.TODO(), "myhash", "key1", "value1").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr := mCli.HMSet(context.TODO(), "myhash", "key1", "value1").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	rVal, rErr = rCli.HMSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr = mCli.HMSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	// Not Supported yet
// 	// rVal, rErr = rCli.HMSet(context.TODO(), "myhash", map[string]interface{}{"key1": "value1", "key2": "value2"}).Result()
// 	// assert.Equal(t, nil, rErr)
// 	// mVal, mErr = mCli.HMSet(context.TODO(), "myhash", map[string]interface{}{"key1": "value1", "key2": "value2"}).Result()
// 	// assert.Equal(t, nil, mErr)
// 	// assert.Equal(t, rVal, mVal)
// }

// func TestHash_HGet(t *testing.T) {
// 	defer test.ClearDb(rCli, testModisHashTableName)

// 	rVal, rErr := rCli.HGet(context.TODO(), "myhash", "key1").Result()
// 	mVal, mErr := mCli.HGet(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, rErr, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	// insert key-value pair
// 	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, rSErr)
// 	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, mSErr)
// 	assert.Equal(t, rSVal, mSVal)

// 	rVal, rErr = rCli.HGet(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr = mCli.HGet(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)
// }

// func TestHash_HMGet(t *testing.T) {
// 	defer test.ClearDb(rCli, testModisHashTableName)

// 	rVal, rErr := rCli.HMGet(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr := mCli.HMGet(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	// insert key-value pair
// 	rSVal, rSErr := rCli.HMSet(context.TODO(), "myhash", []string{"key1", "value1", "key2", "value2"}).Result()
// 	assert.Equal(t, nil, rSErr)
// 	mSVal, mSErr := mCli.HMSet(context.TODO(), "myhash", []string{"key1", "value1", "key2", "value2"}).Result()
// 	assert.Equal(t, nil, mSErr)
// 	assert.Equal(t, rSVal, mSVal)

// 	rVal, rErr = rCli.HMGet(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr = mCli.HMGet(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)
// }

// func TestHash_HDel(t *testing.T) {
// 	defer test.ClearDb(rCli, testModisHashTableName)

// 	rVal, rErr := rCli.HGet(context.TODO(), "myhash", "key1").Result()
// 	mVal, mErr := mCli.HGet(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, rErr, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	// insert key-value pair
// 	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, rSErr)
// 	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, mSErr)
// 	assert.Equal(t, rSVal, mSVal)

// 	rVal, rErr = rCli.HGet(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr = mCli.HGet(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	// delete key-value pair
// 	rDVal, rDErr := rCli.HDel(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, rDErr)
// 	mDVal, mDErr := mCli.HDel(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, mDErr)
// 	assert.Equal(t, rDVal, mDVal)

// 	rVal, rErr = rCli.HGet(context.TODO(), "myhash", "key1").Result()
// 	mVal, mErr = mCli.HGet(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, rErr, mErr)
// 	assert.Equal(t, rVal, mVal)
// }

// func TestHash_HLen(t *testing.T) {
// 	defer test.ClearDb(rCli, testModisHashTableName)

// 	rVal, rErr := rCli.HLen(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr := mCli.HLen(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	// insert key-value pair
// 	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, rSErr)
// 	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, mSErr)
// 	assert.Equal(t, rSVal, mSVal)

// 	rVal, rErr = rCli.HLen(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr = mCli.HLen(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)
// }

// func TestHash_HExists(t *testing.T) {
// 	defer test.ClearDb(rCli, testModisHashTableName)

// 	rVal, rErr := rCli.HExists(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr := mCli.HExists(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	// insert key-value pair
// 	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, rSErr)
// 	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, mSErr)
// 	assert.Equal(t, rSVal, mSVal)

// 	rVal, rErr = rCli.HExists(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr = mCli.HExists(context.TODO(), "myhash", "key1").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)
// }

// func TestHash_HGetAll(t *testing.T) {
// 	defer test.ClearDb(rCli, testModisHashTableName)

// 	rVal, rErr := rCli.HGetAll(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr := mCli.HGetAll(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	// insert key-value pair
// 	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, rSErr)
// 	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, mSErr)
// 	assert.Equal(t, rSVal, mSVal)

// 	rVal, rErr = rCli.HGetAll(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr = mCli.HGetAll(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)
// }

// func TestHash_HIncrBy(t *testing.T) {
// 	defer test.ClearDb(rCli, testModisHashTableName)

// 	rVal, rErr := rCli.HIncrBy(context.TODO(), "myhash", "key1", 5).Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr := mCli.HIncrBy(context.TODO(), "myhash", "key1", 5).Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	// insert key-value pair
// 	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key2", 10).Result()
// 	assert.Equal(t, nil, rSErr)
// 	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key2", 10).Result()
// 	assert.Equal(t, nil, mSErr)
// 	assert.Equal(t, rSVal, mSVal)

// 	rVal, rErr = rCli.HIncrBy(context.TODO(), "myhash", "key2", 7).Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr = mCli.HIncrBy(context.TODO(), "myhash", "key2", 7).Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	rGVal, rGErr := rCli.HGet(context.TODO(), "myhash", "key2").Result()
// 	mGVal, mGErr := mCli.HGet(context.TODO(), "myhash", "key2").Result()
// 	assert.Equal(t, rGErr, mGErr)
// 	assert.Equal(t, rGVal, mGVal)
// }

// func TestHash_HIncrByFloat(t *testing.T) {
// 	defer test.ClearDb(rCli, testModisHashTableName)

// 	rVal, rErr := rCli.HIncrByFloat(context.TODO(), "myhash", "key1", 1.5).Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr := mCli.HIncrByFloat(context.TODO(), "myhash", "key1", 1.5).Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	// insert key-value pair
// 	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key2", 10).Result()
// 	assert.Equal(t, nil, rSErr)
// 	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key2", 10).Result()
// 	assert.Equal(t, nil, mSErr)
// 	assert.Equal(t, rSVal, mSVal)

// 	rVal, rErr = rCli.HIncrByFloat(context.TODO(), "myhash", "key2", 1.7).Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr = mCli.HIncrByFloat(context.TODO(), "myhash", "key2", 1.7).Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	rGVal, rGErr := rCli.HGet(context.TODO(), "myhash", "key2").Result()
// 	mGVal, mGErr := mCli.HGet(context.TODO(), "myhash", "key2").Result()
// 	assert.Equal(t, rGErr, mGErr)
// 	assert.Equal(t, rGVal, mGVal)
// }

// func TestHash_HKeys(t *testing.T) {
// 	defer test.ClearDb(rCli, testModisHashTableName)

// 	rVal, rErr := rCli.HKeys(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr := mCli.HKeys(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	// insert key-value pair
// 	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, rSErr)
// 	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, mSErr)
// 	assert.Equal(t, rSVal, mSVal)

// 	rVal, rErr = rCli.HKeys(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr = mCli.HKeys(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)
// }

// func TestHash_HVals(t *testing.T) {
// 	defer test.ClearDb(rCli, testModisHashTableName)

// 	rVal, rErr := rCli.HVals(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr := mCli.HVals(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)

// 	// insert key-value pair
// 	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, rSErr)
// 	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
// 	assert.Equal(t, nil, mSErr)
// 	assert.Equal(t, rSVal, mSVal)

// 	rVal, rErr = rCli.HVals(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, rErr)
// 	mVal, mErr = mCli.HVals(context.TODO(), "myhash").Result()
// 	assert.Equal(t, nil, mErr)
// 	assert.Equal(t, rVal, mVal)
// }
