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

package hash

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oceanbase/modis/test"
)

func TestHash_HSet(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisHashTableName)

	rVal, rErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	rVal, rErr = rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	rVal, rErr = rCli.HSet(context.TODO(), "myhash", map[string]interface{}{"key1": "value1", "key2": "value2"}).Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HSet(context.TODO(), "myhash", map[string]interface{}{"key1": "value1", "key2": "value2"}).Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)
}

func TestHash_HSetNX(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisHashTableName)

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

func TestHash_HMSet(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisHashTableName)

	rVal, rErr := rCli.HMSet(context.TODO(), "myhash", "key1", "value1").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr := mCli.HMSet(context.TODO(), "myhash", "key1", "value1").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	rVal, rErr = rCli.HMSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HMSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	// Not Supported yet
	// rVal, rErr = rCli.HMSet(context.TODO(), "myhash", map[string]interface{}{"key1": "value1", "key2": "value2"}).Result()
	// assert.Equal(t, nil, rErr)
	// mVal, mErr = mCli.HMSet(context.TODO(), "myhash", map[string]interface{}{"key1": "value1", "key2": "value2"}).Result()
	// assert.Equal(t, nil, mErr)
	// assert.Equal(t, rVal, mVal)
}

func TestHash_HGet(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisHashTableName)

	rVal, rErr := rCli.HGet(context.TODO(), "myhash", "key1").Result()
	mVal, mErr := mCli.HGet(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, rErr, mErr)
	assert.Equal(t, rVal, mVal)

	// insert key-value pair
	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, rSErr)
	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, mSErr)
	assert.Equal(t, rSVal, mSVal)

	rVal, rErr = rCli.HGet(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HGet(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)
}

func TestHash_HMGet(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisHashTableName)

	rVal, rErr := rCli.HMGet(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr := mCli.HMGet(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	// insert key-value pair
	rSVal, rSErr := rCli.HMSet(context.TODO(), "myhash", []string{"key1", "value1", "key2", "value2"}).Result()
	assert.Equal(t, nil, rSErr)
	mSVal, mSErr := mCli.HMSet(context.TODO(), "myhash", []string{"key1", "value1", "key2", "value2"}).Result()
	assert.Equal(t, nil, mSErr)
	assert.Equal(t, rSVal, mSVal)

	rVal, rErr = rCli.HMGet(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HMGet(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)
}

func TestHash_HDel(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisHashTableName)

	rVal, rErr := rCli.HGet(context.TODO(), "myhash", "key1").Result()
	mVal, mErr := mCli.HGet(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, rErr, mErr)
	assert.Equal(t, rVal, mVal)

	// insert key-value pair
	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, rSErr)
	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, mSErr)
	assert.Equal(t, rSVal, mSVal)

	rVal, rErr = rCli.HGet(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HGet(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	// delete key-value pair
	rDVal, rDErr := rCli.HDel(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, rDErr)
	mDVal, mDErr := mCli.HDel(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, mDErr)
	assert.Equal(t, rDVal, mDVal)

	rVal, rErr = rCli.HGet(context.TODO(), "myhash", "key1").Result()
	mVal, mErr = mCli.HGet(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, rErr, mErr)
	assert.Equal(t, rVal, mVal)
}

func TestHash_HLen(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisHashTableName)

	rVal, rErr := rCli.HLen(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr := mCli.HLen(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	// insert key-value pair
	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, rSErr)
	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, mSErr)
	assert.Equal(t, rSVal, mSVal)

	rVal, rErr = rCli.HLen(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HLen(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)
}

func TestHash_HExists(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisHashTableName)

	rVal, rErr := rCli.HExists(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr := mCli.HExists(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	// insert key-value pair
	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, rSErr)
	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, mSErr)
	assert.Equal(t, rSVal, mSVal)

	rVal, rErr = rCli.HExists(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HExists(context.TODO(), "myhash", "key1").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)
}

func TestHash_HGetAll(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisHashTableName)

	rVal, rErr := rCli.HGetAll(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr := mCli.HGetAll(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	// insert key-value pair
	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, rSErr)
	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, mSErr)
	assert.Equal(t, rSVal, mSVal)

	rVal, rErr = rCli.HGetAll(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HGetAll(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)
}

func TestHash_HIncrBy(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisHashTableName)

	rVal, rErr := rCli.HIncrBy(context.TODO(), "myhash", "key1", 5).Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr := mCli.HIncrBy(context.TODO(), "myhash", "key1", 5).Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	// insert key-value pair
	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key2", 10).Result()
	assert.Equal(t, nil, rSErr)
	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key2", 10).Result()
	assert.Equal(t, nil, mSErr)
	assert.Equal(t, rSVal, mSVal)

	rVal, rErr = rCli.HIncrBy(context.TODO(), "myhash", "key2", 7).Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HIncrBy(context.TODO(), "myhash", "key2", 7).Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	rGVal, rGErr := rCli.HGet(context.TODO(), "myhash", "key2").Result()
	mGVal, mGErr := mCli.HGet(context.TODO(), "myhash", "key2").Result()
	assert.Equal(t, rGErr, mGErr)
	assert.Equal(t, rGVal, mGVal)
}

func TestHash_HIncrByFloat(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisHashTableName)

	rVal, rErr := rCli.HIncrByFloat(context.TODO(), "myhash", "key1", 1.5).Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr := mCli.HIncrByFloat(context.TODO(), "myhash", "key1", 1.5).Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	// insert key-value pair
	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key2", 10).Result()
	assert.Equal(t, nil, rSErr)
	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key2", 10).Result()
	assert.Equal(t, nil, mSErr)
	assert.Equal(t, rSVal, mSVal)

	rVal, rErr = rCli.HIncrByFloat(context.TODO(), "myhash", "key2", 1.7).Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HIncrByFloat(context.TODO(), "myhash", "key2", 1.7).Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	rGVal, rGErr := rCli.HGet(context.TODO(), "myhash", "key2").Result()
	mGVal, mGErr := mCli.HGet(context.TODO(), "myhash", "key2").Result()
	assert.Equal(t, rGErr, mGErr)
	assert.Equal(t, rGVal, mGVal)
}

func TestHash_HKeys(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisHashTableName)

	rVal, rErr := rCli.HKeys(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr := mCli.HKeys(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	// insert key-value pair
	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, rSErr)
	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, mSErr)
	assert.Equal(t, rSVal, mSVal)

	rVal, rErr = rCli.HKeys(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HKeys(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)
}

func TestHash_HVals(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisHashTableName)

	rVal, rErr := rCli.HVals(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr := mCli.HVals(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)

	// insert key-value pair
	rSVal, rSErr := rCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, rSErr)
	mSVal, mSErr := mCli.HSet(context.TODO(), "myhash", "key1", "value1", "key2", "value2").Result()
	assert.Equal(t, nil, mSErr)
	assert.Equal(t, rSVal, mSVal)

	rVal, rErr = rCli.HVals(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, rErr)
	mVal, mErr = mCli.HVals(context.TODO(), "myhash").Result()
	assert.Equal(t, nil, mErr)
	assert.Equal(t, rVal, mVal)
}
