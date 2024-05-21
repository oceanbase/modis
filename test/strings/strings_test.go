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

package strings

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"

	"github.com/oceanbase/modis/test"
)

const (
	stringTableName   = "modis_string_table"
	createStringTable = "create table if not exists modis_string_table(db bigint not null,rkey varbinary(1024) not null,value varbinary(1024) not null, expire_ts timestamp(6) default null,primary key(db, rkey)) TTL(expire_ts + INTERVAL 0 SECOND) partition by key(db, rkey) partitions 3;"
)

func TestSetAndGetOneItem(t *testing.T) {
	key := "x"
	value := "foobar"
	// clean data
	defer test.ClearDb(0, redisCli, stringTableName)

	// set
	// redis
	err := redisCli.Set(context.TODO(), key, value, 0).Err()
	assert.Equal(t, nil, err)
	// modis
	err = modisCli.Set(context.TODO(), key, value, 0).Err()
	assert.Equal(t, nil, err)

	// get
	// redis
	expectVal, err := redisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	// modis
	actualVal, err := modisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectVal, actualVal)
	fmt.Println("redis: " + expectVal + "\nmodis: " + actualVal)
}

func TestSetAndGetEmptyItem(t *testing.T) {
	// clean data
	defer test.ClearDb(0, redisCli, stringTableName)

	// get key_not_exist
	key := "key_not_exist"
	// redis
	expectVal, err := redisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, "redis: nil", err.Error())
	// modis
	actualVal, err := modisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, "redis: nil", err.Error())
	assert.EqualValues(t, expectVal, actualVal)

	// set
	key = "x"
	value := ""
	// redis
	err = redisCli.Set(context.TODO(), key, value, 0).Err()
	assert.Equal(t, nil, err)
	// modis
	err = modisCli.Set(context.TODO(), key, value, 0).Err()
	assert.Equal(t, nil, err)

	// get
	// redis
	expectVal, err = redisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	// modis
	actualVal, err = modisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectVal, actualVal)
}

func TestBigPayloadAndRandomAccess(t *testing.T) {
	key_prefix := "key_%d"
	value_prefix := "value_"
	record_size := 100
	defer test.ClearDb(0, redisCli, stringTableName)

	// set
	for i := 0; i < record_size; i++ {
		key := fmt.Sprintf(key_prefix, i)
		var sb strings.Builder
		for j := 1; j < i+900; j++ {
			sb.WriteString("a")
		}
		value := value_prefix + sb.String()
		// redis
		err := redisCli.Set(context.TODO(), key, value, 0).Err()
		assert.Equal(t, nil, err)
		// modis
		err = modisCli.Set(context.TODO(), key, value, 0).Err()
		assert.Equal(t, nil, err)
	}
	// Get
	for i := 0; i < record_size; i++ {
		key := fmt.Sprintf(key_prefix, i)
		// redis
		expectVal, err := redisCli.Get(context.TODO(), key).Result()
		assert.Equal(t, nil, err)
		// modis
		actualVal, err := modisCli.Get(context.TODO(), key).Result()
		assert.Equal(t, nil, err)
		assert.EqualValues(t, expectVal, actualVal)
	}
}

func TestSetNx(t *testing.T) {
	key := "novar"
	value := "foobared"
	defer test.ClearDb(0, redisCli, stringTableName)

	// setnx with key missing
	res := redisCli.SetNX(context.TODO(), key, value, 0)
	assert.Equal(t, nil, res.Err())
	assert.Equal(t, true, res.Val())
	res = modisCli.SetNX(context.TODO(), key, value, 0)
	assert.Equal(t, nil, res.Err())
	assert.Equal(t, true, res.Val())
	// get
	expectVal, err := redisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	actualVal, err := modisCli.Get(context.TODO(), key).Result()
	assert.EqualValues(t, expectVal, actualVal)

	// setnx with key exist
	res = redisCli.SetNX(context.TODO(), key, value, 0)
	assert.Equal(t, nil, res.Err())
	assert.Equal(t, false, res.Val())
	res = modisCli.SetNX(context.TODO(), key, value, 0)
	assert.Equal(t, nil, res.Err())
	assert.Equal(t, false, res.Val())
	// get
	expectVal, err = redisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	actualVal, err = modisCli.Get(context.TODO(), key).Result()
	assert.EqualValues(t, expectVal, actualVal)

}

func TestStrlen(t *testing.T) {
	defer test.ClearDb(0, redisCli, stringTableName)
	// key not exist
	res := redisCli.StrLen(context.TODO(), "key_not_exist")
	assert.Equal(t, nil, res.Err())
	assert.EqualValues(t, 0, res.Val())
	res = modisCli.StrLen(context.TODO(), "key_not_exist")
	assert.Equal(t, nil, res.Err())
	assert.EqualValues(t, 0, res.Val())

	// integer-encoded value
	key := "myinteger"
	value := -555
	err := redisCli.Set(context.TODO(), key, value, 0).Err()
	assert.Equal(t, nil, err)
	expectRes := redisCli.StrLen(context.TODO(), key)
	assert.Equal(t, nil, expectRes.Err())
	assert.EqualValues(t, 4, expectRes.Val())

	err = modisCli.Set(context.TODO(), key, value, 0).Err()
	assert.Equal(t, nil, err)
	actualRes := modisCli.StrLen(context.TODO(), key)
	assert.Equal(t, nil, actualRes.Err())
	assert.EqualValues(t, expectRes.Val(), actualRes.Val())

	// set plain string
	key = "myinteger"
	value2 := "foozzz0123456789 baz"
	err = redisCli.Set(context.TODO(), key, value2, 0).Err()
	assert.Equal(t, nil, err)
	expectRes = redisCli.StrLen(context.TODO(), key)
	assert.Equal(t, nil, expectRes.Err())
	assert.EqualValues(t, 20, expectRes.Val())

	err = modisCli.Set(context.TODO(), key, value2, 0).Err()
	assert.Equal(t, nil, err)
	actualRes = modisCli.StrLen(context.TODO(), key)
	assert.Equal(t, nil, actualRes.Err())
	assert.EqualValues(t, expectRes.Val(), actualRes.Val())
}

func TestMSetAndMGet(t *testing.T) {
	defer test.ClearDb(0, redisCli, stringTableName)
	// base case
	m := map[string]interface{}{
		"x{t}": 10,
		"y{t}": "foo bar",
		"z{t}": "x x x x x x x\\n\\n\\r\\n",
	}
	// mset
	res := redisCli.MSet(context.TODO(), m)
	assert.Equal(t, nil, res.Err())
	res = modisCli.MSet(context.TODO(), m)
	assert.Equal(t, nil, res.Err())
	// mget
	expectRes := redisCli.MGet(context.TODO(), "x{t}", "y{t}", "z{t}")
	assert.Equal(t, nil, expectRes.Err())
	assert.EqualValues(t, strconv.Itoa(10), expectRes.Val()[0])
	assert.EqualValues(t, "foo bar", expectRes.Val()[1])
	assert.EqualValues(t, "x x x x x x x\\n\\n\\r\\n", expectRes.Val()[2])
	actualRes := modisCli.MGet(context.TODO(), "x{t}", "y{t}", "z{t}")
	assert.EqualValues(t, actualRes.Val()[0], expectRes.Val()[0])
	assert.EqualValues(t, actualRes.Val()[1], expectRes.Val()[1])
	assert.EqualValues(t, actualRes.Val()[2], expectRes.Val()[2])

	// wrong number of args
	res = modisCli.MSet(context.TODO(), "x{t}", 10, "y{t}", "foo bar", "z{t}")
	assert.NotEqual(t, nil, res)

	// mset with already existing key
	sameKey := "x{t}"
	value := "xxx"
	value1 := "yyy"
	res = redisCli.MSet(context.TODO(), sameKey, value, sameKey, value1)
	assert.Equal(t, nil, res.Err())
	res = modisCli.MSet(context.TODO(), sameKey, value, sameKey, value1)
	assert.Equal(t, nil, res.Err())
	// verify
	expectRes1, err := redisCli.Get(context.TODO(), sameKey).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, value1, expectRes1)
	actualRes1, err := modisCli.Get(context.TODO(), sameKey).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, actualRes1, expectRes1)

	// mget not existing key
	expectRes = redisCli.MGet(context.TODO(), "x{t2}", "y{t2}", "z{t2}")
	assert.Equal(t, nil, expectRes.Err())
	assert.Equal(t, 3, len(expectRes.Val()))
	actualRes = modisCli.MGet(context.TODO(), "x{t2}", "y{t2}", "z{t2}")
	assert.Equal(t, len(expectRes.Val()), len(actualRes.Val()))
}

func TestGetSet(t *testing.T) {
	defer test.ClearDb(0, redisCli, stringTableName)
	// set new value
	key := "foo"
	value := "bar"
	expectRes := redisCli.GetSet(context.TODO(), key, value)
	assert.NotEqual(t, nil, expectRes.Err())
	assert.Equal(t, "", expectRes.Val())
	actualRes := modisCli.GetSet(context.TODO(), key, value)
	assert.NotEqual(t, nil, actualRes.Err())
	assert.Equal(t, expectRes.Val(), actualRes.Val())
	// get
	res, err := redisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, value, res)
	res2, err := modisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, res, res2)

	// replace old value
	value1 := "xyz"
	expectRes = redisCli.GetSet(context.TODO(), key, value1)
	assert.Equal(t, nil, expectRes.Err())
	assert.Equal(t, value, expectRes.Val())
	actualRes = modisCli.GetSet(context.TODO(), key, value1)
	assert.Equal(t, nil, actualRes.Err())
	assert.Equal(t, expectRes.Val(), actualRes.Val())
	// get
	res, err = redisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, value1, res)
	res2, err = modisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, res, res2)
}

func TestSetEx(t *testing.T) {
	defer test.ClearDb(0, redisCli, stringTableName)
	keyFmt := "key_%d"
	valueFmt := "value_%d"
	recordCount := 10
	expireTime := 5 * time.Second
	for i := 0; i < recordCount; i++ {
		key := fmt.Sprintf(keyFmt, i)
		value := fmt.Sprintf(valueFmt, i)
		err := redisCli.SetEX(context.TODO(), key, value, expireTime).Err()
		assert.Equal(t, nil, err)
		err = modisCli.SetEX(context.TODO(), key, value, expireTime).Err()
		assert.Equal(t, nil, err)
	}

	// get key not expire
	for i := 0; i < recordCount; i++ {
		key := fmt.Sprintf(keyFmt, i)
		value := fmt.Sprintf(valueFmt, i)
		expectVal, err := redisCli.Get(context.TODO(), key).Result()
		assert.Equal(t, nil, err)
		assert.EqualValues(t, value, expectVal)
		actualVal, err := modisCli.Get(context.TODO(), key).Result()
		assert.Equal(t, nil, err)
		assert.EqualValues(t, expectVal, actualVal)
		// refresh exist key
		if i%2 == 0 {
			expireTime2 := 10 * time.Second
			value := fmt.Sprintf(valueFmt, i*2)
			err := redisCli.SetEX(context.TODO(), key, value, expireTime2).Err()
			assert.Equal(t, nil, err)
			err = modisCli.SetEX(context.TODO(), key, value, expireTime2).Err()
			assert.Equal(t, nil, err)
		}
	}

	time.Sleep(expireTime)
	// get key expired
	for i := 0; i < recordCount; i++ {
		key := fmt.Sprintf(keyFmt, i)
		expectVal, expectErr := redisCli.Get(context.TODO(), key).Result()
		actualVal, actual_err := modisCli.Get(context.TODO(), key).Result()
		if i%2 == 0 {
			value := fmt.Sprintf(valueFmt, i*2)
			assert.Equal(t, nil, expectErr)
			assert.Equal(t, nil, actual_err)
			assert.EqualValues(t, value, expectVal)
			assert.EqualValues(t, expectVal, actualVal)
		} else {
			assert.Equal(t, "redis: nil", expectErr.Error())
			assert.Equal(t, "redis: nil", actual_err.Error())
			assert.EqualValues(t, expectVal, actualVal)
		}
	}
}

func TestSetRange(t *testing.T) {
	defer test.ClearDb(0, redisCli, stringTableName)
	// setrange against non-existing key
	testSetRange(t, "mykey1", 0, "foo", "foo")
	testSetRange(t, "mykey2", 1, "foo", "\000foo")

	// setrange against non-existing key with empty value
	key := "mykey3"
	value := ""
	expectVal, err := redisCli.SetRange(context.TODO(), key, 0, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 0, expectVal)
	actualVal, err := modisCli.SetRange(context.TODO(), key, 0, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectVal, actualVal)
	// get
	_, err1 := redisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, "redis: nil", err1.Error())
	_, err2 := modisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, "redis: nil", err2.Error())

	// setrange against string-encoded key
	key = "mykey"
	value = "foo"
	// set key
	SetKey(t, key, value)
	testSetRange(t, key, 0, "b", "boo")
	SetKey(t, key, value)
	testSetRange(t, key, 0, "", "foo")
	SetKey(t, key, value)
	testSetRange(t, key, 1, "b", "fbo")
	SetKey(t, key, value)
	testSetRange(t, key, 4, "bar", "foo\000bar")

	// setrange against integer-encoded key
	key = "mykey"
	valueInt := 1234
	SetKey(t, key, valueInt)
	testSetRange(t, key, 0, "2", "2234")
	SetKey(t, key, valueInt)
	testSetRange(t, key, 0, "", "1234")
	SetKey(t, key, valueInt)
	testSetRange(t, key, 1, "3", "1334")
	SetKey(t, key, valueInt)
	testSetRange(t, key, 5, "2", "1234\0002")

}

func SetKey(t *testing.T, key string, value interface{}) {
	_, err := redisCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	_, err = modisCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)

}

func testSetRange(t *testing.T, key string, offset int64, value string, expectValue string) {
	expectVal, err := redisCli.SetRange(context.TODO(), key, offset, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, len(expectValue), expectVal)
	actualVal, err := modisCli.SetRange(context.TODO(), key, offset, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectVal, actualVal)
	// get
	expectRes, err := redisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectValue, expectRes)
	actualRes, err := modisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectRes, actualRes)
}

func TestGetRange(t *testing.T) {
	defer test.ClearDb(0, redisCli, stringTableName)
	// non-existing key
	key := "key_not_exist"
	testGetRange(t, key, 0, -1, "")

	// getrange against string value
	key = "mykey"
	value := "hello world"
	SetKey(t, key, value)
	testGetRange(t, key, 0, 3, "hell")
	testGetRange(t, key, 0, -1, value)
	testGetRange(t, key, -4, -1, "orld")
	testGetRange(t, key, 5, 3, "")
	testGetRange(t, key, 5, 5000, " world")
	testGetRange(t, key, -5000, 10000, value)

	//  getrange against integer-encoded value
	valueInt := 1234
	SetKey(t, key, valueInt)
	testGetRange(t, key, 0, 2, "123")
	testGetRange(t, key, 0, -1, "1234")
	testGetRange(t, key, -3, -1, "234")
	testGetRange(t, key, 5, 3, "")
	testGetRange(t, key, 3, 5000, "4")
	testGetRange(t, key, -5000, 10000, "1234")
}

func testGetRange(t *testing.T, key string, start int64, end int64, expectRes string) {
	expectVal, err := redisCli.GetRange(context.TODO(), key, start, end).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectRes, expectVal)
	actualVal, err := modisCli.GetRange(context.TODO(), key, start, end).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectRes, actualVal)
}

func TestSetBit(t *testing.T) {
	defer test.ClearDb(0, redisCli, stringTableName)
	// setbit non-existing key
	key := "mykey"
	testSetBit(t, key, 1, 1, 0)

	// setbit against string-encoded key
	SetKey(t, key, "@")
	testSetBit(t, key, 2, 1, 0)
	testSetBit(t, key, 1, 0, 1)

	// setbit against integer-encoded key
	SetKey(t, key, 1)
	testSetBit(t, key, 6, 1, 0)
	testSetBit(t, key, 2, 0, 1)

}

func testSetBit(t *testing.T, key string, offset int64, value int, expectRes int) {
	res, err := redisCli.SetBit(context.TODO(), key, offset, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectRes, res)
	res, err = modisCli.SetBit(context.TODO(), key, offset, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectRes, res)

	redisRes, err := redisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	modisRes, err := modisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, redisRes, modisRes)
}

func TestGetBit(t *testing.T) {
	defer test.ClearDb(0, redisCli, stringTableName)
	key := "mykey"
	// getbit non-existing key
	testGetBit(t, key, 0, 0)

	// getbit string-encoded key
	// f: 01100110
	value := "f"
	SetKey(t, key, value)
	testGetBit(t, key, 0, 0)
	testGetBit(t, key, 1, 1)
	testGetBit(t, key, 2, 1)
	testGetBit(t, key, 3, 0)

	testGetBit(t, key, 8, 0)
	testGetBit(t, key, 100, 0)
	testGetBit(t, key, 1000, 0)

	// getbit integer-encoded key
	SetKey(t, key, 1)
	testGetBit(t, key, 0, 0)
	testGetBit(t, key, 1, 0)
	testGetBit(t, key, 2, 1)
	testGetBit(t, key, 3, 1)

	testGetBit(t, key, 8, 0)
	testGetBit(t, key, 100, 0)
	testGetBit(t, key, 1000, 0)
}

func TestBitCount(t *testing.T) {
	defer test.ClearDb(0, redisCli, stringTableName)
	key := "key_not_exist"
	testBitCount(t, key, nil, 0)

	key = "mykey"
	// f : 01100110 ; bitcount: 4
	// o : 01101111 ; bitcount: 6
	// o : 01101111 ; bitcount: 6
	// b : 01100010 ; bitcount: 3
	// a : 01100001 ; bitcount: 3
	// r : 01110010 ; bitcount: 4
	value := "foobar"
	SetKey(t, key, value)
	testBitCount(t, key, nil, 26)
	testBitCount(t, key, &redis.BitCount{Start: 0, End: 0}, 4)
	testBitCount(t, key, &redis.BitCount{Start: -2, End: -1}, 7)
	testBitCount(t, key, &redis.BitCount{Start: -10, End: -9}, 4)
	testBitCount(t, key, &redis.BitCount{Start: 10, End: 11}, 0)
	testBitCount(t, key, &redis.BitCount{Start: 3, End: 2}, 0)

}

func testBitCount(t *testing.T, key string, bitCount *redis.BitCount, res_count int64) {
	redisCount, err := redisCli.BitCount(context.TODO(), key, bitCount).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, res_count, redisCount)
	modisCount, err := modisCli.BitCount(context.TODO(), key, bitCount).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, res_count, modisCount)
}

func testGetBit(t *testing.T, key string, offset int64, expectVal int64) {
	redisRes, err := redisCli.GetBit(context.TODO(), key, offset).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectVal, redisRes)
	modisRes, err := modisCli.GetBit(context.TODO(), key, offset).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectVal, modisRes)
}

func TestAppend(t *testing.T) {
	defer test.ClearDb(0, redisCli, stringTableName)
	key := "foo"
	value := "bar"
	// key not exist
	testAppend(t, key, value, value)
	// key exist
	newValue := "100"
	expectRes := fmt.Sprintf("%s%s", value, newValue)
	testAppend(t, key, newValue, expectRes)
}

func testAppend(t *testing.T, key string, value string, expectRes string) {
	// append
	redisRes, err := redisCli.Append(context.TODO(), key, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, len(expectRes), redisRes)
	modisRes, err := modisCli.Append(context.TODO(), key, value).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, redisRes, modisRes)
	// get
	redisGetRes, err := redisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectRes, redisGetRes)
	modisGetRes, err := modisCli.Get(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, expectRes, modisGetRes)
}

// func TestIncr(t *testing.T) {
// 	defer test.ClearDb(0, redisCli, stringTableName)
// 	key := "foo"

// 	// incr first
// 	redisRes, err := redisCli.Incr(context.TODO(), key).Result()
// 	assert.Equal(t, nil, err)
// 	modisRes, err := modisCli.Incr(context.TODO(), key).Result()
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, redisRes, modisRes)

// 	// incr again
// 	redisRes, err = redisCli.Incr(context.TODO(), key).Result()
// 	assert.Equal(t, nil, err)
// 	modisRes, err = modisCli.Incr(context.TODO(), key).Result()
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, redisRes, modisRes)
// }

// func TestIncrBy(t *testing.T) {
// 	defer test.ClearDb(0, redisCli, stringTableName)
// 	key := "foo"
// 	var value int64 = 3010101010102

// 	// incr first
// 	redisRes, err := redisCli.IncrBy(context.TODO(), key, value).Result()
// 	assert.Equal(t, nil, err)
// 	modisRes, err := modisCli.IncrBy(context.TODO(), key, value).Result()
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, redisRes, modisRes)

// 	// incr again
// 	redisRes, err = redisCli.IncrBy(context.TODO(), key, value).Result()
// 	assert.Equal(t, nil, err)
// 	modisRes, err = modisCli.IncrBy(context.TODO(), key, value).Result()
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, redisRes, modisRes)
// }

// func TestIncrByFloat(t *testing.T) {
// 	defer test.ClearDb(0, redisCli, stringTableName)
// 	key := "foo"
// 	value := 301.0101010102

// 	// incr first
// 	redisRes, err := redisCli.IncrByFloat(context.TODO(), key, value).Result()
// 	assert.Equal(t, nil, err)
// 	modisRes, err := modisCli.IncrByFloat(context.TODO(), key, value).Result()
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, redisRes, modisRes)

// 	// incr again
// 	redisRes, err = redisCli.IncrByFloat(context.TODO(), key, value).Result()
// 	assert.Equal(t, nil, err)
// 	modisRes, err = modisCli.IncrByFloat(context.TODO(), key, value).Result()
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, redisRes, modisRes)
// }

// func TestDecr(t *testing.T) {
// 	defer test.ClearDb(0, redisCli, stringTableName)
// 	key := "foo"

// 	// decr first
// 	redisRes, err := redisCli.Decr(context.TODO(), key).Result()
// 	assert.Equal(t, nil, err)
// 	modisRes, err := modisCli.Decr(context.TODO(), key).Result()
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, redisRes, modisRes)

// 	// decr again
// 	redisRes, err = redisCli.Decr(context.TODO(), key).Result()
// 	assert.Equal(t, nil, err)
// 	modisRes, err = modisCli.Decr(context.TODO(), key).Result()
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, redisRes, modisRes)
// }

// func TestDecrBy(t *testing.T) {
// 	defer test.ClearDb(0, redisCli, stringTableName)
// 	key := "foo"
// 	var value int64 = 3010101010102

// 	// decr first
// 	redisRes, err := redisCli.IncrBy(context.TODO(), key, value).Result()
// 	assert.Equal(t, nil, err)
// 	modisRes, err := modisCli.IncrBy(context.TODO(), key, value).Result()
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, redisRes, modisRes)

// 	// decr again
// 	redisRes, err = redisCli.IncrBy(context.TODO(), key, value).Result()
// 	assert.Equal(t, nil, err)
// 	modisRes, err = modisCli.IncrBy(context.TODO(), key, value).Result()
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, redisRes, modisRes)
// }
