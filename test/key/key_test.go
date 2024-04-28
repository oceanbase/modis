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

package key

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oceanbase/modis/test"
)

const (
	testModisStringTableName       = "modis_string_table"
	testModisStringCreateStatement = "create table if not exists modis_string_table(db bigint not null,rkey varbinary(1024) not null,value varbinary(1024) not null,expire_ts timestamp(6) default null,primary key(db, rkey)) TTL(expire_ts + INTERVAL 0 SECOND)partition by key(db, rkey) partitions 3;"

	testModisHashTableName       = "modis_hash_table"
	testModisHashCreateStatement = "create table if not exists modis_hash_table(db bigint not null, rkey varbinary(1024) not null, field varbinary(1024) not null, value varbinary(1024) not null, expire_ts timestamp(6) default null, primary key(db, rkey, field)) TTL(expire_ts + INTERVAL 0 SECOND) partition by key(db, rkey) partitions 3;"

	testModisSetTableName       = "modis_set_table"
	testModisSetCreateStatement = "create table if not exists modis_set_table(db bigint not null,rkey varbinary(1024) not null,member varbinary(1024) not null,expire_ts timestamp(6) default null,primary key(db, rkey, member)) TTL(expire_ts + INTERVAL 0 SECOND)partition by key(db, rkey) partitions 3;"
)

func deleteTable() {
	test.DeleteTable(testModisStringTableName)
	test.DeleteTable(testModisHashTableName)
	test.DeleteTable(testModisSetTableName)
}

func TestKey_Del(t *testing.T) {
	key := "Key"
	value := "Value"
	// field := "Field"
	member := "Member"
	defer test.ClearDb(rCli, testModisStringTableName, testModisHashTableName, testModisSetTableName)

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

	// // hash
	// hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hsetRedis, hsetModis)
	// delRedis, err = rCli.Del(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// delModis, err = mCli.Del(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, delRedis, delModis)

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
}

func TestKey_Exists(t *testing.T) {
	key := "Key"
	value := "Value"
	// field := "Field"
	member := "Member"
	defer test.ClearDb(rCli, testModisStringTableName, testModisHashTableName, testModisSetTableName)

	// empty
	existsRedis, err := rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existsModis, err := mCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existsRedis, existsModis)

	// string
	setRedis, err := rCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	setModis, err := mCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, setRedis, setModis)
	existsRedis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existsModis, err = mCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existsRedis, existsModis)
	delRedis, err := rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err := mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// // hash
	// hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hsetRedis, hsetModis)
	// existsRedis, err = rCli.Exists(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// existsModis, err = mCli.Exists(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, existsRedis, existsModis)
	// delRedis, err = rCli.Del(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// delModis, err = mCli.Del(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, delRedis, delModis)

	// set
	saddRedis, err := rCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	saddModis, err := mCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, saddRedis, saddModis)
	existsRedis, err = rCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	existsModis, err = mCli.Exists(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, existsRedis, existsModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)
}

func TestKey_Type(t *testing.T) {
	key := "Key"
	value := "Value"
	// field := "Field"
	member := "Member"
	defer test.ClearDb(rCli, testModisStringTableName, testModisHashTableName, testModisSetTableName)

	// empty
	typeRedis, err := rCli.Type(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	typeModis, err := mCli.Type(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, typeRedis, typeModis)

	// string
	setRedis, err := rCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	setModis, err := mCli.Set(context.TODO(), key, value, 0).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, setRedis, setModis)
	typeRedis, err = rCli.Type(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	typeModis, err = mCli.Type(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, typeRedis, typeModis)
	delRedis, err := rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err := mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// // hash
	// hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hsetRedis, hsetModis)
	// typeRedis, err = rCli.Type(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// typeModis, err = mCli.Type(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, typeRedis, typeModis)
	// delRedis, err = rCli.Del(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// delModis, err = mCli.Del(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, delRedis, delModis)

	// set
	saddRedis, err := rCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	saddModis, err := mCli.SAdd(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, saddRedis, saddModis)
	typeRedis, err = rCli.Type(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	typeModis, err = mCli.Type(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, typeRedis, typeModis)
	delRedis, err = rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err = mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)
}

func TestKey_Expire(t *testing.T) {
	key := "Key"
	value := "Value"
	// field := "Field"
	member := "Member"
	expiration := time.Second * 1
	defer test.ClearDb(rCli, testModisStringTableName, testModisHashTableName, testModisSetTableName)

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

	// // hash
	// hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hsetRedis, hsetModis)
	// expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	// assert.Equal(t, nil, err)
	// expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, expireRedis, expireModis)
	// time.Sleep(expiration)
	// hexistRedis, err := rCli.HExists(context.TODO(), key, field).Result()
	// assert.Equal(t, nil, err)
	// hexistModis, err := rCli.HExists(context.TODO(), key, field).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hexistRedis, hexistModis)

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
	ismemRedis, err := rCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	ismemModis, err := rCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, ismemRedis, ismemModis)
}

func TestKey_ExpireAt(t *testing.T) {
	key := "Key"
	value := "Value"
	// field := "Field"
	member := "Member"
	expiration := 1 * time.Second
	tm := time.Now().Add(expiration)
	defer test.ClearDb(rCli, testModisStringTableName, testModisHashTableName, testModisSetTableName)

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

	// // hash
	// hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hsetRedis, hsetModis)
	// expireRedis, err = rCli.ExpireAt(context.TODO(), key, tm).Result()
	// assert.Equal(t, nil, err)
	// expireModis, err = mCli.ExpireAt(context.TODO(), key, tm).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, expireRedis, expireModis)
	// time.Sleep(expiration)
	// hexistRedis, err := rCli.HExists(context.TODO(), key, field).Result()
	// assert.Equal(t, nil, err)
	// hexistModis, err := rCli.HExists(context.TODO(), key, field).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hexistRedis, hexistModis)

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
}

func TestKey_PExpire(t *testing.T) {
	key := "Key"
	value := "Value"
	// field := "Field"
	member := "Member"
	expiration := 1000 * time.Microsecond
	defer test.ClearDb(rCli, testModisStringTableName, testModisHashTableName, testModisSetTableName)

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

	// // hash
	// hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hsetRedis, hsetModis)
	// expireRedis, err = rCli.PExpire(context.TODO(), key, expiration).Result()
	// assert.Equal(t, nil, err)
	// expireModis, err = mCli.PExpire(context.TODO(), key, expiration).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, expireRedis, expireModis)
	// time.Sleep(expiration)
	// hexistRedis, err := rCli.HExists(context.TODO(), key, field).Result()
	// assert.Equal(t, nil, err)
	// hexistModis, err := rCli.HExists(context.TODO(), key, field).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hexistRedis, hexistModis)

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
}

func TestKey_PExpireAt(t *testing.T) {
	key := "Key"
	value := "Value"
	// field := "Field"
	member := "Member"
	expiration := 1000 * time.Microsecond
	tm := time.Now().Add(expiration)
	defer test.ClearDb(rCli, testModisStringTableName, testModisHashTableName, testModisSetTableName)

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

	// // hash
	// hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hsetRedis, hsetModis)
	// expireRedis, err = rCli.PExpireAt(context.TODO(), key, tm).Result()
	// assert.Equal(t, nil, err)
	// expireModis, err = mCli.PExpireAt(context.TODO(), key, tm).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, expireRedis, expireModis)
	// time.Sleep(expiration)
	// hexistRedis, err := rCli.HExists(context.TODO(), key, field).Result()
	// assert.Equal(t, nil, err)
	// hexistModis, err := rCli.HExists(context.TODO(), key, field).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hexistRedis, hexistModis)

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
}

func TestKey_Persist(t *testing.T) {
	key := "Key"
	value := "Value"
	// field := "Field"
	member := "Member"
	expiration := time.Second * 1
	defer test.ClearDb(rCli, testModisStringTableName, testModisHashTableName, testModisSetTableName)

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

	// // hash
	// hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hsetRedis, hsetModis)
	// expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	// assert.Equal(t, nil, err)
	// expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, expireRedis, expireModis)
	// persistRedis, err = rCli.Persist(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// persistModis, err = mCli.Persist(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, persistRedis, persistModis)
	// time.Sleep(expiration)
	// hexistRedis, err := rCli.HExists(context.TODO(), key, field).Result()
	// assert.Equal(t, nil, err)
	// hexistModis, err := rCli.HExists(context.TODO(), key, field).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hexistRedis, hexistModis)
	// delRedis, err = rCli.Del(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// delModis, err = mCli.Del(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, delRedis, delModis)

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
}

func TestKey_TTL(t *testing.T) {
	key := "Key"
	value := "Value"
	// field := "Field"
	member := "Member"
	expiration := time.Second * 10
	defer test.ClearDb(rCli, testModisStringTableName, testModisHashTableName, testModisSetTableName)

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
	assert.EqualValues(t, ttlRedis, ttlModis)
	delRedis, err := rCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	delModis, err := mCli.Del(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, delRedis, delModis)

	// // hash
	// hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hsetRedis, hsetModis)
	// expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	// assert.Equal(t, nil, err)
	// expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, expireRedis, expireModis)
	// ttlRedis, err = rCli.TTL(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// ttlModis, err = mCli.TTL(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, ttlRedis, ttlModis)
	// delRedis, err = rCli.Del(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// delModis, err = mCli.Del(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, delRedis, delModis)

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
}

func TestKey_PTTL(t *testing.T) {
	key := "Key"
	value := "Value"
	// field := "Field"
	member := "Member"
	expiration := time.Second * 10
	defer test.ClearDb(rCli, testModisStringTableName, testModisHashTableName, testModisSetTableName)

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

	// // hash
	// hsetRedis, err := rCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// hsetModis, err := mCli.HSet(context.TODO(), key, field, value).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, hsetRedis, hsetModis)
	// expireRedis, err = rCli.Expire(context.TODO(), key, expiration).Result()
	// assert.Equal(t, nil, err)
	// expireModis, err = mCli.Expire(context.TODO(), key, expiration).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, expireRedis, expireModis)
	// ttlRedis, err = rCli.PTTL(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// ttlModis, err = mCli.PTTL(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, 0, int64(ttlRedis.Seconds()-ttlModis.Seconds()))
	// delRedis, err = rCli.Del(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// delModis, err = mCli.Del(context.TODO(), key).Result()
	// assert.Equal(t, nil, err)
	// assert.EqualValues(t, delRedis, delModis)

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
}
