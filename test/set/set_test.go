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

package set

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oceanbase/modis/test"
)

const (
	testModisSetTableName       = "modis_set_table"
	testModisSetCreateStatement = "create table if not exists modis_set_table(db bigint not null,rkey varbinary(1024) not null,member varbinary(1024) not null,expire_ts timestamp(6) default null,primary key(db, rkey, member)) TTL(expire_ts + INTERVAL 0 SECOND)partition by key(db, rkey) partitions 3;"
)

func generateTestData(count int) []string {
	users := make([]string, count)

	for i := 0; i < count; i++ {
		users[i] = "user" + strconv.Itoa(i+1)
	}

	return users
}

func difference(slice1, slice2 []string) []string {
	m := make(map[string]bool)
	diff := []string{}

	for _, s := range slice2 {
		m[s] = true
	}

	for _, s := range slice1 {
		if _, ok := m[s]; !ok {
			diff = append(diff, s)
		}
	}

	return diff
}

func TestSet_SAdd(t *testing.T) {
	key := "setKey"
	defer test.ClearDb(0, rCli, testModisSetTableName)

	members := generateTestData(10)
	for _, member := range members {
		err := rCli.SAdd(context.TODO(), key, member).Err()
		assert.Equal(t, nil, err)
		err = mCli.SAdd(context.TODO(), key, member).Err()
		assert.Equal(t, nil, err)
	}

	membersRedis, err := rCli.SMembers(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	membersModis, err := mCli.SMembers(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 0, len(difference(membersRedis, membersModis)))
}

func TestSet_SCard(t *testing.T) {
	key := "setKey"
	defer test.ClearDb(0, rCli, testModisSetTableName)

	countRedis, err := rCli.SCard(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	countModis, err := mCli.SCard(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, countRedis, countModis)

	members := generateTestData(10)
	for _, member := range members {
		err := rCli.SAdd(context.TODO(), key, member).Err()
		assert.Equal(t, nil, err)
		err = mCli.SAdd(context.TODO(), key, member).Err()
		assert.Equal(t, nil, err)
	}
	countRedis, err = rCli.SCard(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	countModis, err = mCli.SCard(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, countRedis, countModis)
}

func TestSet_SDiff(t *testing.T) {
	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	defer test.ClearDb(0, rCli, testModisSetTableName)

	diffRedis, err := rCli.SDiff(context.TODO(), key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	diffModis, err := mCli.SDiff(context.TODO(), key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, diffRedis, diffModis)

	err = rCli.SAdd(context.TODO(), key1, "a", "b", "c").Err()
	assert.Equal(t, nil, err)
	err = rCli.SAdd(context.TODO(), key2, "c", "d", "e").Err()
	assert.Equal(t, nil, err)
	err = rCli.SAdd(context.TODO(), key3, "e", "g", "h").Err()
	assert.Equal(t, nil, err)

	err = mCli.SAdd(context.TODO(), key1, "a", "b", "c").Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key2, "c", "d", "e").Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key3, "e", "g", "h").Err()
	assert.Equal(t, nil, err)

	diffRedis, err = rCli.SDiff(context.TODO(), key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	diffModis, err = mCli.SDiff(context.TODO(), key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 2, len(diffRedis))
	assert.EqualValues(t, 0, len(difference(diffRedis, diffModis)))
}

func TestSet_SDiffStore(t *testing.T) {
	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	dstKey := "set3"
	defer test.ClearDb(0, rCli, testModisSetTableName)

	countRedis, err := rCli.SDiffStore(context.TODO(), dstKey, key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	countModis, err := mCli.SDiffStore(context.TODO(), dstKey, key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, countRedis, countModis)

	err = rCli.SAdd(context.TODO(), key1, "a", "b", "c").Err()
	assert.Equal(t, nil, err)
	err = rCli.SAdd(context.TODO(), key2, "c", "d", "e").Err()
	assert.Equal(t, nil, err)
	err = rCli.SAdd(context.TODO(), key3, "e", "g", "h").Err()
	assert.Equal(t, nil, err)

	err = mCli.SAdd(context.TODO(), key1, "a", "b", "c").Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key2, "c", "d", "e").Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key3, "e", "g", "h").Err()
	assert.Equal(t, nil, err)

	countRedis, err = rCli.SDiffStore(context.TODO(), dstKey, key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	countModis, err = mCli.SDiffStore(context.TODO(), dstKey, key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, countRedis, countModis)
}

func TestSet_SInter(t *testing.T) {
	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	defer test.ClearDb(0, rCli, testModisSetTableName)

	interRedis, err := rCli.SInter(context.TODO(), key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	interModis, err := mCli.SInter(context.TODO(), key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, interRedis, interModis)

	err = rCli.SAdd(context.TODO(), key1, "a", "b", "c").Err()
	assert.Equal(t, nil, err)
	err = rCli.SAdd(context.TODO(), key2, "c", "d", "e").Err()
	assert.Equal(t, nil, err)
	err = rCli.SAdd(context.TODO(), key3, "c", "g", "h").Err()
	assert.Equal(t, nil, err)

	err = mCli.SAdd(context.TODO(), key1, "a", "b", "c").Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key2, "c", "d", "e").Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key3, "c", "g", "h").Err()
	assert.Equal(t, nil, err)

	interRedis, err = rCli.SInter(context.TODO(), key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	interModis, err = mCli.SInter(context.TODO(), key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, interRedis, interModis)
}

func TestSet_SInterStore(t *testing.T) {
	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	dstKey := "set3"
	defer test.ClearDb(0, rCli, testModisSetTableName)

	countRedis, err := rCli.SInterStore(context.TODO(), dstKey, key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	countModis, err := mCli.SInterStore(context.TODO(), dstKey, key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, countRedis, countModis)

	err = rCli.SAdd(context.TODO(), key1, "a", "b", "c").Err()
	assert.Equal(t, nil, err)
	err = rCli.SAdd(context.TODO(), key2, "c", "d", "e").Err()
	assert.Equal(t, nil, err)
	err = rCli.SAdd(context.TODO(), key3, "c", "g", "h").Err()
	assert.Equal(t, nil, err)

	err = mCli.SAdd(context.TODO(), key1, "a", "b", "c").Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key2, "c", "d", "e").Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key3, "c", "g", "h").Err()
	assert.Equal(t, nil, err)

	countRedis, err = rCli.SInterStore(context.TODO(), dstKey, key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	countModis, err = mCli.SInterStore(context.TODO(), dstKey, key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, countRedis, countModis)
}

func TestSet_SUnion(t *testing.T) {
	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	defer test.ClearDb(0, rCli, testModisSetTableName)

	unionRedis, err := rCli.SUnion(context.TODO(), key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	unionModis, err := mCli.SUnion(context.TODO(), key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, unionRedis, unionModis)

	err = rCli.SAdd(context.TODO(), key1, "a", "b", "c").Err()
	assert.Equal(t, nil, err)
	err = rCli.SAdd(context.TODO(), key2, "c", "d", "e").Err()
	assert.Equal(t, nil, err)
	err = rCli.SAdd(context.TODO(), key3, "c", "g", "h").Err()
	assert.Equal(t, nil, err)

	err = mCli.SAdd(context.TODO(), key1, "a", "b", "c").Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key2, "c", "d", "e").Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key3, "c", "g", "h").Err()
	assert.Equal(t, nil, err)

	unionRedis, err = rCli.SUnion(context.TODO(), key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	unionModis, err = mCli.SUnion(context.TODO(), key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 7, len(unionRedis))
	assert.EqualValues(t, 0, len(difference(unionRedis, unionModis)))
}

func TestSet_SUnionStore(t *testing.T) {
	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	dstKey := "set3"
	defer test.ClearDb(0, rCli, testModisSetTableName)

	countRedis, err := rCli.SUnionStore(context.TODO(), dstKey, key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	countModis, err := mCli.SUnionStore(context.TODO(), dstKey, key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, countRedis, countModis)

	err = rCli.SAdd(context.TODO(), key1, "a", "b", "c").Err()
	assert.Equal(t, nil, err)
	err = rCli.SAdd(context.TODO(), key2, "c", "d", "e").Err()
	assert.Equal(t, nil, err)
	err = rCli.SAdd(context.TODO(), key3, "c", "g", "h").Err()
	assert.Equal(t, nil, err)

	err = mCli.SAdd(context.TODO(), key1, "a", "b", "c").Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key2, "c", "d", "e").Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key3, "c", "g", "h").Err()
	assert.Equal(t, nil, err)

	countRedis, err = rCli.SUnionStore(context.TODO(), dstKey, key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	countModis, err = mCli.SUnionStore(context.TODO(), dstKey, key1, key2, key3).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, countRedis, countModis)
}

func TestSet_SMembers(t *testing.T) {
	key := "setKey"
	defer test.ClearDb(0, rCli, testModisSetTableName)

	membersRedis, err := rCli.SMembers(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	membersModis, err := mCli.SMembers(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, membersRedis, membersModis)

	members := generateTestData(10)
	for _, member := range members {
		err := rCli.SAdd(context.TODO(), key, member).Err()
		assert.Equal(t, nil, err)
		err = mCli.SAdd(context.TODO(), key, member).Err()
		assert.Equal(t, nil, err)
	}

	membersRedis, err = rCli.SMembers(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	membersModis, err = mCli.SMembers(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, 0, len(difference(membersRedis, membersModis)))
}

func TestSet_SIsmember(t *testing.T) {
	key := "setKey"
	member := "member"
	memberNot := "memberNot"
	defer test.ClearDb(0, rCli, testModisSetTableName)

	isMemberRedis, err := rCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	isMemberModis, err := mCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, isMemberRedis, isMemberModis)

	err = rCli.SAdd(context.TODO(), key, member).Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key, member).Err()
	assert.Equal(t, nil, err)

	isMemberRedis, err = rCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	isMemberModis, err = mCli.SIsMember(context.TODO(), key, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, isMemberRedis, isMemberModis)

	isMemberRedis, err = rCli.SIsMember(context.TODO(), key, memberNot).Result()
	assert.Equal(t, nil, err)
	isMemberModis, err = mCli.SIsMember(context.TODO(), key, memberNot).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, isMemberRedis, isMemberModis)
}

func TestSet_SMove(t *testing.T) {
	key1 := "set1"
	key2 := "set2"
	member := "member"
	memberNot := "memberNot"
	defer test.ClearDb(0, rCli, testModisSetTableName)

	moveRedis, err := rCli.SMove(context.TODO(), key1, key2, member).Result()
	assert.Equal(t, nil, err)
	moveModis, err := mCli.SMove(context.TODO(), key1, key2, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, moveRedis, moveModis)

	err = rCli.SAdd(context.TODO(), key1, member).Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key1, member).Err()
	assert.Equal(t, nil, err)

	moveRedis, err = rCli.SMove(context.TODO(), key1, key2, member).Result()
	assert.Equal(t, nil, err)
	moveModis, err = mCli.SMove(context.TODO(), key1, key2, member).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, moveRedis, moveModis)

	moveRedis, err = rCli.SMove(context.TODO(), key1, key2, memberNot).Result()
	assert.Equal(t, nil, err)
	moveModis, err = mCli.SMove(context.TODO(), key1, key2, memberNot).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, moveRedis, moveModis)
}

func TestSet_SPop(t *testing.T) {
	key := "set"
	member := "member"
	var count int64 = 10
	defer test.ClearDb(0, rCli, testModisSetTableName)

	spopRedis, errRedis := rCli.SPopN(context.TODO(), key, count).Result()
	spopModis, errModis := mCli.SPopN(context.TODO(), key, count).Result()
	assert.EqualValues(t, errRedis, errModis)

	err := rCli.SAdd(context.TODO(), key, member).Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key, member).Err()
	assert.Equal(t, nil, err)

	spopRedis, err = rCli.SPopN(context.TODO(), key, count).Result()
	assert.Equal(t, nil, err)
	spopModis, err = mCli.SPopN(context.TODO(), key, count).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, spopRedis, spopModis)
}

// todo: protocal error
func TestSet_SRandMember(t *testing.T) {
	key := "setKey"
	member := "member"
	var count int64 = 1
	defer test.ClearDb(0, rCli, testModisSetTableName)

	memberRedis, err := rCli.SRandMemberN(context.TODO(), key, count).Result()
	assert.Equal(t, nil, err)
	memberModis, err := mCli.SRandMemberN(context.TODO(), key, count).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, memberRedis, memberModis)

	err = rCli.SAdd(context.TODO(), key, member).Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key, member).Err()
	assert.Equal(t, nil, err)

	memberRedis, err = rCli.SRandMemberN(context.TODO(), key, count).Result()
	assert.Equal(t, nil, err)
	memberModis, err = mCli.SRandMemberN(context.TODO(), key, count).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, memberRedis, memberModis)
}

func TestSet_SRem(t *testing.T) {
	key := "setKey"
	member := "member"
	memberNot := "memberNot"
	defer test.ClearDb(0, rCli, testModisSetTableName)

	sremRedis, err := rCli.SRem(context.TODO(), key, member, memberNot).Result()
	assert.Equal(t, nil, err)
	sremModis, err := mCli.SRem(context.TODO(), key, member, memberNot).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, sremRedis, sremModis)

	err = rCli.SAdd(context.TODO(), key, member).Err()
	assert.Equal(t, nil, err)
	err = mCli.SAdd(context.TODO(), key, member).Err()
	assert.Equal(t, nil, err)

	sremRedis, err = rCli.SRem(context.TODO(), key, member, memberNot).Result()
	assert.Equal(t, nil, err)
	sremModis, err = mCli.SRem(context.TODO(), key, member, memberNot).Result()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, sremRedis, sremModis)
}
