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

package zset

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/oceanbase/modis/test"
	"github.com/stretchr/testify/assert"
)

func generateTestData(count int) ([]string, []float64) {
	members := make([]string, count)
	scores := make([]float64, count)

	for i := 0; i < count; i++ {
		members[i] = "user" + strconv.Itoa(i+1)
		scores[i] = float64(i+1) * 1.1
	}

	return members, scores
}

func do_zadd(t *testing.T, key string, members []string, scores []float64) error {
	if len(members) == 0 || len(members) != len(scores) {
		return errors.New("invalid members or scores len")
	}
	// do zadd with insert
	for i, member := range members {
		added, err := rCli.ZAdd(context.TODO(), key, &redis.Z{Score: scores[i], Member: member}).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(1), added)

		added_m, err := mCli.ZAdd(context.TODO(), key, &redis.Z{Score: scores[i], Member: member}).Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, added, added_m)
	}

	// do zadd with update
	added, err := rCli.ZAdd(context.TODO(), key, &redis.Z{Score: scores[0], Member: members[0]}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, added, int64(0))

	added_m, err := mCli.ZAdd(context.TODO(), key, &redis.Z{Score: scores[0], Member: members[0]}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, added, added_m)
	return nil
}

func TestZRange(t *testing.T) {
	key := "zsetkey"
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	members, scores := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key, members, scores))

	// check zrange 1
	vals, err := rCli.ZRangeWithScores(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []redis.Z{{
		Score:  scores[0],
		Member: members[0],
	}, {
		Score:  scores[1],
		Member: members[1],
	}, {
		Score:  scores[2],
		Member: members[2],
	}}, vals)

	vals_m, err := mCli.ZRangeWithScores(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, vals, vals_m)

	// check zrange 2
	vals, err = rCli.ZRangeWithScores(context.TODO(), key, 2, 3).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []redis.Z{{
		Score:  scores[2],
		Member: members[2],
	}}, vals)

	vals_m, err = mCli.ZRangeWithScores(context.TODO(), key, 2, 3).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, vals, vals_m)

	// check zrange 3
	vals, err = rCli.ZRangeWithScores(context.TODO(), key, -3, -2).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []redis.Z{{
		Score:  scores[0],
		Member: members[0],
	}, {
		Score:  scores[1],
		Member: members[1],
	}}, vals)

	vals_m, err = mCli.ZRangeWithScores(context.TODO(), key, -3, -2).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, vals, vals_m)
}

func TestZRevRange(t *testing.T) {
	key := "zsetkey"
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	members, scores := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key, members, scores))

	// check zrevrange 1
	vals, err := rCli.ZRevRangeWithScores(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []redis.Z{{
		Score:  scores[2],
		Member: members[2],
	}, {
		Score:  scores[1],
		Member: members[1],
	}, {
		Score:  scores[0],
		Member: members[0],
	}}, vals)

	vals_m, err := mCli.ZRevRangeWithScores(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, vals, vals_m)

	// check zrevrange 2
	vals, err = rCli.ZRevRangeWithScores(context.TODO(), key, 2, 3).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []redis.Z{{
		Score:  scores[0],
		Member: members[0],
	}}, vals)

	vals_m, err = mCli.ZRevRangeWithScores(context.TODO(), key, 2, 3).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, vals, vals_m)

	// check zrevrange 3
	vals, err = rCli.ZRevRangeWithScores(context.TODO(), key, -3, -2).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []redis.Z{{
		Score:  scores[2],
		Member: members[2],
	}, {
		Score:  scores[1],
		Member: members[1],
	}}, vals)

	vals_m, err = mCli.ZRevRangeWithScores(context.TODO(), key, -3, -2).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, vals, vals_m)
}

func TestZRem(t *testing.T) {
	key := "zsetkey"
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	members, scores := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key, members, scores))

	// do zrem
	zRemCount, err := rCli.ZRem(context.TODO(), key, members[1]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(1), zRemCount)

	zRemCount_m, err := mCli.ZRem(context.TODO(), key, members[1]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, zRemCount, zRemCount_m)

	// check result
	vals, err := rCli.ZRangeWithScores(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []redis.Z{{
		Score:  scores[0],
		Member: members[0],
	}, {
		Score:  scores[2],
		Member: members[2],
	}}, vals)

	vals_m, err := mCli.ZRangeWithScores(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, vals, vals_m)
}

func TestZCard(t *testing.T) {
	key := "zsetkey"
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	members, scores := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key, members, scores))

	// do zcard
	card, err := rCli.ZCard(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(3), card)

	card_m, err := mCli.ZCard(context.TODO(), key).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, card, card_m)
}

func TestZIncrBy(t *testing.T) {
	key := "zsetkey"
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	members, scores := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key, members, scores))

	// do incrby
	n, err := rCli.ZIncrBy(context.TODO(), key, 2, members[2]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, scores[2]+2, n)

	n_m, err := mCli.ZIncrBy(context.TODO(), key, 2, members[2]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, n, n_m)

	// check result
	vals, err := rCli.ZRangeWithScores(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []redis.Z{{
		Score:  scores[0],
		Member: members[0],
	}, {
		Score:  scores[1],
		Member: members[1],
	}, {
		Score:  scores[2] + 2,
		Member: members[2],
	}}, vals)

	vals_m, err := mCli.ZRangeWithScores(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, vals, vals_m)
}

func TestZScore(t *testing.T) {
	key := "zsetkey"
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	members, scores := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key, members, scores))

	// do zscore
	score, err := rCli.ZScore(context.TODO(), key, members[1]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, scores[1], score)

	score_m, err := mCli.ZScore(context.TODO(), key, members[1]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, score, score_m)
}

func TestZRank(t *testing.T) {
	key := "zsetkey"
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	members, scores := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key, members, scores))

	// do zrevrank
	rank, err := rCli.ZRank(context.TODO(), key, members[2]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(2), rank)

	rank_m, err := mCli.ZRank(context.TODO(), key, members[2]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, rank, rank_m)
}

func TestZRevRank(t *testing.T) {
	key := "zsetkey"
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	members, scores := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key, members, scores))

	rank, err := rCli.ZRevRank(context.TODO(), key, members[2]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(0), rank)

	rank_m, err := mCli.ZRevRank(context.TODO(), key, members[2]).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, rank, rank_m)
}

func TestZRemRangeByRank(t *testing.T) {
	key := "zsetkey"
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	members, scores := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key, members, scores))

	n, err := rCli.ZRemRangeByRank(context.TODO(), key, 0, 1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(2), n)

	n_m, err := mCli.ZRemRangeByRank(context.TODO(), key, 0, 1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, n, n_m)

	// check result
	vals, err := rCli.ZRangeWithScores(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []redis.Z{{
		Score:  scores[2],
		Member: members[2],
	}}, vals)

	vals_m, err := mCli.ZRangeWithScores(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, vals, vals_m)
}

func TestZCount(t *testing.T) {
	key := "zsetkey"
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	members, scores := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key, members, scores))

	// 1
	n, err := rCli.ZCount(context.TODO(), key, "-inf", "+inf").Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(3), n)

	n_m, err := mCli.ZCount(context.TODO(), key, "-inf", "+inf").Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, n, n_m)

	// 2
	n, err = rCli.ZCount(context.TODO(), key, "(1", "3").Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(2), n)

	n_m, err = mCli.ZCount(context.TODO(), key, "(1", "3").Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, n, n_m)
}

func TestZRangeByScore(t *testing.T) {
	key := "zsetkey"
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	members, scores := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key, members, scores))

	// 1
	rangeScores, err := rCli.ZRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []string{members[0], members[1], members[2]}, rangeScores)

	rangeScores_m, err := mCli.ZRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, rangeScores, rangeScores_m)

	// 2
	rangeScores, err = rCli.ZRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Min: "(1.1",
		Max: "2.2",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []string{members[1]}, rangeScores)

	rangeScores_m, err = mCli.ZRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Min: "(1.1",
		Max: "2.2",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, rangeScores, rangeScores_m)

	// 3
	rangeScores, err = rCli.ZRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Min: "1.1",
		Max: "2.2",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []string{members[0], members[1]}, rangeScores)

	rangeScores_m, err = mCli.ZRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Min: "1.1",
		Max: "2.2",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, rangeScores, rangeScores_m)

	// 4
	rangeScores, err = rCli.ZRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Min: "(1.1",
		Max: "(2.2",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []string{}, rangeScores)

	rangeScores_m, err = mCli.ZRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Min: "(1.1",
		Max: "(2.2",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, rangeScores, rangeScores_m)
}

func TestZRevRangeByScore(t *testing.T) {
	key := "zsetkey"
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	members, scores := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key, members, scores))

	// 1
	rangeScores, err := rCli.ZRevRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []string{members[2], members[1], members[0]}, rangeScores)

	rangeScores_m, err := mCli.ZRevRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, rangeScores, rangeScores_m)

	// 2
	rangeScores, err = rCli.ZRevRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Max: "2.2",
		Min: "(1.1",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []string{members[1]}, rangeScores)

	rangeScores_m, err = mCli.ZRevRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Max: "2.2",
		Min: "(1.1",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, rangeScores, rangeScores_m)

	// 3
	rangeScores, err = rCli.ZRevRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Max: "2.2",
		Min: "1.1",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []string{members[1], members[0]}, rangeScores)

	rangeScores_m, err = mCli.ZRevRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Max: "2.2",
		Min: "1.1",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, rangeScores, rangeScores_m)

	// 4
	rangeScores, err = rCli.ZRevRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Max: "(2.2",
		Min: "(1.1",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []string{}, rangeScores)

	rangeScores_m, err = mCli.ZRevRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Max: "(2.2",
		Min: "(1.1",
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, rangeScores, rangeScores_m)
}

func TestZRemRangeByScore(t *testing.T) {
	key := "zsetkey"
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	members, scores := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key, members, scores))

	zRemCount, err := rCli.ZRemRangeByScore(context.TODO(), key, "-inf", "(2").Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(1), zRemCount)

	zRemCount_m, err := mCli.ZRemRangeByScore(context.TODO(), key, "-inf", "(2").Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, zRemCount, zRemCount_m)

	// check result
	vals, err := rCli.ZRangeWithScores(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []redis.Z{{
		Score:  scores[1],
		Member: members[1],
	}, {
		Score:  scores[2],
		Member: members[2],
	}}, vals)

	vals_m, err := mCli.ZRangeWithScores(context.TODO(), key, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, vals, vals_m)
}

func TestZUnionStore(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	key1 := "zsetkey1"
	members1, scores1 := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key1, members1, scores1))
	key2 := "zsetkey2"
	members2, scores2 := generateTestData(2)
	assert.Equal(t, nil, do_zadd(t, key2, members2, scores2))
	outKey := "out"

	n, err := rCli.ZUnionStore(context.TODO(), outKey, &redis.ZStore{
		Keys:    []string{key1, key2},
		Weights: []float64{2, 3},
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(3), n)

	n_m, err := mCli.ZUnionStore(context.TODO(), outKey, &redis.ZStore{
		Keys:    []string{key1, key2},
		Weights: []float64{2, 3},
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, n, n_m)

	// check result
	vals, err := rCli.ZRangeWithScores(context.TODO(), outKey, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []redis.Z{{
		Score:  2*scores1[0] + 3*scores2[0],
		Member: members1[0],
	}, {
		Score:  2 * scores1[2],
		Member: members1[2],
	}, {
		Score:  2*scores1[1] + 3*scores2[1],
		Member: members1[1],
	}}, vals)

	vals_m, err := mCli.ZRangeWithScores(context.TODO(), outKey, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, vals, vals_m)
}

func TestZInterStore(t *testing.T) {
	defer test.ClearDb(0, rCli, test.TestModisZSetTableName)
	key1 := "zsetkey1"
	members1, scores1 := generateTestData(2)
	assert.Equal(t, nil, do_zadd(t, key1, members1, scores1))
	key2 := "zsetkey2"
	members2, scores2 := generateTestData(3)
	assert.Equal(t, nil, do_zadd(t, key2, members2, scores2))
	outKey := "out"

	n, err := rCli.ZInterStore(context.TODO(), outKey, &redis.ZStore{
		Keys:    []string{key1, key2},
		Weights: []float64{2, 3},
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(2), n)

	n_m, err := mCli.ZInterStore(context.TODO(), outKey, &redis.ZStore{
		Keys:    []string{key1, key2},
		Weights: []float64{2, 3},
	}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, n, n_m)

	// check result
	vals, err := rCli.ZRangeWithScores(context.TODO(), outKey, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, []redis.Z{{
		Score:  2*scores1[0] + 3*scores2[0],
		Member: members1[0],
	}, {
		Score:  2*scores1[1] + 3*scores2[1],
		Member: members1[1],
	}}, vals)

	vals_m, err := mCli.ZRangeWithScores(context.TODO(), outKey, 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, vals, vals_m)
}
