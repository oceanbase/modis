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

package connection

import (
	"fmt"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/oceanbase/modis/test"
	"github.com/stretchr/testify/assert"
)

func TestAuth(t *testing.T) {
	// 1. without pass
	redisCli := redis.NewClient(&redis.Options{
		Addr:     test.RedisAddr,
		Password: "",
		DB:       test.RedisDB,
	})
	_, redis_err := redisCli.Ping(redisCli.Context()).Result()
	assert.NotEqual(t, nil, redis_err)
	assert.Equal(t, "NOAUTH Authentication required.", redis_err.Error())

	modisCli := redis.NewClient(&redis.Options{
		Addr:     test.ModisAddr,
		Password: "",
		DB:       test.ModisDB,
	})
	_, modis_err := modisCli.Ping(modisCli.Context()).Result()
	assert.NotEqual(t, nil, modis_err)
	assert.Equal(t, redis_err.Error(), modis_err.Error())

	// 2. wrong pass
	pass := "wrongpass"
	redisCli = redis.NewClient(&redis.Options{
		Addr:     test.RedisAddr,
		Password: pass,
		DB:       test.RedisDB,
	})
	_, redis_err = redisCli.Ping(redisCli.Context()).Result()
	assert.NotEqual(t, nil, redis_err)

	modisCli = redis.NewClient(&redis.Options{
		Addr:     test.ModisAddr,
		Password: pass,
		DB:       test.ModisDB,
	})
	_, modis_err = modisCli.Ping(modisCli.Context()).Result()
	assert.NotEqual(t, nil, modis_err)

	// 2. correct pass
	pass = "foobared"
	redisCli = redis.NewClient(&redis.Options{
		Addr:     test.RedisAddr,
		Password: pass,
		DB:       test.RedisDB,
	})
	var redis_pong, modis_pong string
	redis_pong, redis_err = redisCli.Ping(redisCli.Context()).Result()
	assert.Equal(t, nil, redis_err)
	fmt.Println("redis ping: " + redis_pong)

	modisCli = redis.NewClient(&redis.Options{
		Addr:     test.ModisAddr,
		Password: pass,
		DB:       test.ModisDB,
	})
	_, modis_err = modisCli.Ping(modisCli.Context()).Result()
	assert.Equal(t, nil, modis_err)
	fmt.Println("modis ping: " + modis_pong)
}
