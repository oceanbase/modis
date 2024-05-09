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
