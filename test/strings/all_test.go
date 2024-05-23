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
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/go-redis/redis/v8"

	"github.com/oceanbase/modis/test"
)

var redisCli *redis.Client
var modisCli *redis.Client
var GlobalDB *sql.DB

func setup() {
	// redis
	redisCli = test.CreateRedisClient()
	redis_pong, err := redisCli.Ping(redisCli.Context()).Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("redis ping: " + redis_pong)

	// modis
	modisCli = test.CreateModisClient()
	modis_pong, err := modisCli.Ping(modisCli.Context()).Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("modis ping: " + modis_pong)

	// Connect
	test.CreateDB()
	test.CreateTable(createStringTable)
	test.ClearDb(0, redisCli, stringTableName)
}

func teardown() {
	redisCli.Close()
	modisCli.Close()
	test.CloseDB()
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}
