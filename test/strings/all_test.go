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

package strings

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"

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
