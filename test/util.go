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

package test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/oceanbase/modis/config"
	"github.com/oceanbase/modis/log"

	_ "github.com/go-mysql-org/go-mysql/driver"
	"github.com/go-redis/redis/v8"
)

// Config for Redis Server
const (
	RedisAddr = "127.0.0.1:6379"
	RedisPwd  = ""
	RedisDB   = 0
)

// Config for Modis
const (
	ModisAddr = "127.0.0.1:8085"
	ModisPwd  = ""
	ModisDB   = 0
)

// Config for Tests
const (
	SqlUser     = "root@mysql"
	SqlPassWord = ""
	SqlIp       = "127.0.0.1"
	SqlPort     = "20903"
	SqlDatabase = "test"
)

var GlobalDB *sql.DB

func CreateRedisClient() *redis.Client {
	cli := redis.NewClient(&redis.Options{
		Addr:     RedisAddr,
		Password: RedisPwd,
		DB:       RedisDB,
	})

	err := cli.Ping(context.TODO()).Err()
	if err != nil {
		panic(err)
	}

	return cli
}

func CreateModisClient() *redis.Client {
	cli := redis.NewClient(&redis.Options{
		Addr:     ModisAddr,
		Password: ModisPwd,
		DB:       ModisDB,
	})

	err := cli.Ping(context.TODO()).Err()
	if err != nil {
		panic(err)
	}

	return cli
}

func CreateDB() {
	if GlobalDB == nil {
		// dsn format: "user:password@addr?dbname"
		dsn := fmt.Sprintf("%s:%s@%s:%s?%s", SqlUser, SqlPassWord, SqlIp, SqlPort, SqlDatabase)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			panic(err.Error())
		}
		GlobalDB = db
	}
}

func ClearDb(db int64, rCli *redis.Client, tableNames ...string) {
	err := rCli.FlushDB(context.TODO()).Err()
	if err != nil {
		panic(err.Error())
	}
	for _, tb := range tableNames {
		delSql := "delete from " + tb + " where db = " + strconv.FormatInt(db, 10) + ";"
		_, err = GlobalDB.Exec(delSql)
		if err != nil {
			panic(err.Error())
		}
	}
}

func CloseDB() {
	GlobalDB.Close()
}

func CreateTable(createTableStatement string) {
	_, err := GlobalDB.Exec(createTableStatement)
	if err != nil {
		panic(err.Error())
	}
}

func DropTable(tableName string) {
	_, err := GlobalDB.Exec(fmt.Sprintf("drop table %s;", tableName))
	if err != nil {
		panic(err.Error())
	}
}

func TruncateTable(tableName string) {
	_, err := GlobalDB.Exec(fmt.Sprintf("truncate table %s;", tableName))
	if err != nil {
		panic(err.Error())
	}
}

func InitLogger() {
	cfg := config.LogConfig{
		FilePath:          ".",
		SingleFileMaxSize: 256,
		MaxBackupFileSize: 10,
		MaxAgeFileRem:     30,
		Compress:          false,
		Level:             "debug",
	}
	err := log.InitLoggerWithConfig(cfg)
	if err != nil {
		panic(err.Error())
	}
}
