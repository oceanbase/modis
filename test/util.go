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

package test

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/oceanbase/modis/config"
	"github.com/oceanbase/modis/log"

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
	SqlPort     = ""
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
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", SqlUser, SqlPassWord, SqlIp, SqlPort, SqlDatabase)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			panic(err.Error())
		}
		GlobalDB = db
	}
}

func ClearDb(rCli *redis.Client, tableNames ...string) {
	err := rCli.FlushDB(context.TODO()).Err()
	if err != nil {
		panic(err.Error())
	}
	for _, tb := range tableNames {
		_, err = GlobalDB.Exec("truncate table " + tb + ";")
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

func DeleteTable(tableName string) {
	_, err := GlobalDB.Exec(fmt.Sprintf("delete from %s;", tableName))
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
