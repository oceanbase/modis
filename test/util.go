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
	"os"
	"strconv"

	"github.com/fsnotify/fsnotify"

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
	SqlUser     = "root@mysql_tenant"
	SqlPassWord = ""
	SqlIp       = "11.162.218.236"
	SqlPort     = "55605"
	SqlDatabase = "test"
)

// table sql
const (
	TestModisZSetTableName       = "modis_zset_table"
	TestModisZSetCreateStatement = `CREATE TABLE if not exists modis_zset_table(
	db bigint not null,
	rkey varbinary(1024) not null,
	is_data tinyint(1) default 1,
	insert_ts timestamp(6) DEFAULT NULL,
	expire_ts timestamp(6) default null,
	member varbinary(1024) not null,
	score double default null,
	index index_score(db, rkey, score) local,
	PRIMARY KEY(db, rkey, is_data, member))
	KV_ATTRIBUTES ='{"Redis": {"isTTL": true, "model": "zset"}}'
	PARTITION BY KEY(db, rkey) PARTITIONS 3;`

	TestModisListTableName       = "modis_list_table"
	TestModisListCreateStatement = "CREATE TABLE if not exists modis_list_table(" +
		"db BIGINT NOT NULL," +
		"rkey VARBINARY(1024) NOT NULL," +
		"is_data tinyint(1) default 1," +
		"insert_ts TIMESTAMP(6) DEFAULT NULL, " +
		"expire_ts timestamp(6) default null," +
		"value VARBINARY(1024) DEFAULT NULL," +
		"`index` BIGINT NOT NULL," +
		"PRIMARY KEY(db, rkey, is_data, `index`)" +
		")" +
		"KV_ATTRIBUTES ='{\"Redis\": {\"isTTL\": true, \"model\": \"list\"}}'" +
		"PARTITION BY KEY(db, rkey)" +
		"PARTITIONS 3;"

	TestModisStringTableName       = "modis_string_table"
	TestModisStringCreateStatement = `create table if not exists modis_string_table(
		db bigint not null,
		rkey varbinary(1024) not null,
		value varbinary(1024) not null,
		expire_ts timestamp(6) default null,
		primary key(db, rkey)) 
		TTL(expire_ts + INTERVAL 0 SECOND) 
		partition by key(db, rkey) partitions 3;`

	TestModisSetTableName       = "modis_set_table"
	TestModisSetCreateStatement = `CREATE TABLE if not exists modis_set_table(
		db bigint not null,
		rkey varbinary(1024) not null,
		is_data tinyint(1) default 1,
		insert_ts timestamp(6) DEFAULT NULL,
		expire_ts timestamp(6) default null,
		member varbinary(1024) not null,
		PRIMARY KEY(db, rkey, is_data, member))
		KV_ATTRIBUTES ='{"Redis": {"isTTL": true, "model": "zset"}}'
		PARTITION BY KEY(db, rkey) PARTITIONS 3;`

	TestModisHashTableName       = "modis_hash_table"
	TestModisHashCreateStatement = `CREATE TABLE if not exists modis_hash_table(
		db bigint not null,
		rkey varbinary(1024) not null,
		is_data tinyint(1) default 1,
		insert_ts timestamp(6) DEFAULT NULL,
		expire_ts timestamp(6) default null,
		field varbinary(1024) not null,
		value varbinary(1024) default null,
		PRIMARY KEY(db, rkey, is_data, field))
		KV_ATTRIBUTES ='{"Redis": {"isTTL": true, "model": "hash"}}'
		PARTITION BY KEY(db, rkey) PARTITIONS 3;`
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
	log_watcher, err := fsnotify.NewWatcher()
	if err != nil {
		fmt.Println("fail to init watcher, ", err)
		os.Exit(1)
	}
	defer log_watcher.Close()

	err = log.InitLoggerWithConfig(cfg, log_watcher)
	if err != nil {
		panic(err.Error())
	}
}
