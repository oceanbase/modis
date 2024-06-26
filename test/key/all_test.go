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

package key

import (
	"os"
	"testing"

	"github.com/go-redis/redis/v8"

	"github.com/oceanbase/modis/test"
)

var rCli *redis.Client
var mCli *redis.Client

func setup() {
	rCli = test.CreateRedisClient()
	mCli = test.CreateModisClient()

	test.CreateDB()

	test.CreateTable(test.TestModisStringCreateStatement)
	test.CreateTable(test.TestModisHashCreateStatement)
	test.CreateTable(test.TestModisSetCreateStatement)
	test.CreateTable(test.TestModisZSetCreateStatement)
	test.CreateTable(test.TestModisListCreateStatement)
	test.ClearDb(0, rCli, test.TestModisSetTableName, test.TestModisStringTableName, test.TestModisHashTableName, test.TestModisZSetTableName, test.TestModisListTableName)
}

func teardown() {
	rCli.Close()
	mCli.Close()

	test.DropTable(test.TestModisStringTableName)
	test.DropTable(test.TestModisSetTableName)
	test.DropTable(test.TestModisHashTableName)
	test.DropTable(test.TestModisZSetTableName)
	test.DropTable(test.TestModisListTableName)
	test.CloseDB()
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}
