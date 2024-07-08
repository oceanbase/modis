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

package command

import (
	"bytes"
	"errors"
	"strings"

	"github.com/oceanbase/modis/connection/conncontext"
	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/modis/storage/obkv"
)

type void struct{}

const (
	dbColumnName     = "db"
	keyColumnName    = "rkey"
	valueColumnName  = "value"
	expireColumnName = "expire_ts"
	memberColumnName = "member"
	indexColumnName  = "index"
	stringTableName  = "modis_string_table"
	setTableName     = "modis_set_table"
	listTableName    = "modis_list_table"
	hashTableName    = "modis_hash_table"
	zsetTableName    = "modis_zset_table"
)

var (
	tbNames = []string{
		stringTableName,
		hashTableName,
		setTableName,
		zsetTableName,
		listTableName,
	}
	models = []string{
		"string",
		"hash",
		"set",
		"zset",
		"list",
	}
)

func bitCount(bytes []byte, start, end int) (int, error) {
	length := len(bytes)
	if end < 0 {
		end = length + end
	}

	if start < 0 {
		start = length + start
	}

	if start > end || start >= length {
		return 0, nil
	}

	if start < 0 {
		start = 0
	}

	if end < 0 {
		end = 0
	} else if end >= length {
		end = length - 1
	}

	count := 0
	for i := start; i <= end; i++ {
		for j := 0; j < 8; j++ {
			if (bytes[i]>>j)&1 == 1 {
				count++
			}
		}
	}

	return count, nil
}

func getRange(bytes []byte, start, end int) []byte {
	if start < 0 && end < 0 && start > end {
		return nil
	}
	length := len(bytes)
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = length + start
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end || length == 0 {
		return nil
	}
	return bytes[start : end+1]
}

func setRange(bytes []byte, offset int64, value []byte) []byte {
	if int64(len(bytes)) < offset+int64(len(value)) {
		bytes = append(bytes, make([]byte, offset+int64(len(value))-int64(len(bytes)))...)
	}
	copy(bytes[offset:], value)

	return bytes
}

func getExclusiveElements(firstMembers [][]byte, secondMembers [][]byte) [][]byte {
	exclusiveElements := [][]byte{}

	for _, member := range firstMembers {
		found := false
		for _, otherMember := range secondMembers {
			if bytes.Equal(member, otherMember) {
				found = true
				break
			}
		}
		if !found {
			exclusiveElements = append(exclusiveElements, member)
		}
	}

	return exclusiveElements
}

func getIntersection(slices ...[][]byte) [][]byte {
	if len(slices) == 0 {
		return [][]byte{}
	}

	intersection := make(map[string]int)

	for _, slice := range slices {
		for _, element := range slice {
			intersection[string(element)]++
		}
	}

	var result [][]byte

	for element, count := range intersection {
		if count == len(slices) {
			result = append(result, []byte(element))
		}
	}

	return result
}

func getUnion(slices ...[][]byte) [][]byte {
	union := make(map[string]bool)

	for _, slice := range slices {
		for _, element := range slice {
			union[string(element)] = true
		}
	}

	result := [][]byte{}

	for element := range union {
		result = append(result, []byte(element))
	}

	return result
}

func getDifference(slices ...[][]byte) [][]byte {
	if len(slices) == 0 {
		return nil
	}

	diff := slices[0]

	for i := 1; i < len(slices); i++ {
		diff = getExclusiveElements(diff, slices[i])
	}

	return diff
}

func clientFlag2Str(flag conncontext.ClientFlag) string {
	flagStr := ""
	if (flag & conncontext.ClientMonitor) != 0 {
		flagStr += "O"
	}
	if flagStr == "" {
		flagStr = "N"
	}
	return flagStr
}

func getDBInfo(ctx *CmdContext, db int64) (*DBInfo, error) {
	var tbInfo *obkv.TableInfo
	var err error
	dbInfo := &DBInfo{Keys: 0, Expires: 0}
	for _, tbName := range tables {
		tbInfo, err = ctx.CodecCtx.DB.Storage.GetTableInfo(ctx.CodecCtx.DB.Ctx, db, tbName)
		if err != nil {
			log.Warn("command", ctx.TraceID, "fail to get table info",
				log.Errors(err), log.Int64("db", db), log.String("table name", tbName))
			return nil, err
		}
		dbInfo.Keys += tbInfo.Keys
		dbInfo.Expires += tbInfo.Expires
	}
	return dbInfo, nil
}

func replaceWithRedacted(arg []byte) {
	red := []byte("(redacted)")
	if !bytes.Equal(arg, red) {
		arg = red
	}
}

func getTableNameByModel(model string) (string, error) {
	switch strings.ToLower(model) {
	case "hash":
		return hashTableName, nil
	case "list":
		return listTableName, nil
	case "set":
		return setTableName, nil
	case "zset":
		return zsetTableName, nil
	case "string":
		return stringTableName, nil
	}
	return "", errors.New("invalid model name")
}
