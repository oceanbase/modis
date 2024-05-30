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
	"strconv"
	"strings"

	"github.com/oceanbase/obkv-table-client-go/table"
	"github.com/oceanbase/obkv-table-client-go/util"

	"github.com/oceanbase/modis/protocol/resp"
)

const (
	hashTableName = "modis_hash_table"
)

// HDel removes the specified fields from the hash stored at key
func HDel(ctx *CmdContext) error {
	key := ctx.Args[0]
	kvs := ctx.Args[1:]
	fields := make([][]byte, len(kvs))
	copy(fields, kvs)

	deleteNum, err := ctx.CodecCtx.DB.Storage.HDel(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, fields)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(deleteNum)
	}
	return nil
}

// HSetNX sets field in the hash stored at key to value, only if field does not yet exist
func HSetNX(ctx *CmdContext) error {
	key := ctx.Args[0]
	if len(ctx.Args) != 3 {
		ctx.OutContent = resp.EncError("ERR wrong number of arguments for 'hsetnx' command")
	} else {
		field := ctx.Args[1]
		value := ctx.Args[2]
		insertCount, err := ctx.CodecCtx.DB.Storage.HSetNx(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, field, value)
		if err != nil {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
		} else {
			ctx.OutContent = resp.EncInteger(int64(insertCount))
		}
	}
	return nil
}

// HGet returns the value associated with field in the hash stored at key
func HGet(ctx *CmdContext) error {
	key := ctx.Args[0]
	field := ctx.Args[1]
	val, err := ctx.CodecCtx.DB.Storage.HGet(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, field)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		if val == nil {
			ctx.OutContent = resp.EncNullBulkString()
		} else {
			ctx.OutContent = resp.EncBulkString(util.BytesToString(val))
		}
	}
	return nil
}

// HGetAll returns all fields and values of the hash stored at key
func HGetAll(ctx *CmdContext) error {
	key := ctx.Args[0]
	resValue, err := ctx.CodecCtx.DB.Storage.HGetAll(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncArray(resValue)
	}
	return nil
}

// HExists returns if field is an existing field in the hash stored at key
func HExists(ctx *CmdContext) error {
	key := ctx.Args[0]
	field := ctx.Args[1]
	val, err := ctx.CodecCtx.DB.Storage.HGet(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, field)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		if val == nil {
			ctx.OutContent = resp.EncInteger(int64(0))
		} else {
			ctx.OutContent = resp.EncInteger(int64(1))
		}
	}
	return nil
}

// HIncrBy increments the number stored at field in the hash stored at key by increment
func HIncrBy(ctx *CmdContext) error {
	key := []byte(ctx.Args[0])
	field := []byte(ctx.Args[1])
	value := ctx.Args[2]
	_, err := strconv.ParseInt(util.BytesToString(ctx.Args[2]), 10, 64)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
		return nil
	}

	res, err := ctx.CodecCtx.DB.Storage.HIncrBy(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, field, value)
	if err != nil {
		if strings.Contains(err.Error(), "-4262") {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
		} else {
			ctx.OutContent = resp.EncError("ERR hash value is not an integer")
		}
	} else {
		ctx.OutContent = resp.EncInteger(res)
	}
	return nil
}

// HIncrByFloat increment the specified field of a hash stored at key,
// and representing a floating point number, by the specified increment
func HIncrByFloat(ctx *CmdContext) error {
	key := ctx.Args[0]
	field := ctx.Args[1]
	value := ctx.Args[2]

	_, err := strconv.ParseFloat(util.BytesToString(ctx.Args[2]), 64)
	if err != nil {
		ctx.OutContent = resp.ResponseFloatErr
		return nil
	}

	f64, err := ctx.CodecCtx.DB.Storage.HIncrByFloat(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, field, value)
	if err != nil {
		if strings.Contains(err.Error(), "-4262") {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
		} else {
			ctx.OutContent = resp.EncError("ERR hash value is not a float")
		}
	} else {
		ctx.OutContent = resp.EncBulkString(strconv.FormatFloat(f64, 'f', -1, 64))
	}
	return nil
}

// HKeys returns all field names in the hash stored at key
func HKeys(ctx *CmdContext) error {
	key := ctx.Args[0]
	resValue, err := ctx.CodecCtx.DB.Storage.HKeys(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncArray(resValue)
	}
	return nil
}

// HVals returns all values in the hash stored at key
func HVals(ctx *CmdContext) error {
	key := ctx.Args[0]
	resValue, err := ctx.CodecCtx.DB.Storage.HVals(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncArray(resValue)
	}
	return nil
}

// HLen returns the number of fields contained in the hash stored at key
func HLen(ctx *CmdContext) error {
	key := []byte(ctx.Args[0])
	size, err := ctx.CodecCtx.DB.Storage.HLen(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(size)
	}
	return nil
}

// HMGet returns the values associated with the specified fields in the hash stored at key
func HMGet(ctx *CmdContext) error {
	key := ctx.Args[0]
	kvs := ctx.Args[1:]
	fields := make([][]byte, len(kvs))
	copy(fields, kvs)

	values, err := ctx.CodecCtx.DB.Storage.HMGet(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, fields)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncArray(values)
	}
	return nil
}

// compat server
func HashCmdWithKey(ctx *CmdContext) error {
	key := ctx.Args[0]
	var err error
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, ctx.CodecCtx.DB.ID),
		table.NewColumn(keyColumnName, key),
	}
	ctx.OutContent, err = ctx.CodecCtx.DB.Storage.ObServerCmd(ctx.CodecCtx.DB.Ctx, hashTableName, rowKey, ctx.PlainReq)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	}
	return nil
}
