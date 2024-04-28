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

package command

import (
	"strconv"

	"github.com/oceanbase/obkv-table-client-go/util"

	"github.com/oceanbase/modis/protocol/resp"
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

// HSet sets field in the hash stored at key to value
func HSet(ctx *CmdContext) error {
	key := ctx.Args[0]
	kvs := ctx.Args[1:]
	if len(kvs)%2 != 0 {
		ctx.OutContent = resp.EncError("ERR wrong number of arguments for HSET")
	} else {
		setValues := make(map[string][]byte, len(kvs)/2)
		for i := 2; i <= len(kvs); i += 2 {
			kv := kvs[i-2 : i]
			setValues[util.BytesToString(kv[0])] = kv[1]
		}

		size, err := ctx.CodecCtx.DB.Storage.HSet(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, setValues)
		if err != nil {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
		} else {
			ctx.OutContent = resp.EncInteger(int64(size))
		}
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
		ctx.OutContent = resp.EncError("ERR " + err.Error())
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
		ctx.OutContent = resp.EncError("ERR " + err.Error())
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

// HMSet sets the specified fields to their respective values in the hash stored at key
func HMSet(ctx *CmdContext) error {
	key := ctx.Args[0]
	kvs := ctx.Args[1:]
	if len(kvs)%2 != 0 {
		ctx.OutContent = resp.EncError("ERR wrong number of arguments for HMSET")
	} else {
		setValues := make(map[string][]byte, len(kvs)/2)
		for i := 2; i <= len(kvs); i += 2 {
			kv := kvs[i-2 : i]
			setValues[util.BytesToString(kv[0])] = kv[1]
		}

		_, err := ctx.CodecCtx.DB.Storage.HSet(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, setValues)
		if err != nil {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
		} else {
			ctx.OutContent = resp.ResponsesOk
		}
	}
	return nil
}
