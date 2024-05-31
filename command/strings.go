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
	"time"

	"github.com/oceanbase/obkv-table-client-go/util"

	"github.com/oceanbase/modis/protocol/resp"
)

// Get the value of key
func Get(ctx *CmdContext) error {
	key := ctx.Args[0]
	val, err := ctx.CodecCtx.DB.Storage.Get(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else if val == nil {
		ctx.OutContent = resp.EncNullBulkString()
	} else {
		ctx.OutContent = resp.EncBulkString(string(val))
	}
	return nil
}

// Set key to hold the string value
func Set(ctx *CmdContext) error {
	key := ctx.Args[0]
	value := ctx.Args[1]

	err := ctx.CodecCtx.DB.Storage.Set(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, value)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.ResponsesOk
	}
	return nil
}

// MGet returns the values of all specified key
func MGet(ctx *CmdContext) error {
	count := len(ctx.Args)
	keys := make([][]byte, count)
	copy(keys, ctx.Args)

	resValues, err := ctx.CodecCtx.DB.Storage.MGet(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, keys)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncArray(resValues)
	}
	return nil
}

// MSet sets the given keys to their respective values
func MSet(ctx *CmdContext) error {
	argc := len(ctx.Args)
	args := ctx.Args
	if argc%2 != 0 {
		ctx.OutContent = resp.EncError("ERR INVALID ARGS NUMS")
	} else {
		setValues := make(map[string][]byte, argc/2)
		for i := 2; i <= argc; i += 2 {
			kv := args[i-2 : i]
			setValues[util.BytesToString(kv[0])] = kv[1]
		}

		_, err := ctx.CodecCtx.DB.Storage.MSet(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, setValues)
		if err != nil {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
		} else {
			ctx.OutContent = resp.ResponsesOk
		}
	}
	return nil
}

// Strlen returns the length of the string value stored at key
func Strlen(ctx *CmdContext) error {
	key := ctx.Args[0]
	val, err := ctx.CodecCtx.DB.Storage.Get(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(len(util.BytesToString(val))))
	}
	return nil
}

// Append a value to a key
func Append(ctx *CmdContext) error {
	key := ctx.Args[0]
	value := ctx.Args[1]
	length, err := ctx.CodecCtx.DB.Storage.Append(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, value)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(length))
	}
	return nil
}

// GetSet sets the string value of a key and return its old value
func GetSet(ctx *CmdContext) error {
	key := ctx.Args[0]
	value := ctx.Args[1]
	val, err := ctx.CodecCtx.DB.Storage.GetSet(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, value)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else if val == nil {
		ctx.OutContent = resp.EncNullBulkString()
	} else {
		ctx.OutContent = resp.EncBulkString(util.BytesToString(val))
	}
	return nil
}

// SetNx sets the value of a key ,only if the key does not exist
func SetNx(ctx *CmdContext) error {
	key := ctx.Args[0]
	value := ctx.Args[1]
	resValue, err := ctx.CodecCtx.DB.Storage.SetNx(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, value)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(resValue))
	}
	return nil
}

// SetEx sets the value and expiration of a key KEY_NAME TIMEOUT VALUE
func SetEx(ctx *CmdContext) error {
	key := ctx.Args[0]
	value := ctx.Args[2]

	ui, err := strconv.ParseUint(util.BytesToString(ctx.Args[1]), 10, 64)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
		return nil
	}
	if ui <= 0 {
		ctx.OutContent = resp.ErrInvalidExpire(ctx.FullName)
		return nil
	}
	expireTimes := ui * uint64(time.Second)

	err = ctx.CodecCtx.DB.Storage.SetEx(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, expireTimes, value)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.ResponsesOk
	}
	return nil
}

// PSetEx sets the value and expiration in milliseconds of a key
func PSetEx(ctx *CmdContext) error {
	key := ctx.Args[0]
	value := ctx.Args[2]
	ui, err := strconv.ParseUint(util.BytesToString(ctx.Args[1]), 10, 64)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	}
	if ui <= 0 {
		ctx.OutContent = resp.ErrInvalidExpire(ctx.FullName)
		return nil
	}
	expireTimeMs := ui * uint64(time.Millisecond)

	err = ctx.CodecCtx.DB.Storage.PSetEx(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, expireTimeMs, value)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.ResponsesOk
	}
	return nil
}

// Incr increments the integer value of a key  by one
func Incr(ctx *CmdContext) error {
	key := []byte(ctx.Args[0])
	res, err := ctx.CodecCtx.DB.Storage.IncrBy(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, []byte("1"))
	if err != nil {
		ctx.OutContent = resp.ResponseIntegerErr
	} else {
		ctx.OutContent = resp.EncInteger(res)
	}
	return nil
}

// IncrBy increments the integer value of a key by the given amount
func IncrBy(ctx *CmdContext) error {
	key := ctx.Args[0]
	value := ctx.Args[1]

	_, err := strconv.ParseInt(util.BytesToString(ctx.Args[1]), 10, 64)
	if err != nil {
		ctx.OutContent = resp.ResponseIntegerErr
		return nil
	}

	res, err := ctx.CodecCtx.DB.Storage.IncrBy(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, value)
	if err != nil {
		ctx.OutContent = resp.ResponseIntegerErr
	} else {
		ctx.OutContent = resp.EncInteger(res)
	}
	return nil
}

// IncrByFloat increments the float value of a key by the given amount
func IncrByFloat(ctx *CmdContext) error {
	key := ctx.Args[0]
	value := ctx.Args[1]

	_, err := strconv.ParseFloat(util.BytesToString(ctx.Args[1]), 64)
	if err != nil {
		ctx.OutContent = resp.ResponseFloatErr
		return nil
	}

	f64, err := ctx.CodecCtx.DB.Storage.IncrByFloat(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, value)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncBulkString(strconv.FormatFloat(f64, 'f', -1, 64))
	}
	return nil
}

// Decr decrements the integer value of a key by one
func Decr(ctx *CmdContext) error {
	key := ctx.Args[0]
	res, err := ctx.CodecCtx.DB.Storage.IncrBy(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, []byte("-1"))
	if err != nil {
		ctx.OutContent = resp.ResponseIntegerErr
	} else {
		ctx.OutContent = resp.EncInteger(res)
	}
	return nil
}

// DecrBy decrements the integer value of a key by the given number
func DecrBy(ctx *CmdContext) error {
	key := ctx.Args[0]

	delta, err := strconv.ParseInt(util.BytesToString(ctx.Args[1]), 10, 64)
	if err != nil {
		ctx.OutContent = resp.ResponseIntegerErr
		return nil
	}

	res, err := ctx.CodecCtx.DB.Storage.IncrBy(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, []byte(strconv.FormatInt(-delta, 10)))
	if err != nil {
		ctx.OutContent = resp.ResponseIntegerErr
	} else {
		ctx.OutContent = resp.EncInteger(res)
	}
	return nil
}

// SetBit sets or clears the bit at offset in the string value stored at key.
func SetBit(ctx *CmdContext) error {
	key := ctx.Args[0]
	offset, err := strconv.Atoi(util.BytesToString(ctx.Args[1]))
	if err != nil || offset < 0 {
		ctx.OutContent = resp.ResponseBitOffsetErr
		return nil
	}

	on, err := strconv.Atoi(util.BytesToString(ctx.Args[2]))
	if err != nil || (on & ^1) != 0 {
		ctx.OutContent = resp.ResponseBitIntegerErr
		return nil
	}

	res, err := ctx.CodecCtx.DB.Storage.SetBit(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, offset, on)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(res))
	}
	return nil
}

// GetBit gets the bit at offset in the string value stored at key.
func GetBit(ctx *CmdContext) error {
	key := ctx.Args[0]
	offset, err := strconv.Atoi(util.BytesToString(ctx.Args[1]))
	if err != nil || offset < 0 {
		ctx.OutContent = resp.ResponseBitOffsetErr
		return nil
	}

	res, err := ctx.CodecCtx.DB.Storage.GetBit(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, offset)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(res))
	}
	return nil
}

// BitCount counts the number of set bits (population counting) in a string.
func BitCount(ctx *CmdContext) error {
	key := ctx.Args[0]
	val, err := ctx.CodecCtx.DB.Storage.Get(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else if val == nil {
		ctx.OutContent = resp.EncInteger(0)
	} else {
		var begin, end int
		switch len(ctx.Args) {
		case 3:
			begin, err = strconv.Atoi(util.BytesToString(ctx.Args[1]))
			if err != nil {
				ctx.OutContent = resp.ResponseIntegerErr
				return nil
			}
			end, err = strconv.Atoi(util.BytesToString(ctx.Args[2]))
			if err != nil {
				ctx.OutContent = resp.ResponseIntegerErr
				return nil
			}
		case 1:
			begin = 0
			end = len(val) - 1
		default:
			ctx.OutContent = resp.ResponseSyntaxErr
			return nil
		}

		count, err := bitCount(val, begin, end)
		if err != nil {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
		} else {
			ctx.OutContent = resp.EncInteger(int64(count))
		}
	}
	return nil
}

const (
	MaxRange = 2<<29 - 1 // 512M
)

// SetRange overwrites part of the string stored at key, starting at the specified offset, for the entire length of value.
func SetRange(ctx *CmdContext) error {
	offset, err := strconv.Atoi(util.BytesToString(ctx.Args[1]))
	if err != nil {
		ctx.OutContent = resp.ResponseIntegerErr
		return nil
	}

	key := []byte(ctx.Args[0])
	if offset < 0 || offset > MaxRange {
		ctx.OutContent = resp.ResponseMaximumErr
		return nil
	}

	// 1. get
	val, err := ctx.CodecCtx.DB.Storage.Get(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
		return nil
	}

	// 2. construct value
	resBytes := setRange(val, int64(offset), ctx.Args[2])
	if resBytes == nil {
		ctx.OutContent = resp.EncInteger(0)
		return nil
	}

	// 3. insert
	err = ctx.CodecCtx.DB.Storage.Set(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, resBytes)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(len(resBytes)))
	}
	return nil
}

// GetRange increments the integer value of a keys by the given amount
func GetRange(ctx *CmdContext) error {
	key := ctx.Args[0]

	val, err := ctx.CodecCtx.DB.Storage.Get(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
		return nil
	}

	start, err := strconv.Atoi(util.BytesToString(ctx.Args[1]))
	if err != nil {
		ctx.OutContent = resp.ResponseIntegerErr
		return nil
	}
	end, err := strconv.Atoi(util.BytesToString(ctx.Args[2]))
	if err != nil {
		ctx.OutContent = resp.ResponseIntegerErr
		return nil
	}

	if val == nil {
		ctx.OutContent = resp.EncBulkString("")
	} else {
		sub := getRange(val, start, end)
		ctx.OutContent = resp.EncBulkString(util.BytesToString(sub))
	}
	return nil
}
