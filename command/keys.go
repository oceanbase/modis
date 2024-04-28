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

package command

import (
	"math"
	"strconv"
	"time"

	"github.com/oceanbase/obkv-table-client-go/util"

	"github.com/oceanbase/modis/protocol/resp"
)

// Delete removes the specified keys. A key is ignored if it does not exist
func Delete(ctx *CmdContext) error {
	keys := make([][]byte, len(ctx.Args))
	copy(keys, ctx.Args)

	delNum, err := ctx.CodecCtx.DB.Storage.Delete(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, keys)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(delNum)
	}
	return nil
}

// Exists returns if key exists
func Exists(ctx *CmdContext) error {
	keys := make([][]byte, len(ctx.Args))
	copy(keys, ctx.Args)
	val, err := ctx.CodecCtx.DB.Storage.Exists(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, keys)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(val)
	}
	return nil
}

// Expire sets a timeout on key
func Expire(ctx *CmdContext) error {
	key := ctx.Args[0]
	seconds, err := strconv.ParseInt(util.BytesToString(ctx.Args[1]), 10, 64)
	if err != nil {
		ctx.OutContent = resp.ResponseIntegerErr
		return nil
	}

	at := time.Now().Add(time.Second * time.Duration(seconds))
	res, err := ctx.CodecCtx.DB.Storage.Expire(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, at)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(res))
	}
	return nil
}

// ExpireAt sets an absolute timestamp to expire on key
func ExpireAt(ctx *CmdContext) error {
	key := ctx.Args[0]
	timestamp, err := strconv.ParseInt(util.BytesToString(ctx.Args[1]), 10, 64)
	if err != nil {
		ctx.OutContent = resp.ResponseIntegerErr
		return nil
	}

	at := time.Unix(timestamp, 0)
	res, err := ctx.CodecCtx.DB.Storage.Expire(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, at)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(res))
	}
	return nil
}

// Persist removes the existing timeout on key, turning the key from volatile to persistent
func Persist(ctx *CmdContext) error {
	key := ctx.Args[0]

	res, err := ctx.CodecCtx.DB.Storage.Persist(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(res))
	}
	return nil
}

// PExpire works exactly like expire but the time to live of the key is specified in milliseconds instead of seconds
func PExpire(ctx *CmdContext) error {
	key := ctx.Args[0]
	ms, err := strconv.ParseInt(util.BytesToString(ctx.Args[1]), 10, 64)
	if err != nil {
		ctx.OutContent = resp.ResponseIntegerErr
		return nil
	}

	at := time.Now().Add(time.Millisecond * time.Duration(ms))
	res, err := ctx.CodecCtx.DB.Storage.Expire(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, at)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(res))
	}
	return nil
}

// PExpireAt has the same effect and semantic as expireAt,
// but the Unix time at which the key will expire is specified in milliseconds instead of seconds
func PExpireAt(ctx *CmdContext) error {
	key := ctx.Args[0]
	ms, err := strconv.ParseInt(util.BytesToString(ctx.Args[1]), 10, 64)
	if err != nil {
		ctx.OutContent = resp.ResponseIntegerErr
		return nil
	}

	nanoseconds := ms * int64(time.Millisecond)
	at := time.Unix(0, nanoseconds)
	res, err := ctx.CodecCtx.DB.Storage.Expire(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, at)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(res))
	}
	return nil
}

// TTL returns the remaining time to live of a key that has a timeout
func TTL(ctx *CmdContext) error {
	key := ctx.Args[0]

	res, err := ctx.CodecCtx.DB.Storage.TTL(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else if res < 0 {
		ctx.OutContent = resp.EncInteger(int64(res))
	} else {
		ctx.OutContent = resp.EncInteger(int64(math.Ceil(res.Seconds())))
	}
	return nil
}

// PTTL likes TTL this command returns the remaining time to live of a key that has an expire set,
// with the sole difference that TTL returns the amount of remaining time in seconds while PTTL returns it in milliseconds
func PTTL(ctx *CmdContext) error {
	key := ctx.Args[0]

	res, err := ctx.CodecCtx.DB.Storage.TTL(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else if res < 0 {
		ctx.OutContent = resp.EncInteger(int64(res))
	} else {
		ctx.OutContent = resp.EncInteger(res.Milliseconds())
	}
	return nil
}

// Type returns the string representation of the type of the value stored at key
func Type(ctx *CmdContext) error {
	key := ctx.Args[0]
	val, err := ctx.CodecCtx.DB.Storage.Type(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncBulkString(val)
	}
	return nil
}
