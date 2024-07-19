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

	"github.com/oceanbase/obkv-table-client-go/table"

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
	var err error
	var res int
	for _, tbName := range tbNames {
		var str string
		str, err = GenericCmdWithKey(ctx, tbName)
		if err != nil {
			break
		}
		var retInt int
		retInt, err = strconv.Atoi(str)
		if err != nil {
			break
		}
		res += retInt
	}
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(res))
	}
	return nil
}

func ExpireCommon(ctx *CmdContext) error {
	var err error
	ctx.OutContent, err = GenericCmdWithKey(ctx, stringTableName)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	}
	return nil
}

func TTL(ctx *CmdContext) error {
	var err error
	var ttl int64 = -2
	for _, tbName := range tbNames {
		var str string
		str, err = GenericCmdWithKey(ctx, tbName)
		if err != nil {
			break
		}
		if str == "-2" {
			// do nothing
		} else if str == "-1" {
			ttl = -1
		} else {
			var ret_num int
			ret_num, err = strconv.Atoi(str)
			if err != nil {
				break
			}
			ttl = int64(ret_num)
			// all key ttl is same
			break
		}
	}
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(ttl)
	}
	return nil
}

// Type returns the string representation of the type of the value stored at key
func Type(ctx *CmdContext) error {
	var err error
	var res string
	for i, tbName := range tbNames {
		var str string
		str, err = GenericCmdWithKey(ctx, tbName)
		if err != nil {
			break
		}
		if str == "1" {
			if len(res) > 0 {
				res += ", "
			}
			res += models[i]
		}
	}
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else if len(res) == 0 {
		ctx.OutContent = resp.EncBulkString("none")
	} else {
		ctx.OutContent = resp.EncBulkString(res)
	}
	return err
}

// compat server
func GenericCmdWithKey(ctx *CmdContext, tableName string) (string, error) {
	key := ctx.Args[0]
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, ctx.CodecCtx.DB.ID),
		table.NewColumn(keyColumnName, key),
	}
	return ctx.CodecCtx.DB.Storage.ObServerCmd(ctx.CodecCtx.DB.Ctx, tableName, rowKey, ctx.PlainReq)
}
