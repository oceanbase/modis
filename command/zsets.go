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
	"math"
	"strconv"
	"strings"

	"github.com/oceanbase/modis/protocol/resp"
	"github.com/oceanbase/obkv-table-client-go/table"
	"github.com/oceanbase/obkv-table-client-go/util"
)

func ZSetCmdWithKey(ctx *CmdContext) error {
	key := ctx.Args[0]
	var err error
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, ctx.CodecCtx.DB.ID),
		table.NewColumn(keyColumnName, key),
	}
	ctx.OutContent, err = ctx.CodecCtx.DB.Storage.ObServerCmd(ctx.CodecCtx.DB.Ctx, ctx.FullName, rowKey, ctx.PlainReq)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	}
	return nil
}

func ZIncrBy(ctx *CmdContext) error {
	var err error
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, ctx.CodecCtx.DB.ID),
		table.NewColumn(keyColumnName, ctx.Args[0]),
		table.NewColumn(memberColumnName, ctx.Args[2]),
	}
	ctx.OutContent, err = ctx.CodecCtx.DB.Storage.ObServerCmd(ctx.CodecCtx.DB.Ctx, ctx.FullName, rowKey, ctx.PlainReq)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	}
	return nil
}

func ZSetCmdWithKeyMember(ctx *CmdContext) error {
	var err error
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, ctx.CodecCtx.DB.ID),
		table.NewColumn(keyColumnName, ctx.Args[0]),
		table.NewColumn(memberColumnName, ctx.Args[1]),
	}
	ctx.OutContent, err = ctx.CodecCtx.DB.Storage.ObServerCmd(ctx.CodecCtx.DB.Ctx, ctx.FullName, rowKey, ctx.PlainReq)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	}
	return nil
}

func ZRangeByScore(ctx *CmdContext) error {
	var err error
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, ctx.CodecCtx.DB.ID),
		table.NewColumn(keyColumnName, ctx.Args[0]),
		table.NewColumn(memberColumnName, ctx.Args[1]),
	}
	is_count_less_zero := false
	if len(ctx.Args) >= 4 {
		idx := 3
		for idx < len(ctx.Args) {
			option := util.BytesToString(ctx.Args[idx])
			idx++
			if strings.EqualFold(option, "withscores") {
			} else if strings.EqualFold(option, "limit") {
				if (len(ctx.Args) - idx) < 2 {
					ctx.OutContent = resp.ResponseSyntaxErr
					return nil
				} else {
					offset, err := strconv.Atoi(util.BytesToString(ctx.Args[idx]))
					idx++
					if err != nil {
						ctx.OutContent = resp.ResponseIntegerErr
						return nil
					}
					if offset < 0 {
						var empty_arr [][]byte
						ctx.OutContent = resp.EncArray(empty_arr)
						return nil
					}
					count, err := strconv.Atoi(util.BytesToString(ctx.Args[idx]))
					if err != nil {
						ctx.OutContent = resp.ResponseIntegerErr
						return nil
					}
					if count == 0 {
						var empty_arr [][]byte
						ctx.OutContent = resp.EncArray(empty_arr)
						return nil
					}
					if count < 0 {
						is_count_less_zero = true
						count = math.MaxInt32
						count_str := strconv.Itoa(count)
						ctx.Args[idx] = []byte(count_str)
					}
					idx++
				}
			} else {
				ctx.OutContent = resp.ResponseSyntaxErr
				return nil
			}
		}

	}
	if is_count_less_zero {
		var new_args [][]byte
		new_args = append(new_args, []byte(ctx.FullName))
		new_args = append(new_args, ctx.Args...)
		ctx.PlainReq = util.StringToBytes(resp.EncArray(new_args))
	}
	ctx.OutContent, err = ctx.CodecCtx.DB.Storage.ObServerCmd(ctx.CodecCtx.DB.Ctx, ctx.FullName, rowKey, ctx.PlainReq)

	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	}
	return nil
}
