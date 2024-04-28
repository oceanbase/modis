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
	"strings"

	"github.com/oceanbase/modis/protocol/resp"
	"github.com/oceanbase/modis/util"
)

// ZAdd adds the specified members with scores to the sorted set
func ZAdd(ctx *CmdContext) error {
	key := []byte(ctx.Args[0])

	kvs := ctx.Args[1:]
	if len(kvs)%2 != 0 {
		ctx.OutContent = resp.EncError("ERR syntax error")
		return nil
	}

	uniqueMembers := make(map[string]bool)

	memberScore := make(map[string]int64)

	// 倒序，因为后面填的参数会覆盖前面
	for i := len(kvs) - 1; i >= 0; i -= 2 {
		member := kvs[i]
		if _, ok := uniqueMembers[util.BytesToString(member)]; ok {
			continue
		}

		score, err := strconv.ParseInt(util.BytesToString(kvs[i-1]), 10, 64)
		if err != nil {
			ctx.OutContent = resp.EncError("ERR syntax error")
			return nil
		}

		memberScore[util.BytesToString(member)] = score
		uniqueMembers[util.BytesToString(member)] = true
	}
	returnValue, err := ctx.CodecCtx.DB.Storage.ZAdd(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, memberScore)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(returnValue))
	}
	return nil
}

func ZRange(ctx *CmdContext) error {
	key := []byte(ctx.Args[0])
	start, err := strconv.ParseInt(util.BytesToString(ctx.Args[1]), 10, 64)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR syntax error")
		return nil
	}
	stop, err := strconv.ParseInt(util.BytesToString(ctx.Args[2]), 10, 64)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR syntax error")
		return nil
	}
	withScore := false
	if len(ctx.Args) >= 4 {
		if strings.ToUpper(util.BytesToString(ctx.Args[3])) == "WITHSCORES" {
			withScore = true
		}
	}

	returnValue, err := ctx.CodecCtx.DB.Storage.ZRange(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, start, stop, withScore)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncArray(returnValue)
	}
	return nil
}

func ZRem(ctx *CmdContext) error {
	key := []byte(ctx.Args[0])

	uniqueMembers := make(map[string]bool)
	members := make([][]byte, 0, len(ctx.Args)-1)
	for _, member := range ctx.Args[1:] {
		if _, ok := uniqueMembers[util.BytesToString(member)]; ok {
			continue
		}

		members = append(members, member)
		uniqueMembers[util.BytesToString(member)] = true
	}

	returnValue, err := ctx.CodecCtx.DB.Storage.ZRem(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, members)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(returnValue))
	}
	return nil
}

func ZCard(ctx *CmdContext) error {
	key := []byte(ctx.Args[0])
	returnValue, err := ctx.CodecCtx.DB.Storage.ZCard(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(returnValue))
	}
	return nil
}
