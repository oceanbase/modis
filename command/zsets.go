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

	"github.com/oceanbase/modis/protocol/resp"
	"github.com/oceanbase/modis/util"
)

// ZAdd adds the specified members with scores to the sorted set
func ZAdd(ctx *CmdContext) error {
	key := ctx.Args[0]
	var err error
	ctx.OutContent, err = ctx.CodecCtx.DB.Storage.ObServerCmd(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, ctx.PlainReq)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
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
