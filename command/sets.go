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
	"github.com/oceanbase/obkv-table-client-go/util"

	"github.com/oceanbase/modis/protocol/resp"
)

// SMembers returns all the members of the set value stored at key
func SMembers(ctx *CmdContext) error {
	key := ctx.Args[0]

	values, err := ctx.CodecCtx.DB.Storage.SMembers(ctx.CodecCtx.DB.Ctx, ctx.FullName, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncArray(values)
	}
	return nil
}

// SRandMember returns random members of the set
func SRandMember(ctx *CmdContext) error {
	key := ctx.Args[0]
	count := 1
	var err error
	if len(ctx.Args) > 2 {
		ctx.OutContent = resp.ResponseSyntaxErr
		return nil
	} else if len(ctx.Args) == 2 {
		count, err = strconv.Atoi(util.BytesToString(ctx.Args[1]))
		if err != nil {
			ctx.OutContent = resp.ResponseIntegerErr
			return nil
		}
	}
	// note: count < 0 is not support in this ver
	if count < 0 {
		ctx.OutContent = resp.ResponseIntegerErr
		return nil
	}

	members, err := ctx.CodecCtx.DB.Storage.SRandMember(ctx.CodecCtx.DB.Ctx, ctx.FullName, ctx.CodecCtx.DB.ID, key, count)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else if len(ctx.Args) == 1 {
		// return bulk string
		if len(members) == 0 {
			ctx.OutContent = resp.EncNullBulkString()
		} else {
			ctx.OutContent = resp.EncBulkString(util.BytesToString(members[0]))
		}
	} else {
		// return array
		ctx.OutContent = resp.EncArray(members)
	}
	return nil
}

// SCard returns the set cardinality (number of elements) of the set stored at key
func SCard(ctx *CmdContext) error {
	key := ctx.Args[0]
	size, err := ctx.CodecCtx.DB.Storage.SCard(ctx.CodecCtx.DB.Ctx, ctx.FullName, ctx.CodecCtx.DB.ID, key)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(size)
	}
	return nil
}

// SIsmember returns if member is a member of the set stored at key
func SIsmember(ctx *CmdContext) error {
	key := ctx.Args[0]
	member := ctx.Args[1]
	returnValue, err := ctx.CodecCtx.DB.Storage.SIsmember(ctx.CodecCtx.DB.Ctx, ctx.FullName, ctx.CodecCtx.DB.ID, key, member)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(returnValue))
	}
	return nil
}

// SPop removes and returns one or more random elements from the set value storage at key
func SPop(ctx *CmdContext) error {
	if len(ctx.Args) > 2 {
		ctx.OutContent = resp.ResponseSyntaxErr
		return nil
	}

	key := ctx.Args[0]
	count := 1
	var err error
	if len(ctx.Args) == 2 {
		count, err = strconv.Atoi(util.BytesToString(ctx.Args[1]))
		if err != nil {
			ctx.OutContent = resp.ResponseIntegerErr
			return nil
		}
	}

	members, err := ctx.CodecCtx.DB.Storage.SPop(ctx.CodecCtx.DB.Ctx, ctx.FullName, ctx.CodecCtx.DB.ID, key, count)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else if len(ctx.Args) == 1 {
		// return bulk string
		if len(members) == 0 {
			ctx.OutContent = resp.EncNullBulkString()
		} else {
			ctx.OutContent = resp.EncBulkString(util.BytesToString(members[0]))
		}
	} else {
		// return array
		ctx.OutContent = resp.EncArray(members)
	}
	return nil
}

// SRem removes the specified members from the set stored at key
func SRem(ctx *CmdContext) error {
	var members [][]byte
	key := []byte(ctx.Args[0])
	for _, member := range ctx.Args[1:] {
		members = append(members, []byte(member))
	}
	returnValue, err := ctx.CodecCtx.DB.Storage.SRem(ctx.CodecCtx.DB.Ctx, ctx.FullName, ctx.CodecCtx.DB.ID, key, members)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(returnValue)
	}
	return nil
}

// SMove movies member from the set at source to the set at destination
func SMove(ctx *CmdContext) error {
	srcKey := ctx.Args[0]
	dstKey := ctx.Args[1]
	member := ctx.Args[2]

	res, err := ctx.CodecCtx.DB.Storage.Smove(ctx.CodecCtx.DB.Ctx, ctx.FullName, ctx.CodecCtx.DB.ID, srcKey, dstKey, member)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(res))
	}
	return nil
}

func SetCmdWithKey(ctx *CmdContext) error {
	key := ctx.Args[0]
	var err error
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, ctx.CodecCtx.DB.ID),
		table.NewColumn(vitualKeyColumnName, key),
	}
	ctx.OutContent, err = ctx.CodecCtx.DB.Storage.ObServerCmd(ctx.CodecCtx.DB.Ctx, ctx.FullName, rowKey, ctx.PlainReq)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	}
	return nil
}
