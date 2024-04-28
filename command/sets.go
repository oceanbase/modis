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

// SAdd adds the specified members to the set stored at key
func SAdd(ctx *CmdContext) error {
	key := ctx.Args[0]
	members := make([][]byte, len(ctx.Args[1:]))
	for i, member := range ctx.Args[1:] {
		members[i] = member
	}

	addNum, err := ctx.CodecCtx.DB.Storage.SAdd(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, members)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(addNum)
	}
	return nil
}

// SMembers returns all the members of the set value stored at key
func SMembers(ctx *CmdContext) error {
	key := ctx.Args[0]

	values, err := ctx.CodecCtx.DB.Storage.SMembers(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
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
	if len(ctx.Args) == 2 {
		count, err = strconv.Atoi(util.BytesToString(ctx.Args[1]))
		if err != nil {
			ctx.OutContent = resp.ResponseIntegerErr
			return nil
		}
	}

	members, err := ctx.CodecCtx.DB.Storage.SRandMember(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, int64(count))
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncArray(members)
	}
	return nil
}

// SCard returns the set cardinality (number of elements) of the set stored at key
func SCard(ctx *CmdContext) error {
	key := ctx.Args[0]
	size, err := ctx.CodecCtx.DB.Storage.SCard(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
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
	returnValue, err := ctx.CodecCtx.DB.Storage.SIsmember(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, member)
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

	members, err := ctx.CodecCtx.DB.Storage.SPop(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, int64(count))
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
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
	returnValue, err := ctx.CodecCtx.DB.Storage.SRem(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key, members)
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

	res, err := ctx.CodecCtx.DB.Storage.Smove(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, srcKey, dstKey, member)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(res))
	}
	return nil
}

// SUnion returns the members of the set resulting from the union of all the given sets.
func SUnion(ctx *CmdContext) error {
	var allMembers [][][]byte
	for i := 0; i < len(ctx.Args); i++ {
		key := ctx.Args[i]
		members, err := ctx.CodecCtx.DB.Storage.SMembers(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
		if err != nil {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
			return nil
		}

		allMembers = append(allMembers, members)
	}
	ctx.OutContent = resp.EncArray(getUnion(allMembers...))

	return nil
}

// SUnionStore stores the members of the set resulting from the union of all the given sets.
func SUnionStore(ctx *CmdContext) error {
	dstKey := ctx.Args[0]

	// 1. Get members and do diff
	var allMembers [][][]byte
	for i := 1; i < len(ctx.Args); i++ {
		key := ctx.Args[i]
		members, err := ctx.CodecCtx.DB.Storage.SMembers(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
		if err != nil {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
			return nil
		}

		allMembers = append(allMembers, members)
	}

	unionMembers := getUnion(allMembers...)
	if len(unionMembers) == 0 {
		ctx.OutContent = resp.EncInteger(int64(0))
		return nil
	}

	// 2. Store dstKey unionMembers
	_, err := ctx.CodecCtx.DB.Storage.SAdd(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, dstKey, unionMembers)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(len(unionMembers)))
	}

	return nil
}

// SInter returns the members of the set resulting from the intersection of all the given sets.
func SInter(ctx *CmdContext) error {
	var allMembers [][][]byte
	for i := 0; i < len(ctx.Args); i++ {
		key := ctx.Args[i]
		members, err := ctx.CodecCtx.DB.Storage.SMembers(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
		if err != nil {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
			return nil
		}

		allMembers = append(allMembers, members)
	}
	ctx.OutContent = resp.EncArray(getIntersection(allMembers...))

	return nil
}

// SInterStore stores the members of the set resulting from the intersection of all the given sets.
func SInterStore(ctx *CmdContext) error {
	dstKey := ctx.Args[0]

	// 1. Get members and do diff
	var allMembers [][][]byte
	for i := 1; i < len(ctx.Args); i++ {
		key := ctx.Args[i]
		members, err := ctx.CodecCtx.DB.Storage.SMembers(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
		if err != nil {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
			return nil
		}

		allMembers = append(allMembers, members)
	}

	interMembers := getIntersection(allMembers...)
	if len(interMembers) == 0 {
		ctx.OutContent = resp.EncInteger(int64(0))
		return nil
	}

	// 2. Store dstKey interMembers
	_, err := ctx.CodecCtx.DB.Storage.SAdd(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, dstKey, interMembers)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(len(interMembers)))
	}

	return nil
}

// SDiff returns the members of the set resulting from the difference between the first set and all the successive sets.
func SDiff(ctx *CmdContext) error {
	// 1. Get first key members
	firstKey := ctx.Args[0]
	members, err := ctx.CodecCtx.DB.Storage.SMembers(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, firstKey)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
		return nil
	}

	// 2. Get other key members and cale exclusive members
	for i := 1; i < len(ctx.Args); i++ {
		key := ctx.Args[i]
		tmpMembers, err := ctx.CodecCtx.DB.Storage.SMembers(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
		if err != nil {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
			return nil
		}

		members = getExclusiveElements(members, tmpMembers)
	}

	// 3. Return result
	ctx.OutContent = resp.EncArray(members)
	return nil
}

// SDiffStore stores the difference set between the given sets in the specified set.
func SDiffStore(ctx *CmdContext) error {
	dstKey := ctx.Args[0]
	keys := ctx.Args[1:]

	// 1. Get members and do diff
	var allMembers [][][]byte
	for _, key := range keys {
		members, err := ctx.CodecCtx.DB.Storage.SMembers(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, key)
		if err != nil {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
			return nil
		}

		allMembers = append(allMembers, members)
	}
	diffMembers := getDifference(allMembers...)
	if len(diffMembers) == 0 {
		ctx.OutContent = resp.EncInteger(int64(0))
		return nil
	}

	// 2. Store dstKey diffMembers
	_, err := ctx.CodecCtx.DB.Storage.SAdd(ctx.CodecCtx.DB.Ctx, ctx.CodecCtx.DB.ID, dstKey, diffMembers)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncInteger(int64(len(diffMembers)))
	}

	return nil
}
