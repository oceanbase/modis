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

	"github.com/oceanbase/modis/protocol/resp"
	"github.com/oceanbase/modis/storage"
	"github.com/oceanbase/modis/util"
)

// Auth verifies the client
func Auth(ctx *CmdContext) error {
	if ctx.ServCtx.Password == "" {
		ctx.OutContent =
			resp.EncError("ERR AUTH <password> called without any password configured. Are you sure your configuration is correct?")
		return nil
	}

	password := util.BytesToString(ctx.Args[0])
	if password != ctx.ServCtx.Password {
		ctx.OutContent =
			resp.EncError("WRONGPASS invalid password.")
		return nil
	}

	ctx.OutContent = resp.ResponsesOk
	ctx.CodecCtx.Authenticated = true

	return nil
}

// Echo the given string
func Echo(ctx *CmdContext) error {
	ctx.OutContent = resp.EncBulkString(util.BytesToString(ctx.Args[0]))
	return nil
}

// Ping the server
func Ping(ctx *CmdContext) error {
	args := ctx.Args
	if len(args) > 0 {
		ctx.OutContent = resp.EncBulkString(util.BytesToString(args[0]))
	} else {
		ctx.OutContent = resp.ResponsesPong
	}
	return nil
}

// Select the logical database
func Select(ctx *CmdContext) error {
	args := ctx.Args
	idx, err := strconv.Atoi(util.BytesToString(args[0]))
	if err != nil {
		ctx.OutContent = resp.EncError("ERR invalid DB index")
		return nil
	}
	if idx < 0 {
		ctx.OutContent = resp.EncError("ERR invalid DB index")
		return nil
	}
	namespace := ctx.CodecCtx.DB.Namespace
	ctx.CodecCtx.DB = storage.NewDB(namespace, int64(uint64(idx)), ctx.ServCtx.Storage)
	ctx.OutContent = resp.ResponsesOk
	return nil
}

// Quit asks the server to close the connection
func Quit(ctx *CmdContext) error {
	// TODO: implement it
	// close(ctx.CodecCtx.Done)
	ctx.OutContent = resp.ResponsesOk
	return nil
}

// SwapDB swaps two Redis databases
func SwapDB(ctx *CmdContext) error {
	ctx.OutContent = resp.EncError("ERR not supported")
	return nil
}
