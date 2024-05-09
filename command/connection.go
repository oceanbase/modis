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
