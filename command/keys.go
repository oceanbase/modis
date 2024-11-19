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
	"github.com/oceanbase/obkv-table-client-go/table"

	"github.com/oceanbase/modis/protocol/resp"
)

func ExpireCommon(ctx *CmdContext) error {
	var err error
	ctx.OutContent, err = GenericCmdWithKey(ctx, "get")
	if err != nil {
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	}
	return nil
}

// compat server
func GenericCmdWithKey(ctx *CmdContext, cmdName string) (string, error) {
	key := ctx.Args[0]
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, ctx.CodecCtx.DB.ID),
		table.NewColumn(keyColumnName, key),
	}
	return ctx.CodecCtx.DB.Storage.ObServerCmd(ctx.CodecCtx.DB.Ctx, cmdName, rowKey, ctx.PlainReq)
}
