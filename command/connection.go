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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/oceanbase/modis/connection/conncontext"
	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/modis/protocol/resp"
	"github.com/oceanbase/modis/storage"
	"github.com/oceanbase/modis/util"
)

// Auth verifies the client
func Auth(ctx *CmdContext) error {
	if ctx.ServCtx.Password == "" {
		replaceWithRedacted(ctx.Args[0])
		ctx.OutContent =
			resp.EncError("ERR AUTH <password> called without any password configured. Are you sure your configuration is correct?")
		return nil
	}

	password := util.BytesToString(ctx.Args[0])
	if password != ctx.ServCtx.Password {
		ctx.OutContent =
			resp.EncError("WRONGPASS invalid username-password pair or user is disabled.")
	} else {
		ctx.OutContent = resp.ResponsesOk
		ctx.CodecCtx.Authenticated = true
	}
	replaceWithRedacted(ctx.Args[0])
	return nil
}

// Echo the given string
func Echo(ctx *CmdContext) error {
	ctx.OutContent = resp.EncBulkString(util.BytesToString(ctx.Args[0]))
	return nil
}

// Ping the server
func Ping(ctx *CmdContext) error {
	argc := len(ctx.Args)
	if argc > 2 {
		ctx.OutContent = resp.ErrWrongArgs(ctx.FullName)
	} else if argc > 0 {
		ctx.OutContent = resp.EncBulkString(util.BytesToString(ctx.Args[0]))
	} else {
		ctx.OutContent = resp.ResponsesPong
	}
	return nil
}

// Select the logical database
func Select(ctx *CmdContext) error {
	args := ctx.Args
	idxTmp, err := strconv.Atoi(util.BytesToString(args[0]))
	if err != nil {
		ctx.OutContent = resp.ErrOutRange(0, int64(ctx.ServCtx.DbNum))
		return nil
	}
	idx := int64(idxTmp)
	if idx < 0 || idx >= ctx.ServCtx.DbNum {
		ctx.OutContent = resp.EncError("ERR DB index is out of range")
		return nil
	}

	var db *storage.DB
	db, err = ctx.ServCtx.GetDB(idx)
	if err != nil {
		ctx.OutContent = resp.EncError("ERR fetch db failed")
		return nil
	}
	ctx.CodecCtx.DB = db
	ctx.OutContent = resp.ResponsesOk
	return nil
}

// Quit asks the server to close the connection
func Quit(ctx *CmdContext) error {
	close(ctx.CodecCtx.CloseChan)
	ctx.OutContent = resp.ResponsesOk
	return nil
}

// SwapDB swaps two modis databases
func SwapDB(ctx *CmdContext) error {
	args := ctx.Args
	idx1, err := strconv.Atoi(util.BytesToString(args[0]))
	if err != nil {
		ctx.OutContent = resp.EncError("ERR invalid first DB index")
		return nil
	}
	idx2, err := strconv.Atoi(util.BytesToString(args[1]))
	if err != nil {
		ctx.OutContent = resp.EncError("invalid second DB index")
		return nil
	}

	if idx1 < 0 || int64(idx1) >= ctx.ServCtx.DbNum ||
		idx2 < 0 || int64(idx2) >= ctx.ServCtx.DbNum {
		ctx.OutContent = resp.EncError("ERR invalid DB index")
		return nil
	}

	var db1, db2 *storage.DB
	db1, err = ctx.ServCtx.GetDB(int64(idx1))
	if err != nil {
		ctx.OutContent = resp.EncError("ERR fetch db failed")
		return nil
	}
	db2, err = ctx.ServCtx.GetDB(int64(idx2))
	if err != nil {
		ctx.OutContent = resp.EncError("ERR fetch db failed")
		return nil
	}
	tmpDB := *db1
	*db1 = *db2
	*db2 = tmpDB
	ctx.OutContent = resp.ResponsesOk
	return nil
}

func ClientHelp(ctx *CmdContext) error {
	out := [][]byte{
		[]byte("INFO"),
		[]byte("    Return information about the current client connection."),
		[]byte("LIST"),
		[]byte("    Return information about client connections."),
		[]byte("HELP"),
		[]byte("    Print this help."),
	}
	ctx.OutContent = resp.EncArray(out)
	return nil
}

func getClientInfo(infoBuilder *strings.Builder, cliCtx *conncontext.CodecContext) error {
	unixTime := time.Now().Unix()
	flag := clientFlag2Str(cliCtx.Flag)
	queNum := cliCtx.QueNum.Load()
	_, err := infoBuilder.WriteString(fmt.Sprintf(
		"id=%d addr=%s laddr=%s fd=%d name=%s age=%d idle=%d flags=%s db=%d sub=%d psub=%d "+
			"ssub=%d multi=%d qbuf=%d qbuf-free=%d argv-mem=%d multi-mem=%d rbs=%d rbp=%d obl=%d "+
			"oll=%d omem=%d tot-mem=%d events=%s cmd=%s user=%s redir=%d resp=%d lib-name=%s lib-ver=%s\r\n",
		cliCtx.ID,
		cliCtx.Conn.RemoteAddr().String(),
		cliCtx.Conn.LocalAddr().String(),
		cliCtx.Fd,
		cliCtx.Name,
		unixTime-cliCtx.StartTime.Unix(),
		unixTime-cliCtx.LastCmdTime.Unix(),
		flag,
		cliCtx.DB.ID,
		0,
		0,
		0,
		-1,
		queNum,
		cliCtx.QueLimit-queNum,
		cliCtx.LastArgvLen,
		0,
		0,  // TODO: rbs
		0,  // TODO: rbp
		0,  // TODO: obl
		0,  // TODO: oll
		0,  // TODO: omem
		0,  // TODO: tot-mem
		"", // TODO: events
		cliCtx.LastCmd,
		"(superuser)",
		-1,
		cliCtx.RespVer,
		"",
		"",
	))
	return err
}

func ClientInfo(ctx *CmdContext) error {
	var infoBuilder strings.Builder
	err := getClientInfo(&infoBuilder, ctx.CodecCtx)
	if err != nil {
		log.Warn("command", ctx.TraceID, "fail to get client info", log.Errors(err))
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	} else {
		ctx.OutContent = resp.EncBulkString(infoBuilder.String())
	}
	return nil
}

func ClientList(ctx *CmdContext) error {
	argc := len(ctx.Args)
	args := ctx.Args[1:] // without ctx.Args[0] = list
	var cliType conncontext.ClientType
	if argc == 3 && strings.EqualFold(util.BytesToString(args[0]), "type") {
		cliType = conncontext.GetClientTypeByName(util.BytesToString(args[1]))
		if cliType == conncontext.ClientTypeMax {
			ctx.OutContent = resp.EncError("Unknown client type: " + util.BytesToString(args[1]))
		}
	} else if argc > 2 && strings.EqualFold(util.BytesToString(args[0]), "id") {
		var infoBuilder strings.Builder
		var err error
		var id int
		for _, arg := range args[1:] {
			id, err = strconv.Atoi(util.BytesToString(arg))
			if err != nil {
				ctx.OutContent = resp.EncError("Invalid client id: " + util.BytesToString(arg))
				return nil
			}
			if cliCtx, ok := ctx.ServCtx.Clients.Get(int64(id)); ok {
				err = getClientInfo(&infoBuilder, cliCtx)
				if err != nil {
					log.Warn("command", ctx.TraceID, "fail to get client info", log.Errors(err))
					break
				}
			}
		}

		if err != nil {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
		} else {
			ctx.OutContent = resp.EncBulkString(infoBuilder.String())
		}
	} else if argc != 1 {
		ctx.OutContent = resp.ResponseSyntaxErr
	} else {
		var infoBuilder strings.Builder
		var err error
		ctx.ServCtx.Clients.ForEach(func(id int64, cliCtx *conncontext.CodecContext) bool {
			// return `true` to continue iteration and `false` to break iteration
			err = getClientInfo(&infoBuilder, cliCtx)
			if err != nil {
				log.Warn("command", ctx.TraceID, "fail to get client info", log.Errors(err))
				return false
			}
			return true
		})

		if err != nil {
			ctx.OutContent = resp.EncError("ERR " + err.Error())
		} else {
			ctx.OutContent = resp.EncBulkString(infoBuilder.String())
		}
	}

	return nil
}
