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
	"strings"
	"time"

	"github.com/oceanbase/modis/connection/conncontext"
	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/modis/protocol/resp"
	"github.com/oceanbase/modis/util"
)

func TempNotSupport(ctx *CmdContext) error {
	ctx.OutContent = resp.EncError("ERR not supported")
	return nil
}

func feedMonitors(ctx *CmdContext) {
	var infoBuilder strings.Builder
	var err error
	tm := float64(time.Now().UnixMicro()) / 1000000
	ctx.ServCtx.Monitors.ForEach(func(id int64, cliCtx *conncontext.CodecContext) bool {
		// return `true` to continue iteration and `false` to break iteration
		infoBuilder.Reset()
		_, err = infoBuilder.WriteString(fmt.Sprintf(
			"%.6f [%d %s] ",
			tm,
			cliCtx.DB.ID,
			cliCtx.Conn.RemoteAddr(),
		))
		if err != nil {
			log.Warn("command", nil, "write string to builder failed, can not send monitor info",
				log.Errors(err), log.Int64("client id", int64(id)))
			return true
		}
		_, err = infoBuilder.WriteString("\"" + ctx.Name + "\"")
		if err != nil {
			log.Warn("command", nil, "write string to builder failed, can not send monitor info",
				log.Errors(err), log.Int64("client id", int64(id)))
			return true
		}
		for _, arg := range ctx.Args {
			_, err = infoBuilder.WriteString(" \"" + util.BytesToString(arg) + "\"")
			if err != nil {
				log.Warn("command", nil, "write string to builder failed, can not send monitor info",
					log.Errors(err), log.Int64("client id", int64(id)))
				break
			}
		}
		if err != nil {
			return true
		}
		_, err := cliCtx.Conn.Write([]byte(resp.EncSimpleString(infoBuilder.String())))
		if err != nil {
			// send message failed, delete from map
			ctx.ServCtx.Monitors.Del(id)
		}
		return true
	})
}

// Call a command
func Call(ctx *CmdContext) {
	// check auth
	if ctx.FullName != "auth" &&
		ctx.ServCtx.Password != "" &&
		!ctx.CodecCtx.Authenticated {
		ctx.OutContent = resp.ResponsesNoautherr
		return
	}

	// check command info
	argc := len(ctx.Args) + 1 // include the command name
	for _, slc := range secondLevelCmd {
		if ctx.FullName == slc {
			if argc < 2 {
				ctx.OutContent = resp.ErrWrongArgs(ctx.FullName)
				return
			}
			ctx.FullName += "|" + strings.ToLower(util.BytesToString(ctx.Args[0]))
		}
	}
	ctx.CodecCtx.LastCmd = ctx.FullName
	cmdInfo, ok := commands[ctx.FullName]
	if !ok {
		ctx.OutContent = resp.ErrUnKnownCommand(ctx.FullName)
		return
	}
	arity := cmdInfo.Arity
	if (arity > 0 && argc != arity) ||
		(arity < 0 && argc < -arity) {
		ctx.OutContent = resp.ErrWrongArgs(ctx.FullName)
		return
	}

	// exec command
	st := time.Now()
	err := cmdInfo.Cmd(ctx)
	if err != nil {
		log.Warn("command", ctx.TraceID, "fail to exec command", log.Errors(err))
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	}
	if strings.Contains(ctx.OutContent, "errCode:-10515") {
		ctx.OutContent = resp.ResponseSyntaxErr
	}

	// feed monitor
	if (commands[ctx.FullName].Flag & (CmdSkipMonitor | CmdAdmin)) == 0 {
		feedMonitors(ctx)
	}

	// stats after exec command
	dur := time.Since(st)
	cmdInfo.Stats.Calls++
	cmdInfo.Stats.MicroSec += dur.Microseconds()
}
