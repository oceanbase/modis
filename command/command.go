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
	"context"
	"strings"

	"github.com/oceanbase/modis/connection/conncontext"
	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/modis/protocol/resp"
)

// CmdContext is the runtime context of a command
type CmdContext struct {
	Name       string   // command name，e.g. "set"
	Args       [][]byte // command's args，e.g. ["key", "value"]
	OutContent string   // command's output
	TraceID    string
	CodecCtx   *conncontext.CodecContext
	ServCtx    *conncontext.ServerContext
	context.Context
}

// Command is a redis command implementation
type Command func(ctx *CmdContext) error

// NewCmdContext create a new command context
func NewCmdContext(name string, args [][]byte, traceID string, codecCtx *conncontext.CodecContext, servCtx *conncontext.ServerContext) *CmdContext {
	return &CmdContext{
		Name:       strings.ToLower(name),
		Args:       args,
		OutContent: "",
		TraceID:    traceID,
		CodecCtx:   codecCtx,
		ServCtx:    servCtx,
		Context:    context.Background(),
	}
}

// Call a command
func Call(ctx *CmdContext) {
	if ctx.Name != "auth" &&
		ctx.ServCtx.Password != "" &&
		!ctx.CodecCtx.Authenticated {
		ctx.OutContent = resp.ResponsesNoautherr
		return
	}

	cmdInfo, ok := commands[ctx.Name]
	if !ok {
		ctx.OutContent = resp.ErrUnKnownCommand(ctx.Name)
		return
	}
	argc := len(ctx.Args) + 1 // include the command name
	arity := cmdInfo.Arity

	if (arity > 0 && argc != arity) ||
		(arity < 0 && argc < -arity) {
		ctx.OutContent = resp.ErrWrongArgs(ctx.Name)
		return
	}
	err := cmdInfo.Cmd(ctx)
	if err != nil {
		log.Warn("command", ctx.TraceID, "fail to exec command", log.Errors(err))
		ctx.OutContent = resp.EncError("ERR " + err.Error())
	}
}

// CmdInfo describes a command with constraints
type CmdInfo struct {
	Cmd   Command
	Arity int // number of arguments, it is possible to use -N to say >= N
}
