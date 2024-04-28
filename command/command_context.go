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
)

// CmdContext is the runtime context of a command
type CmdContext struct {
	Name       string   // command name，e.g. "client"
	FullName   string   // command name，e.g. "client|info"
	Args       [][]byte // command's args，e.g. ["key", "value"]
	OutContent string   // command's output
	TraceID    string
	CodecCtx   *conncontext.CodecContext
	ServCtx    *conncontext.ServerContext
	context.Context
}

type CmdFlag int

const (
	CmdNone  CmdFlag = 0
	CmdAdmin CmdFlag = 1 << iota
	CmdSkipMonitor
)

// CmdInfo describes a command with constraints
type CmdInfo struct {
	Cmd Command
	// number of arguments, it is possible to use -N to say >= N
	Arity int
	Flag  CmdFlag
	Stats CmdStats
}

// CmdStat describes command statistics
type CmdStats struct {
	Calls    int64
	MicroSec int64
}

func (cs *CmdStats) GetUsecPerCall() float64 {
	return float64(cs.MicroSec) / float64(cs.Calls)
}

// Command is a modis command implementation
type Command func(ctx *CmdContext) error

var (
	secondLevelCmd = []string{"client"}
)

// NewCmdContext create a new command context
func NewCmdContext(name string, args [][]byte, traceID string, codecCtx *conncontext.CodecContext, servCtx *conncontext.ServerContext) *CmdContext {
	return &CmdContext{
		Name:       name,
		FullName:   strings.ToLower(name),
		Args:       args,
		OutContent: "",
		TraceID:    traceID,
		CodecCtx:   codecCtx,
		ServCtx:    servCtx,
		Context:    context.Background(),
	}
}
