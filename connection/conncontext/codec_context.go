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

package conncontext

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/oceanbase/modis/storage"
)

const (
	runIDLength = 40
)

type ClientFlag int

const (
	ClientNone ClientFlag = 0
	// This client is a slave monitor
	ClientMonitor ClientFlag = 1 << iota
)

type ClientType int

const (
	ClientNormal ClientType = iota // 0
	ClientTypeMax
)

// CodecContext completes interface of OBKV RPC Server
type CodecContext struct {
	Conn          net.Conn
	ID            int64
	DB            *storage.DB
	StartTime     time.Time
	CloseChan     chan struct{}
	Authenticated bool
	Reader        *bufio.Reader
	TotalBytes    *int64
	TotalArgvLen  int64
	ArgvMem       int64
	Fd            int // set -1 if get fd of conn failed
	Name          string
	LastCmdTime   time.Time
	LastArgvLen   int64
	LastCmd       string
	RespVer       int        // only support 2 currently
	Flag          ClientFlag // only support ClientNone currently
	Type          ClientType // only support ClientNormal currently
	QueLimit      int64
	QueNum        *atomic.Int64
}

// ReadCounter record totoal bytes read from reader
type ReadCounter struct {
	reader     io.Reader
	TotalBytes int64
}

func (rc *ReadCounter) Read(p []byte) (int, error) {
	n, err := rc.reader.Read(p)
	rc.TotalBytes += int64(n)
	return n, err
}

// NewCodecCtx creates a new client context
func NewCodecCtx(conn net.Conn, id int64, db *storage.DB, queLimit int) *CodecContext {
	tm := time.Now()
	cc := &CodecContext{
		Conn:         conn,
		ID:           id,
		DB:           db,
		CloseChan:    make(chan struct{}),
		Fd:           -1,
		Name:         "",
		StartTime:    tm,
		LastCmdTime:  tm,
		TotalArgvLen: 0,
		LastArgvLen:  0,
		RespVer:      2,
		Flag:         ClientNone,
		Type:         ClientNormal,
		QueLimit:     int64(queLimit),
		QueNum:       new(atomic.Int64),
	}
	rc := &ReadCounter{reader: conn}
	cc.TotalBytes = &rc.TotalBytes
	cc.Reader = bufio.NewReader(rc)
	return cc
}

func GetClientTypeByName(str string) ClientType {
	switch strings.ToLower(str) {
	case "normal":
		return ClientNormal
	default:
		return ClientTypeMax
	}
}
