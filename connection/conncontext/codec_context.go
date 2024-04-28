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
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		file, err := tcpConn.File()
		if err == nil {
			// closing file does not affect conn
			defer file.Close()
			cc.Fd = int(file.Fd())
		}
	}
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
