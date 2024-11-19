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

package server

import (
	"bytes"
	"strconv"
	"time"

	"github.com/oceanbase/modis/command"
	"github.com/oceanbase/modis/connection/conncontext"
	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/modis/protocol/resp"
	respPak "github.com/oceanbase/modis/protocol/resp"
	"github.com/oceanbase/obkv-table-client-go/obkvrpc"
	"github.com/oceanbase/obkv-table-client-go/util"
	mutil "github.com/oceanbase/modis/util"

	"github.com/google/uuid"
)

// RedisCodec exec commands and reply
type RedisCodec struct {
	CodecCtx *conncontext.CodecContext
	ServCtx  *conncontext.ServerContext
}

// NewRedisCodec creates a new client
func NewRedisCodec(codecCtx *conncontext.CodecContext, servCtx *conncontext.ServerContext) *RedisCodec {
	client := &RedisCodec{CodecCtx: codecCtx, ServCtx: servCtx}
	return client
}

func (rs *RedisCodec) GetCloseChan() *chan struct{} {
	return &rs.CodecCtx.CloseChan
}

// ReadRequest implement obkvrpc.CodecServer interface
func (rs *RedisCodec) ReadRequest(req *obkvrpc.Request) error {
	req.ID = uuid.NewString()
	args, err := rs.readCommand(req)
	if err != nil {
		log.Warn("server", req.ID, "fail to read command", log.Errors(err))
		return err
	}
	req.Method = string(args[0])
	log.Debug("server", req.ID, "read command", log.String("name", req.Method))
	if len(args) > 1 {
		req.Args = args[1:]
	}
	return nil
}

// WriteResponse implement obkvrpc.CodecServer interface
func (rs *RedisCodec) WriteResponse(resp *obkvrpc.Response) error {
	conn := rs.CodecCtx.Conn
	_, err := conn.Write(resp.RspContent)
	if err != nil {
		// rs.CodecCtx.Conn.Close()
		log.Warn("server", resp.ID, "write net failed", log.String("addr", conn.RemoteAddr().String()),
			log.Int64("clientid", rs.CodecCtx.ID),
			log.String("namespace", rs.CodecCtx.DB.Namespace),
			log.String("error", err.Error()))
		return nil
	}
	rs.ServCtx.TotalWriteBytes.Inc(int64(len(resp.RspContent)))
	return nil
}

// Call implement obkvrpc.CodecServer interface
func (rs *RedisCodec) Call(req *obkvrpc.Request, resp *obkvrpc.Response) error {
	rs.CodecCtx.LastCmdTime = time.Now()
	ctx := command.NewCmdContext(req.Method, req.Args, req.ID, req.PlainReq, rs.CodecCtx, rs.ServCtx)
	command.Call(ctx)
	outLen := len(ctx.OutContent)
	if outLen < 3 ||
		ctx.OutContent[outLen-1] != '\n' ||
		ctx.OutContent[outLen-2] != '\r' {
		// should end with \r\n, otherwise redis-cli may get stuck
		log.Warn("Server", ctx.TraceID, "OutContent has syntax error", log.String("err msg", ctx.OutContent))
		ctx.OutContent = respPak.ResponseOutContentErr
	}

	if ctx.OutContent[0] == '-' {
		// log error
		log.Warn("Server", ctx.TraceID, "execute command failed",
			log.String("err msg", ctx.OutContent),
			log.String("command", req.Method),
			log.String("modis ip", rs.CodecCtx.Conn.LocalAddr().String()),
			log.String("client ip", rs.CodecCtx.Conn.RemoteAddr().String()))
	}

	resp.ID = ctx.TraceID
	resp.RspContent = []byte(ctx.OutContent)
	rs.ServCtx.TotalCmdNum.Inc(1)
	rs.CodecCtx.QueNum.Add(-1)
	return nil
}

// Close implement obkvrpc.CodecServer interface
func (rs *RedisCodec) Close() {
	log.Debug("server", nil, "close RPC Server",
		log.Int64("ID", rs.CodecCtx.ID),
		log.Int64("ID", rs.CodecCtx.ID),
		log.String("addr", rs.CodecCtx.Conn.RemoteAddr().String()),
	)
	err := rs.CodecCtx.Conn.Close()
	if err != nil {
		log.Warn("server", "", "fail to close client connection",
			log.Errors(err), log.Int64("ID", rs.CodecCtx.ID),
			log.String("addr", rs.CodecCtx.Conn.RemoteAddr().String()))
	}
	rs.ServCtx.ClientNum.Add(-1)
	rs.ServCtx.Clients.Del(rs.CodecCtx.ID)
}

func (rs *RedisCodec) readCommand(req *obkvrpc.Request) ([][]byte, error) {
	plainReq := &req.PlainReq
	lastReadBytes := *rs.CodecCtx.TotalBytes
	buf, err := mutil.ReadBytesNoClone(rs.CodecCtx.Reader, '\n', plainReq)
	if err != nil {
		log.Warn("server", nil, "fail to read bytes", log.Errors(err))
		return nil, err
	}
	l := len(buf)
	if l < len("*\r\n") {
		return nil, resp.ErrInvalidProtocol
	}
	if buf[l-2] != '\r' {
		return nil, resp.ErrInvalidProtocol
	}
	// not array
	if buf[0] != '*' {
		line := bytes.TrimRight(buf, resp.CRLF)
		return bytes.Fields(line), nil
	}
	// array
	argc, err := strconv.Atoi(util.BytesToString(buf[1 : l-2]))
	if err != nil || argc < 0 {
		log.Warn("server", nil, "fail to do atoi", log.Errors(err))
		return nil, resp.ErrInvalidProtocol
	}
	if argc == 0 {
		return [][]byte{}, nil
	}
	rs.CodecCtx.LastArgvLen = 0
	argv := make([][]byte, argc)
	for i := 0; i < argc; i++ {
		argv[i], err = resp.ReadBulkString(rs.CodecCtx.Reader, plainReq)
		if err != nil {
			log.Warn("server", nil, "fail to read bulk string", log.Errors(err))
			return nil, err
		}
		rs.CodecCtx.LastArgvLen += int64(len(argv[i]))
	}
	rs.CodecCtx.TotalArgvLen += rs.CodecCtx.LastArgvLen
	rs.ServCtx.TotalReadBytes.Inc((*rs.CodecCtx.TotalBytes) - lastReadBytes)
	rs.CodecCtx.QueNum.Add(1)
	return argv, nil
}

func (rs *RedisCodec) GetNormalErrMsg() []byte {
	return []byte(resp.ErrRedisCodec())
}
