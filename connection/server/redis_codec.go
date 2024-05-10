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
	"io"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/oceanbase/modis/command"
	"github.com/oceanbase/modis/connection/conncontext"
	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/modis/protocol/resp"
	"github.com/oceanbase/obkv-table-client-go/obkvrpc"
	"github.com/oceanbase/obkv-table-client-go/util"

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
	args, err := rs.readCommand(&req.PlainReq)
	if err != nil {
		log.Warn("server", req.ID, "fail to read command", log.Errors(err))
		return err
	}
	req.Method = string(args[0])
	log.Debug("server", req.ID, "read command", log.String("name", req.Method))
	if len(args) > 1 {
		req.Args = args[1:]
	}
	req.ID = uuid.NewString()
	return nil
}

// WriteResponse implement obkvrpc.CodecServer interface
func (rs *RedisCodec) WriteResponse(resp *obkvrpc.Response) error {
	conn := rs.CodecCtx.Conn
	_, err := conn.Write(resp.RspContent)
	if err != nil {
		log.Warn("server", resp.ID, "fail to read command", log.Errors(err))
		rs.CodecCtx.Conn.Close()
		if err == io.EOF {
			log.Info("server", resp.ID, "close connection", log.String("addr", conn.RemoteAddr().String()),
				log.Int64("clientid", rs.CodecCtx.ID))
		} else {
			log.Error("server", resp.ID, "write net failed", log.String("addr", conn.RemoteAddr().String()),
				log.Int64("clientid", rs.CodecCtx.ID),
				log.String("namespace", rs.CodecCtx.DB.Namespace),
				log.String("error", err.Error()))
			return err
		}
	}
	rs.ServCtx.TotalWriteBytes.Inc(int64(len(resp.RspContent)))
	return nil
}

// Call implement obkvrpc.CodecServer interface
func (rs *RedisCodec) Call(req *obkvrpc.Request, resp *obkvrpc.Response) error {
	rs.CodecCtx.LastCmdTime = time.Now()
	ctx := command.NewCmdContext(req.Method, req.Args, req.ID, req.PlainReq, rs.CodecCtx, rs.ServCtx)
	command.Call(ctx)

	resp.ID = ctx.TraceID
	resp.RspContent = []byte(ctx.OutContent)
	rs.ServCtx.TotalCmdNum.Inc(1)
	rs.CodecCtx.QueNum.Add(-1)
	return nil
}

// Close implement obkvrpc.CodecServer interface
func (rs *RedisCodec) Close() {
	log.Info("server", nil, "close RPC Server", log.String("stack", string(debug.Stack())))
	err := rs.CodecCtx.Conn.Close()
	if err != nil {
		log.Warn("server", "", "fail to close client connection",
			log.Errors(err), log.Int64("ID", rs.CodecCtx.ID),
			log.String("addr", rs.CodecCtx.Conn.RemoteAddr().String()))
	}
	rs.ServCtx.ClientNum--
	delete(rs.ServCtx.Clients, rs.CodecCtx.ID)
}

func (rs *RedisCodec) readCommand(plainReq *[]byte) ([][]byte, error) {
	lastReadBytes := *rs.CodecCtx.TotalBytes
	buf, err := rs.CodecCtx.Reader.ReadBytes('\n')
	*plainReq = append(*plainReq, buf...)
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
		arg, err := resp.ReadBulkString(rs.CodecCtx.Reader, plainReq)
		if err != nil {
			log.Warn("server", nil, "fail to read bulk string", log.Errors(err))
			return nil, err
		}
		argv[i] = arg
		rs.CodecCtx.LastArgvLen += int64(len(arg))
		log.Debug("server", nil, "read command", log.Int("arg idx", i), log.String("val", string(arg)))
	}
	rs.CodecCtx.TotalArgvLen += rs.CodecCtx.LastArgvLen
	rs.ServCtx.TotalReadBytes.Inc((*rs.CodecCtx.TotalBytes) - lastReadBytes)
	rs.CodecCtx.QueNum.Add(1)
	return argv, nil
}
