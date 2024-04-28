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

package server

import (
	"bufio"
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
	Reader   *bufio.Reader
}

// NewRedisCodec creates a new client
func NewRedisCodec(codecCtx *conncontext.CodecContext, servCtx *conncontext.ServerContext) *RedisCodec {
	client := &RedisCodec{CodecCtx: codecCtx, ServCtx: servCtx, Reader: bufio.NewReader(codecCtx.Conn)}
	client.CodecCtx.StartTime = time.Now()
	return client
}

func (rs *RedisCodec) GetCloseChan() *chan struct{} {
	return &rs.CodecCtx.CloseChan
}

// ReadRequest implement obkvrpc.CodecServer interface
func (rs *RedisCodec) ReadRequest(req *obkvrpc.Request) error {
	args, err := rs.readCommand()
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
	return nil
}

// Call implement obkvrpc.CodecServer interface
func (rs *RedisCodec) Call(req *obkvrpc.Request, resp *obkvrpc.Response) error {
	ctx := command.NewCmdContext(req.Method, req.Args, req.ID, rs.CodecCtx, rs.ServCtx)
	command.Call(ctx)

	resp.ID = ctx.TraceID
	resp.RspContent = []byte(ctx.OutContent)
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
}

func (rs *RedisCodec) readCommand() ([][]byte, error) {
	buf, err := rs.Reader.ReadBytes('\n')
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

	argv := make([][]byte, argc)
	for i := 0; i < argc; i++ {
		arg, err := resp.ReadBulkString(rs.Reader)
		if err != nil {
			log.Warn("server", nil, "fail to read bulk string", log.Errors(err))
			return nil, err
		}
		argv[i] = arg
	}
	return argv, nil
}
