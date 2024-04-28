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
	"net"
	"time"

	"github.com/oceanbase/modis/config"
	"github.com/oceanbase/modis/storage"
)

// ServerContext connect server and clients
type ServerContext struct {
	Storage   storage.Storage
	StartTime time.Time
	Password  string
}

// CodecContext completes interface of OBKV RPC Server
type CodecContext struct {
	Conn          net.Conn
	ID            int64
	DB            *storage.DB
	StartTime     time.Time
	CloseChan     chan struct{}
	Authenticated bool
}

// NewServerContext creates a new client context
func NewServerContext(s storage.Storage, cfg *config.ServerConfig) *ServerContext {
	return &ServerContext{
		Storage:  s,
		Password: cfg.Password,
	}
}

// NewCodecCtx creates a new client context
func NewCodecCtx(conn net.Conn, id int64, db *storage.DB) *CodecContext {
	return &CodecContext{Conn: conn, ID: id, DB: db, CloseChan: make(chan struct{})}
}
