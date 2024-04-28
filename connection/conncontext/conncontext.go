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
	"errors"
	"net"
	"time"

	"github.com/oceanbase/modis/config"
	"github.com/oceanbase/modis/storage"
)

const (
	// DefaultNamespace is default namespace of DB
	DefaultNamespace = "default"
)

// ServerContext connect server and clients
type ServerContext struct {
	Storage   storage.Storage
	StartTime time.Time
	Password  string
	DbNum     int
	dbs       []*storage.DB
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
	sc := &ServerContext{
		Storage:  s,
		Password: cfg.Password,
		DbNum:    cfg.DBNum,
		dbs:      make([]*storage.DB, 0, cfg.DBNum),
	}

	for i := 0; i < cfg.DBNum; i++ {
		sc.dbs = append(sc.dbs, storage.NewDB(DefaultNamespace, int64(i), s))
	}
	return sc
}

// NewCodecCtx creates a new client context
func NewCodecCtx(conn net.Conn, id int64, db *storage.DB) *CodecContext {
	return &CodecContext{Conn: conn, ID: id, DB: db, CloseChan: make(chan struct{})}
}

// GetDB prevents visit db out of range
func (sc *ServerContext) GetDB(index int) (*storage.DB, error) {
	if index >= sc.DbNum {
		return nil, errors.New("visit db out of range")
	}
	return sc.dbs[index], nil
}
