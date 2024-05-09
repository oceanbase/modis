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
	"crypto/tls"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/facebookgo/grace/gracenet"
	"github.com/oceanbase/modis/config"
	"github.com/oceanbase/modis/connection/conncontext"
	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/modis/storage"
	"github.com/pkg/errors"

	"github.com/oceanbase/obkv-table-client-go/obkvrpc"
)

const maxQueueCmd = 100

// Server accept request from redis clients
type Server struct {
	ServCtx     *conncontext.ServerContext
	Listener    net.Listener
	IDGenerator func() int64
	CloseChan   chan struct{}
	clientNum   int
}

// NewServer creates a new server
func NewServer(servCtx *conncontext.ServerContext, idGenerator func() int64) *Server {
	return &Server{
		ServCtx:     servCtx,
		IDGenerator: idGenerator,
		CloseChan:   make(chan struct{}),
		clientNum:   0}
}

// Close close server, error should not be returned during execution
func (s *Server) Close() {
	log.Debug("server", nil, "close server", log.String("stack", string(debug.Stack())))
	// stop create new connections
	err := s.Listener.Close()
	if err != nil {
		log.Warn("server", nil, "fail to close linstener", log.Errors(err))
	}
	// close current connection
	close(s.CloseChan)
	if s.ServCtx.Storage != nil {
		err = s.ServCtx.Storage.Close()
		if err != nil {
			log.Warn("server", nil, "fail to close storage", log.Errors(err))
		}
	}
}

// SignalHandle handle signal
func (s *Server) SignalHandle(gnet *gracenet.Net, sigChan chan os.Signal) {
	log.Debug("server", nil, "start to wait signals")
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	sig := <-sigChan
	switch sig {
	case syscall.SIGTERM, syscall.SIGINT:
		log.Info("server", nil, "receive SIGTERM/SIGINT, stop server")
		signal.Stop(sigChan)
		s.Close()
	case syscall.SIGHUP:
		// restart
		pid, err := gnet.StartProcess()
		if err != nil {
			log.Warn("server", nil, "fail to restart process", log.Errors(err), log.Int("pid", pid))
		} else {
			log.Info("server", nil, "success to restart process", log.Errors(err), log.Int("pid", pid))
		}
		signal.Stop(sigChan)
		s.Close()
	}
}

// ListenAndServe handle connection and request from clients
func (s *Server) ListenAndServe(servCfg *config.ServerConfig, tlsCfg *tls.Config) error {
	defer func() {
		if err := recover(); err != nil {
			log.Error("server", nil, "server panic, exit", log.Any("error", err), log.String("stack", string(debug.Stack())))
		}
	}()
	var err error
	if servCfg.MaxConnection > 10000 || servCfg.MaxConnection < 1 {
		err = errors.New("server max connection should be >= 1 and <= 10000")
		return err
	}
	// Listen
	gnet := &gracenet.Net{}
	s.Listener, err = gnet.Listen("tcp", servCfg.Listen)
	if err != nil {
		log.Warn("server", nil, "fail to listen address", log.Errors(err), log.String("addr", servCfg.Listen))
		return err
	}
	if tlsCfg != nil {
		s.Listener = tls.NewListener(s.Listener, tlsCfg)
	}
	log.Debug("server", nil, "tcp: listen to ", log.String("addr", servCfg.Listen))
	// process signal
	sigChan := make(chan os.Signal, 1)
	go s.SignalHandle(gnet, sigChan)

	// Serve
	s.ServCtx.StartTime = time.Now()
	obkvServer, err := obkvrpc.NewServer(servCfg.MaxConnection, &s.CloseChan)
	db := storage.NewDB(DefaultNamespace, DefaultDBNum, s.ServCtx.Storage)
	if err != nil {
		log.Error("server", nil, "fail to create new OBKV RPC server", log.Errors(err))
		return err
	}
	for { // until Accept() return error
		conn, err := s.Listener.Accept()
		if err != nil {
			log.Error("server", nil, "fail to accept connection", log.Errors(err), log.String("addr", s.Listener.Addr().String()))
			return err
		}
		if s.clientNum+1 > servCfg.MaxConnection {
			log.Warn("server", nil, "exceed max connection num", log.Errors(err), log.String("addr", s.Listener.Addr().String()))
			conn.Close()
			continue
		}
		s.clientNum += 1
		cliCtx := conncontext.NewCodecCtx(conn, s.IDGenerator(), db)
		redisSrv := NewRedisCodec(cliCtx, s.ServCtx)
		go obkvServer.ServeCodec(redisSrv, maxQueueCmd)
	}
}
