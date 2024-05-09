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

package connection

import (
	"syscall"
	"testing"
	"time"

	"os"

	"github.com/facebookgo/grace/gracenet"
	"github.com/oceanbase/modis/connection/conncontext"
	"github.com/oceanbase/modis/connection/server"
	"github.com/oceanbase/modis/test"
	"github.com/stretchr/testify/assert"
)

func TestTerminateSignals(t *testing.T) {
	test.InitLogger()
	for _, s := range []os.Signal{syscall.SIGINT, syscall.SIGTERM} {
		srv := server.NewServer(&conncontext.ServerContext{}, server.GenClientID())
		gnet := &gracenet.Net{}
		var err error
		srv.Listener, err = gnet.Listen("tcp", "127.0.0.1:1234")
		assert.Equal(t, nil, err)
		sigChan := make(chan os.Signal, 1)
		go srv.SignalHandle(gnet, sigChan)
		sigChan <- s
		_, err = srv.Listener.Accept()
		assert.Equal(t, "accept tcp 127.0.0.1:1234: use of closed network connection", err.Error())
		time.Sleep(time.Millisecond * 500)
	}
}
