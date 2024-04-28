/*-
 * #%L
 * Modis
 * %%
 * Copyright (C) 2024 OceanBase
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
