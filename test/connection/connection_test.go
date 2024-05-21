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
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestAuth(t *testing.T) {
	cmds_m, err := mCli.Pipelined(context.TODO(), func(pipe redis.Pipeliner) error {
		pipe.Auth(context.TODO(), "password")
		pipe.Auth(context.TODO(), "")
		return nil
	})
	assert.NotEqual(t, nil, err)
	assert.Contains(t, err.Error(), "ERR AUTH")
	assert.Contains(t, cmds_m[0].Err().Error(), "ERR AUTH")
	assert.Contains(t, cmds_m[1].Err().Error(), "ERR AUTH")
}

func TestEcho(t *testing.T) {
	msg := "hello"
	pipe := rCli.Pipeline()
	echo := pipe.Echo(context.TODO(), msg)
	_, err := pipe.Exec(context.TODO())
	assert.Equal(t, nil, err)
	assert.Equal(t, nil, echo.Err())
	assert.Equal(t, msg, echo.Val())

	pipe = mCli.Pipeline()
	echo_m := pipe.Echo(context.TODO(), msg)
	_, err = pipe.Exec(context.TODO())
	assert.Equal(t, nil, err)
	assert.Equal(t, echo, echo_m)
}

func TestPing(t *testing.T) {
	ping := rCli.Ping(context.TODO())
	assert.Equal(t, nil, ping.Err())
	assert.Equal(t, "PONG", ping.Val())

	ping_m := mCli.Ping(context.TODO())
	assert.Equal(t, ping, ping_m)
}
