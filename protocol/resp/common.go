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

package resp

import (
	"fmt"
	"math"
)

const (
	SimpleErrFlag = "-"
	SimpleStrFlag = "+"
	BulkStrFlag   = "$"
	IntFlag       = ":"
	ArrayFlag     = "*"
	CRLF          = "\r\n"
	Space         = " "

	// Shared command responses
	ResponsesOk             = "+OK\r\n"
	ResponsesNullBulkString = "$-1\r\n"
	ResponsesPong           = "+PONG\r\n"

	// Shared command error responses
	ResponsesNoautherr      = "-NOAUTH Authentication required.\r\n"
	ResponseIntegerErr      = "-ERR value is not an integer or out of range\r\n"
	ResponseFloatErr        = "-ERR value is not a valid float\r\n"
	ResponseBitIntegerErr   = "-ERR bit is not an integer or out of range\r\n"
	ResponseBitOffsetErr    = "-ERR bit offset is not an integer or out of range\r\n"
	ResponseSyntaxErr       = "-ERR syntax error\r\n"
	ResponseMaximumErr      = "-ERR string exceeds maximum allowed size\r\n"
)

// ErrUnKnownCommand return RedisError of the cmd
func ErrUnKnownCommand(cmd string) string {
	return "-unknown command '" + cmd + "'\r\n"
}

// ErrWrongArgs return RedisError of the cmd
func ErrWrongArgs(cmd string) string {
	return "-ERR wrong number of arguments for '" + cmd + "' command\r\n"
}

func ErrOutRange(min int64, max int64) string {
	return fmt.Sprintf("-ERR value is out of range, value must between %d and %d", min, max)
}

func ErrOutRangeDefault() string {
	return fmt.Sprintf("-ERR value is out of range, value must between %d and %d", math.MaxInt, math.MinInt)
}

func ErrRedisCodec() string {
	return "-ERR error occurred in obkv go client"
}

func ErrInvalidExpire(funcName string) string {
	return "-ERR invalid expire time in" + funcName + "\r\n"
}
