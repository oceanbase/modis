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

package resp

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
	ResponsesExpireSetExErr = "-ERR invalid expire time in setex\r\n"
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
