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

package protocol

// Encoder defines the interface of a RESP encoder
type Encoder interface {
	Error(s string) string
	SimpleString(s string) string
	BulkString(s string) string
	NullBulkString() string
	Integer(v int64) string
	Array(array [][]byte) string
}

// Decoder defines the interface of a RESP decoder
type Decoder interface {
	BulkString() (string, error)
}
