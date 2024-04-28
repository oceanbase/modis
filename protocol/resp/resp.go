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

import (
	"bufio"
	"errors"
	"io"
	"strconv"

	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/modis/util"
)

var (
	//ErrInvalidProtocol indicates a wrong protocol format
	ErrInvalidProtocol = errors.New("invalid protocol")
)

///////////////////////////////////////////////////////////////////////////////////////////////////
// Encoder //
///////////////////////////////////////////////////////////////////////////////////////////////////

// ReplyError replies an error
func EncError(msg string) string {
	return NewEncoder().Error(msg)
}

// ReplySimpleString replies a simplestring
func EncSimpleString(msg string) string {
	return NewEncoder().SimpleString(msg)
}

// ReplyBulkString replies a bulkstring
func EncBulkString(msg string) string {
	return NewEncoder().BulkString(msg)
}

// ReplyNullBulkString replies a null bulkstring
func EncNullBulkString() string {
	return NewEncoder().NullBulkString()
}

// ReplyInteger replies an integer
func EncInteger(val int64) string {
	return NewEncoder().Integer(val)
}

// ReplyArray replies an array
func EncArray(a [][]byte) string {
	return NewEncoder().Array(a)
}

// Encoder implements the Encoder interface
type Encoder struct {
}

// NewEncoder creates a RESP encoder
func NewEncoder() *Encoder {
	return &Encoder{}
}

// Encode Simple Error
func (r *Encoder) Error(s string) string {
	return SimpleErrFlag + s + CRLF
}

// Encode Simple String
func (r *Encoder) SimpleString(s string) string {
	return SimpleStrFlag + s + CRLF
}

// Encode Bulk String
func (r *Encoder) BulkString(s string) string {
	length := strconv.Itoa(len(s))
	return BulkStrFlag + length + CRLF + s + CRLF
}

// Encode Null Bulk String
func (r *Encoder) NullBulkString() string {
	return ResponsesNullBulkString
}

// Encode Integer
func (r *Encoder) Integer(v int64) string {
	s := strconv.FormatInt(v, 10)
	return IntFlag + s + CRLF
}

// Encode Array
func (r *Encoder) Array(array [][]byte) string {
	s := strconv.Itoa(len(array))
	encResString := ArrayFlag + s + CRLF
	for _, str := range array {
		if str == nil {
			encResString += r.NullBulkString()
		} else {
			encResString += r.BulkString(util.BytesToString(str))
		}
	}
	return encResString
}

type Reply interface {
	GetBytes() []byte
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Decoder //
///////////////////////////////////////////////////////////////////////////////////////////////////

// ReadBulkString reads a bulkstring
func ReadBulkString(r *bufio.Reader) ([]byte, error) {
	return NewDecoder(r).BulkString()
}

// Decoder implements the decoder interface
type Decoder struct {
	bufReader *bufio.Reader
}

// NewDecoder creates a RESP decoder
func NewDecoder(r *bufio.Reader) *Decoder {
	return &Decoder{r}
}

// BulkString parses a RESP bulkstring
func (r *Decoder) BulkString() ([]byte, error) {
	line, err := r.bufReader.ReadBytes('\n')
	if err != nil {
		log.Warn("decoder", nil, "fail to read bytes", log.Errors(err))
		return nil, err
	}
	l := len(line)
	if l < len("$*\r\n") || line[l-2] != '\r' || line[0] != '$' {
		return nil, ErrInvalidProtocol
	}

	msgLen, err := strconv.Atoi(util.BytesToString(line[1 : l-2]))
	if err != nil || msgLen < 0 {
		log.Warn("decoder", nil, "fail to read bytes", log.Errors(err))
		return nil, ErrInvalidProtocol
	}

	body := make([]byte, msgLen+2) // end with \r\n
	_, err = io.ReadFull(r.bufReader, body)
	if err != nil {
		log.Warn("decoder", nil, "fail to read bytes", log.Errors(err))
		return nil, ErrInvalidProtocol
	}
	return body[:len(body)-2], nil
}
