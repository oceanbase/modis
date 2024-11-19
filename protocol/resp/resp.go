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
	"bufio"
	"bytes"
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
func ReadBulkString(r *bufio.Reader, plainReq *[]byte) ([]byte, error) {
	return NewDecoder(r).BulkString(plainReq)
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
func (r *Decoder) BulkString(plainReq *[]byte) ([]byte, error) {
	line, err := util.ReadBytesNoClone(r.bufReader, '\n', plainReq)
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

	*plainReq = util.ExpandSlice(*plainReq, msgLen+2)
	body := (*plainReq)[len(*plainReq)-(msgLen+2):]
	_, err = io.ReadFull(r.bufReader, body)
	if err != nil {
		log.Warn("decoder", nil, "fail to read bytes", log.Errors(err))
		return nil, ErrInvalidProtocol
	}
	return body[:len(body)-2], nil
}

func (r *Decoder) Integer() (int, error) {
	line, err := r.bufReader.ReadBytes('\n')
	if err != nil {
		log.Warn("decoder", nil, "fail to read bytes", log.Errors(err))
		return 0, err
	}

	l := len(line)
	if l < len("$*\r\n") || line[l-2] != '\r' || line[0] != ':' {
		return 0, ErrInvalidProtocol
	}

	retInt, err := strconv.Atoi(util.BytesToString(line[1 : l-2]))
	if err != nil {
		log.Warn("decoder", nil, "fail to read bytes", log.Errors(err))
		return 0, ErrInvalidProtocol
	}
	return retInt, nil
}

func DecInteger(msg string) (int, error) {
	d := NewDecoder(bufio.NewReader(bytes.NewBufferString(msg)))
	val, err := d.Integer()
	return val, err
}
