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
	"testing"

	"github.com/oceanbase/modis/protocol/resp"
	"github.com/stretchr/testify/assert"
)

func TestArray_Encode(t *testing.T) {
	assert := assert.New(t)
	e := resp.NewEncoder()

	// Empty array
	var empty_arr [][]byte
	code_array := e.Array(empty_arr)

	assert.Equal("*0\r\n", code_array)

	stringSliceOfSlice := [][]string{
		{"hello", "world"},
		{"foo", "bar", "baz"},
	}

	// 初始化byte切片的切片，大小与string切片的切片相同
	byteSliceOfSlice := make([][]byte, len(stringSliceOfSlice))

	// 遍历string切片的切片
	for i, stringSlice := range stringSliceOfSlice {
		// 为每个子切片分配空间
		byteSliceOfSlice[i] = make([]byte, 0, len(stringSlice))
		for _, s := range stringSlice {
			// 转换string为byte切片并添加到对应的子切片
			byteSliceOfSlice[i] = append(byteSliceOfSlice[i], []byte(s)...)
		}
	}

	// Array with one item
	code_array = e.Array(byteSliceOfSlice)
	assert.Equal("*2\r\n$10\r\nhelloworld\r\n$9\r\nfoobarbaz\r\n", code_array)
}

func TestSimpleString_Encode(t *testing.T) {
	assert := assert.New(t)
	encode_msg := resp.EncSimpleString("OK")
	assert.Equal("+OK\r\n", encode_msg)
}

func TestBulkString_Decode(t *testing.T) {
	var plainReq []byte
	assert := assert.New(t)
	d := resp.NewDecoder(bufio.NewReader(bytes.NewBufferString("$4\r\ntest\r\n")))
	val, err := d.BulkString(&plainReq)
	assert.NoError(err)
	assert.Equal("test", string(val))
	assert.Equal("$4\r\ntest\r\n", string(plainReq))

	// Truncated data
	plainReq = plainReq[:0]
	d = resp.NewDecoder(bufio.NewReader(bytes.NewBufferString("$3\r\ntest\r\n")))
	val, err = d.BulkString(&plainReq)
	assert.NoError(err)
	assert.Equal("tes", string(val))
	assert.Equal("$3\r\ntes\r\n", string(plainReq))

	// Invalid indicator
	plainReq = plainReq[:0]
	d = resp.NewDecoder(bufio.NewReader(bytes.NewBufferString("*4\r\ntest\r\n")))
	val, err = d.BulkString(&plainReq)
	assert.Error(err)
	assert.Equal("", string(val))

	// Invalid delimiter
	plainReq = plainReq[:0]
	d = resp.NewDecoder(bufio.NewReader(bytes.NewBufferString("*4\rtest\r\n")))
	val, err = d.BulkString(&plainReq)
	assert.Error(err)
	assert.Equal("", string(val))

	// Naughty string
	plainReq = plainReq[:0]
	d = resp.NewDecoder(bufio.NewReader(bytes.NewBufferString("asdfghjk")))
	val, err = d.BulkString(&plainReq)
	assert.Error(err)
	assert.Equal("", string(val))
}

func TestBulkString_Encode(t *testing.T) {
	assert := assert.New(t)
	enc_msg := resp.EncBulkString("test")
	assert.Equal("$4\r\ntest\r\n", enc_msg)
}

func TestError_Encode(t *testing.T) {
	assert := assert.New(t)
	enc_msg := resp.EncError("error")
	assert.Equal("-error\r\n", enc_msg)
}

func TestInteger_Encode(t *testing.T) {
	assert := assert.New(t)
	enc_msg := resp.EncInteger(1)
	assert.Equal(":1\r\n", enc_msg)
}

func TestNullBulkString_Encode(t *testing.T) {
	assert := assert.New(t)
	enc_msg := resp.EncNullBulkString()
	assert.Equal("$-1\r\n", enc_msg)
}
