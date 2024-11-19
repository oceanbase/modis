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

package util

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"net"
	"os"
	"strconv"
	"time"
	"unsafe"

	"github.com/oceanbase/modis/log"
)

var (
	fixedSeed     = false
	seed          = make([]byte, 64)
	randomCounter = 0
)

// BytesToString change []byte to string without copy
func BytesToString(bys []byte) string {
	if len(bys) == 0 {
		return ""
	}
	return *(*string)(unsafe.Pointer(&bys))
}

// StringToBytes change string to []byte without copy
func StringToBytes(str string) []byte {
	if str == "" {
		return nil
	}
	return *(*[]byte)(unsafe.Pointer(&str))
}

// SdNotify send stateMsg to systemd
// return error if NOTIFY_SOCKET not exist or send stateMsg failed
func SdNotify(stateMsg string) error {
	socketName := os.Getenv("NOTIFY_SOCKET")
	socketNet := "unixgram"
	if socketName == "" {
		err := errors.New("NOTIFY_SOCKET is empty")
		log.Warn("util", nil, "fail to send status to systemd", log.Errors(err), log.String("stateMsg", stateMsg))
		return err
	}
	socketAddr := &net.UnixAddr{
		Name: socketName,
		Net:  socketNet,
	}
	conn, err := net.DialUnix(socketNet, nil, socketAddr)
	if err != nil {
		log.Warn("util", nil, "fail to send status to systemd", log.Errors(err), log.String("stateMsg", stateMsg))
		return err
	}
	defer conn.Close()
	_, err = conn.Write([]byte(stateMsg))
	if err != nil {
		log.Warn("util", nil, "fail to send status to systemd", log.Errors(err), log.String("stateMsg", stateMsg))
		return err
	}
	return nil
}

// updateSeed try to get seed from rand.Read
// if failed, generate seed from pid and time
func updateSeed() {
	if fixedSeed {
		// already get seed from rand.Read, do nothing
		return
	}
	useUrandom := true
	_, err := rand.Read(seed)
	if err != nil {
		useUrandom = false
	} else {
		fixedSeed = true
	}

	if !useUrandom {
		for i := 0; i < 64; i++ {
			pid := os.Getpid()
			now := time.Now()
			seed[i] = byte(now.Unix() ^ now.UnixMicro() ^ int64(pid))
		}
	}
}

// GenRandomBytes generate random bytes using HMAC256(seed, randomCounter)
// truncate return []byte to size length
func GenRandomBytes(size int) ([]byte, error) {
	updateSeed()
	mac := hmac.New(sha256.New, seed)
	_, err := mac.Write([]byte(strconv.Itoa(randomCounter)))
	if err != nil {
		log.Warn("util", nil, "fail to do hmac write", log.Errors(err))
		return nil, err
	}
	randomCounter++
	rb := mac.Sum(nil)
	if len(rb) > size {
		rb = rb[:size]
	}
	return rb, nil
}

// Use bufio.ReadSlice read bytes appending to palinReq until
// the first occurrence of delim. It returns []bytes in plainReq.
func ReadBytesNoClone(b *bufio.Reader, delim byte, plainReq *[]byte) (ret_slice []byte, err error) {
	var frag []byte
	var start int = len(*plainReq)
	for {
		frag, err = b.ReadSlice(delim)
		if err == nil { // no err, got final fragment
			break
		}
		if err != bufio.ErrBufferFull { // unexpected error
			break
		}

		*plainReq = append(*plainReq, frag...)
	}
	*plainReq = append(*plainReq, frag...)
	return (*plainReq)[start:], err
}

func ExpandSlice[T any](org []T, expand_len int) []T {
	available_len := cap(org) - len(org)
	dst_len := len(org) + expand_len
	if available_len >= expand_len {
		return org[:dst_len]
	}
	new_bytes := make([]T, dst_len)
	copy(new_bytes, org)
	return new_bytes
}
