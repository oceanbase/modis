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

package obkv

import (
	"errors"
	"math/rand"
	"time"
)

func setBit(bytes []byte, n int, value byte) (byte, error) {
	if value != 0 && value != 1 {
		return 0, errors.New("value must be 0 or 1")
	}

	byteIndex := n / 8 // 计算字节索引
	bitIndex := n % 8  // 计算位索引

	oldBitValue := (bytes[byteIndex] >> uint(7-bitIndex)) & 1
	//根据value的值进行判断和设置
	if oldBitValue == value {
		// do nothing
	} else if value == 1 {
		bytes[byteIndex] = bytes[byteIndex] | (1 << uint8(7-bitIndex)) // 设置第n位为1
	} else {
		bytes[byteIndex] = bytes[byteIndex] &^ (1 << uint8(7-bitIndex)) // 设置第n位为0
	}

	return oldBitValue, nil
}

func getBit(bytes []byte, offset int) (byte, error) {
	byteIndex := offset / 8
	bitIndex := offset % 8

	if byteIndex >= len(bytes) {
		return 0, nil
	}

	// 获取指定位的值, 从每个byte的最高有效位开始访问
	bitValue := (bytes[byteIndex] >> uint(7-bitIndex)) & 1
	return bitValue, nil
}

func getRandomArray(min int, max int, count int) []int {
	randGen := rand.New(rand.NewSource(time.Now().UnixNano()))
	rangeSize := max - min
	permArr := randGen.Perm(rangeSize)
	arr := make([]int, 0, count)
	for i := 0; i < count; i++ {
		arr = append(arr, permArr[i])
	}
	return arr
}
