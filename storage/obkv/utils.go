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
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/obkv-table-client-go/client/option"
	"github.com/oceanbase/obkv-table-client-go/table"
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

// ObServerCmd is a general interface for commands that can be executed on the observer side
func (s *Storage) ObServerCmd(ctx context.Context, tableName string, rowKey []*table.Column, plainText []byte) (string, error) {
	mutateColumns := []*table.Column{
		table.NewColumn("REDIS_CODE_STR", plainText),
	}

	// Create query
	result, err := s.cli.Redis(
		ctx,
		tableName,
		rowKey,
		mutateColumns,
		option.WithReturnAffectedEntity(true),
	)
	log.Debug("storage", nil, "Redis command", log.String("table name", tableName), log.String("table name", string(plainText)))
	if err != nil {
		return "", err
	}
	encodedRes, ok := result.Value("REDIS_CODE_STR").(string)
	if !ok {
		err = errors.New("result returned by obkv client is not string type")
		return "", err
	}
	return encodedRes, nil
}

// SimplifyNumber simplify 1.000000000 to 1
// do not use to simplify 1000 -> 1
func SimplifyNumber(num []byte) []byte {
	truncIdx := len(num) - 1
	for ; truncIdx >= 0 && num[truncIdx] == '0'; truncIdx-- {
		// do nothing
	}
	if truncIdx >= 0 && num[truncIdx] == '.' {
		truncIdx--
	}
	return num[:truncIdx+1]
}
