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