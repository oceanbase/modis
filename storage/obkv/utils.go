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

package obkv

import (
	"errors"
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
