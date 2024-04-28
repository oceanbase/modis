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

package command

import (
	"bytes"
)

func bitCount(bytes []byte, start, end int) (int, error) {
	length := len(bytes)
	if end < 0 {
		end = length + end
	}

	if start < 0 {
		start = length + start
	}

	if start > end || start >= length {
		return 0, nil
	}

	if start < 0 {
		start = 0
	}

	if end < 0 {
		end = 0
	} else if end >= length {
		end = length - 1
	}

	count := 0
	for i := start; i <= end; i++ {
		for j := 0; j < 8; j++ {
			if (bytes[i]>>j)&1 == 1 {
				count++
			}
		}
	}

	return count, nil
}

func getRange(bytes []byte, start, end int) []byte {
	length := len(bytes)
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = length + start
	}
	if start > end || start > length || end < 0 {
		return nil
	}
	if end > length {
		end = length - 1
	}
	if start < 0 {
		start = 0
	}
	return bytes[start : end+1]
}

func setRange(bytes []byte, offset int64, value []byte) []byte {
	if int64(len(bytes)) < offset+int64(len(value)) {
		bytes = append(bytes, make([]byte, offset+int64(len(value))-int64(len(bytes)))...)
	}
	copy(bytes[offset:], value)

	return bytes
}

func getExclusiveElements(firstMembers [][]byte, secondMembers [][]byte) [][]byte {
	exclusiveElements := [][]byte{}

	for _, member := range firstMembers {
		found := false
		for _, otherMember := range secondMembers {
			if bytes.Equal(member, otherMember) {
				found = true
				break
			}
		}
		if !found {
			exclusiveElements = append(exclusiveElements, member)
		}
	}

	return exclusiveElements
}

func getIntersection(slices ...[][]byte) [][]byte {
	if len(slices) == 0 {
		return [][]byte{}
	}

	intersection := make(map[string]int)

	for _, slice := range slices {
		for _, element := range slice {
			intersection[string(element)]++
		}
	}

	var result [][]byte

	for element, count := range intersection {
		if count == len(slices) {
			result = append(result, []byte(element))
		}
	}

	return result
}

func getUnion(slices ...[][]byte) [][]byte {
	union := make(map[string]bool)

	for _, slice := range slices {
		for _, element := range slice {
			union[string(element)] = true
		}
	}

	result := [][]byte{}

	for element := range union {
		result = append(result, []byte(element))
	}

	return result
}

func getDifference(slices ...[][]byte) [][]byte {
	if len(slices) == 0 {
		return nil
	}

	diff := slices[0]

	for i := 1; i < len(slices); i++ {
		diff = getExclusiveElements(diff, slices[i])
	}

	return diff
}
