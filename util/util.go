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
	"unsafe"
)

// BytesToString change []byte to string without copy
func BytesToString(bys []byte) string {
	if len(bys) == 0 {
		return ""
	}
	return *(*string)(unsafe.Pointer(&bys))
}

func StringToBytes(str string) []byte {
	if str == "" {
		return nil
	}
	return *(*[]byte)(unsafe.Pointer(&str))
}
