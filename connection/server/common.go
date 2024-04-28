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

package server

import (
	"sync/atomic"
)

const (
	// InitClientID + 1 is the first client's ID
	InitClientID = 0
	// DefaultDBNum is default num of DB
	DefaultDBNum = 0
)

// GenClientID generates client id
func GenClientID() func() int64 {
	var id int64 = InitClientID
	return func() int64 {
		return atomic.AddInt64(&id, 1)
	}
}
