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

package server

import (
	"sync/atomic"
)

const (
	// DefaultNamespace is default namespace of DB
	DefaultNamespace = "default"
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
