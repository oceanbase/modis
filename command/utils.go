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

package command

import (
	"bytes"

	"github.com/oceanbase/modis/connection/conncontext"
)

const (
	dbColumnName        = "db"
	keyColumnName       = "rkey"
	vitualKeyColumnName = "vk"
	valueColumnName     = "value"
	expireColumnName    = "expire_ts"
	memberColumnName    = "member"
	indexColumnName     = "index"
)

func clientFlag2Str(flag conncontext.ClientFlag) string {
	flagStr := ""
	if (flag & conncontext.ClientMonitor) != 0 {
		flagStr += "O"
	}
	if flagStr == "" {
		flagStr = "N"
	}
	return flagStr
}

func replaceWithRedacted(arg []byte) {
	red := []byte("(redacted)")
	if !bytes.Equal(arg, red) {
		arg = red
	}
}
