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

package storage

import (
	"context"
)

// DB is a redis compatible data structure storage
type DB struct {
	Namespace string
	ID        int64
	Storage   Storage
	Ctx       context.Context
	IsInit    bool
}

func NewDB(namespace string, id int64, storage Storage) *DB {
	return &DB{
		Namespace: namespace,
		ID:        id,
		Storage:   storage,
		Ctx:       context.TODO(),
		IsInit:    false,
	}
}
