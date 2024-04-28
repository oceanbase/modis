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
}

func NewDB(namespace string, id int64, storage Storage) *DB {
	return &DB{
		Namespace: namespace,
		ID:        id,
		Storage:   storage,
		Ctx:       context.TODO(),
	}
}
