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
	"fmt"

	"github.com/oceanbase/modis/config"
	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/modis/storage/obkv"
	"github.com/oceanbase/obkv-table-client-go/table"
)

type Storage interface {
	Initialize() error

	// general interface for commands that can be executed on the observer side
	ObServerCmd(ctx context.Context, cmdName string, rowKey []*table.Column, plainText []byte) (string, error)

	Close() error
}

func NewStorage(cfg Config) Storage {
	return obkv.NewStorage(cfg.(*obkv.Config))
}

// Open a storage instance
func Open(config *config.ObkvStorageConfig) (Storage, error) {
	fmt.Println("start to connect to database...")
	log.Info("Storage", nil, "start to connect to database...")
	cfg := NewConfig(config)
	storage := NewStorage(cfg)
	if err := storage.Initialize(); err != nil {
		return nil, err
	}
	log.Info("Storage", nil, "connect to database ended")
	fmt.Println("connect to database ended")
	return storage, nil
}
