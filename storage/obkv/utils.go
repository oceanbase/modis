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

	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/obkv-table-client-go/client/option"
	"github.com/oceanbase/obkv-table-client-go/table"
)

// ObServerCmd is a general interface for commands that can be executed on the observer side
func (s *Storage) ObServerCmd(ctx context.Context, cmdName string, rowKey []*table.Column, plainText []byte) (string, error) {
	mutateColumns := []*table.Column{
		table.NewColumn("REDIS_CODE_STR", plainText),
	}
	tableName, err := s.getTableNameByCmdName(cmdName)
	if err != nil {
		return "", err
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
