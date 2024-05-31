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
	"strconv"
	"strings"
	"time"

	"github.com/oceanbase/obkv-table-client-go/client/option"
	"github.com/oceanbase/obkv-table-client-go/table"

	"github.com/oceanbase/modis/util"
)

/*
strings table model:
	create table modis_string_table(
		db bigint not null,
		rkey varbinary(1024) not null,
		value varbinary(1024) not null,
		expire_ts timestamp(6) default null,
		primary key(db, rkey)) TTL(expire_ts + INTERVAL 0 SECOND)
		partition by key(db, rkey) partitions 3;
*/

const (
	stringTableName = "modis_string_table"
)

// Get value by key. Return value if exists, nil if not exists
func (s *Storage) Get(ctx context.Context, db int64, key []byte) ([]byte, error) {
	tableName := stringTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}

	// Execute
	selectColumns := []string{valueColumnName}
	res, err := s.cli.Get(ctx, tableName, rowKey, selectColumns)
	if err != nil {
		return nil, err
	}

	// Return value if exists, nil if not exists
	if res.Value(valueColumnName) != nil {
		return res.Value(valueColumnName).([]byte), nil
	} else {
		return nil, nil
	}
}

// GetSet replace old value with new value, and return old value. If key not exist, set new value and return nil.
// Note that we do not guarantee the atomicity of queries and overwrites.
func (s *Storage) GetSet(ctx context.Context, db int64, key []byte, value []byte) ([]byte, error) {
	tableName := stringTableName

	// Check whether the key exists firstly
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}
	mutates := []*table.Column{
		table.NewColumn(valueColumnName, value),
	}

	selectColumns := []string{valueColumnName}
	res, err := s.cli.Get(ctx, tableName, rowKey, selectColumns)
	if err != nil {
		return nil, err
	}

	// Not exist, set new value and return nil
	notExists := res.IsEmptySet()
	if notExists {
		_, err = s.cli.InsertOrUpdate(ctx, tableName, rowKey, mutates)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	// Exist, do replace and return old value
	_, err = s.cli.Replace(ctx, tableName, rowKey, mutates)
	if err != nil {
		return nil, err
	}
	return res.Value(valueColumnName).([]byte), nil
}

// MGet obtain key-value pairs in batches. If keys do not exist, null is returned.
func (s *Storage) MGet(ctx context.Context, db int64, keys [][]byte) ([][]byte, error) {
	tableName := stringTableName
	batchExecutor := s.cli.NewBatchExecutor(tableName)

	// Add get operations
	selectColumns := []string{valueColumnName}
	for _, key := range keys {
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
		}

		err := batchExecutor.AddGetOp(rowKey, selectColumns)
		if err != nil {
			return nil, err
		}
	}

	// Execute
	res, err := batchExecutor.Execute(ctx)
	if err != nil {
		return nil, err
	}

	// Construct return result
	returnValues := make([][]byte, 0, res.Size())
	for i := 0; i < res.Size(); i++ {
		if res.GetResults()[i].Value(valueColumnName) != nil {
			returnValues = append(returnValues, res.GetResults()[i].Value(valueColumnName).([]byte))
		} else {
			returnValues = append(returnValues, nil)
		}
	}

	return returnValues, nil
}

// MSet set key pairs in batches. If the key already exists, the old value is overwritten.
// Returns the number of keys successfully set
func (s *Storage) MSet(ctx context.Context, db int64, kv map[string][]byte) (int, error) {
	tableName := stringTableName
	batchExecutor := s.cli.NewBatchExecutor(tableName)

	// Add insert operations
	for key, value := range kv {
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
		}

		mutates := []*table.Column{
			table.NewColumn(valueColumnName, value),
		}

		err := batchExecutor.AddInsertOrUpdateOp(rowKey, mutates)
		if err != nil {
			return -1, err
		}
	}

	// Execute
	res, err := batchExecutor.Execute(ctx)
	if err != nil {
		return -1, err
	}

	return res.Size(), nil
}

// PSetEx set the value and expiration time (in milliseconds), update key if the key already exists.
func (s *Storage) PSetEx(ctx context.Context, db int64, key []byte, expireTime uint64, value []byte) error {
	tableName := stringTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}

	// Set other columns
	mutates := []*table.Column{
		table.NewColumn(valueColumnName, value),
		table.NewColumn(expireColumnName, table.TimeStamp(time.Now().Local().Add(time.Duration(expireTime)))),
	}

	// Execute
	_, err := s.cli.InsertOrUpdate(ctx, tableName, rowKey, mutates)
	if err != nil {
		return err
	}

	return nil
}

// Set the value of the specified key, insert if it does not exist and update if it does.
func (s *Storage) Set(ctx context.Context, db int64, key []byte, value []byte) error {
	tableName := stringTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}

	// Set other columns
	mutates := []*table.Column{
		table.NewColumn(valueColumnName, value),
	}

	// Execute
	_, err := s.cli.InsertOrUpdate(ctx, tableName, rowKey, mutates)
	if err != nil {
		return err
	}
	return nil
}

// SetEx set the value and expiration time (in second), update key if the key already exists.
func (s *Storage) SetEx(ctx context.Context, db int64, key []byte, expireTime uint64, value []byte) error {
	tableName := stringTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}

	// Set other columns
	mutates := []*table.Column{
		table.NewColumn(valueColumnName, value),
		table.NewColumn(expireColumnName, table.TimeStamp(time.Now().Local().Add(time.Duration(expireTime)))),
	}

	// Execute
	_, err := s.cli.InsertOrUpdate(ctx, tableName, rowKey, mutates)
	if err != nil {
		return err
	}

	return nil
}

// SetNx set a key-value pair, returning 0 if the key already exists and setting a value if the key does not exist.
func (s *Storage) SetNx(ctx context.Context, db int64, key []byte, value []byte) (int, error) {
	tableName := stringTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}

	// Set other columns
	mutates := []*table.Column{
		table.NewColumn(valueColumnName, value),
	}

	// Execute, return 0 if key exist, return 1 if key not exist.
	_, err := s.cli.Insert(ctx, tableName, rowKey, mutates)
	if err != nil {
		errString := err.Error()
		errMsg := "errCode:-5024"
		if strings.Contains(errString, errMsg) {
			return 0, nil
		} else {
			return -1, err
		}
	} else {
		return 1, nil
	}
}

// Append appends a string to the value of the key. Returns the length of the final value.
func (s *Storage) Append(ctx context.Context, db int64, key []byte, value []byte) (int, error) {
	tableName := stringTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}

	// Set other columns
	mutates := []*table.Column{
		table.NewColumn(valueColumnName, value),
	}

	// Execute
	res, err := s.cli.Append(ctx, tableName, rowKey, mutates, option.WithReturnAffectedEntity(true))
	if err != nil {
		return -1, err
	}

	return len(util.BytesToString(res.Value(valueColumnName).([]byte))), nil
}

// IncrBy Add value from the value of the key.
// If the key does not exist, value is written and value is returned
// Returns the value add value when key is present;
func (s *Storage) IncrBy(ctx context.Context, db int64, key []byte, value []byte) (int64, error) {
	tableName := stringTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}

	// Set other columns
	mutates := []*table.Column{
		table.NewColumn(valueColumnName, value),
	}

	// Execute
	res, err := s.cli.Increment(ctx, tableName, rowKey, mutates, option.WithReturnAffectedEntity(true))
	if err != nil {
		return -1, err
	}

	// Convert result(string type) to int
	resByte := res.Value(valueColumnName).([]byte)
	resByte = SimplifyNumber(resByte)
	num, err := strconv.ParseInt(util.BytesToString(resByte), 10, 64)
	if err != nil {
		return -1, err
	}

	return num, nil
}

// IncrByFloat Add value from the value of the key.
// If the key does not exist, value is written and value is returned
// Returns the value add value when key is present;
func (s *Storage) IncrByFloat(ctx context.Context, db int64, key []byte, value []byte) (float64, error) {
	tableName := stringTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}

	// Set other columns
	mutates := []*table.Column{
		table.NewColumn(valueColumnName, value),
	}

	// Execute
	res, err := s.cli.Increment(ctx, tableName, rowKey, mutates, option.WithReturnAffectedEntity(true))
	if err != nil {
		return -1, err
	}

	// Convert result(string type) to int
	resByte := res.Value(valueColumnName).([]byte)
	resByte = SimplifyNumber(resByte)
	f64, err := strconv.ParseFloat(util.BytesToString(resByte), 64)
	if err != nil {
		return -1, err
	}

	return f64, nil
}

// SetBit set the bit value of the specified offset position in the value of the specified key.
func (s *Storage) SetBit(ctx context.Context, db int64, key []byte, offset int, value int) (int, error) {
	tableName := stringTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}

	// Get value
	selectColumns := []string{valueColumnName}
	res, err := s.cli.Get(ctx, tableName, rowKey, selectColumns)
	if err != nil {
		return -1, err
	}

	// Select value
	var bytes []byte
	var oldBitVal byte
	if res.Value(valueColumnName) != nil {
		bytes = res.Value(valueColumnName).([]byte)
	} else {
		bytes = make([]byte, (offset/8 + 1))
	}
	oldBitVal, err = setBit(bytes, offset, byte(value))
	if err != nil {
		return -1, err
	}

	// Set value
	mutates := []*table.Column{
		table.NewColumn(valueColumnName, bytes),
	}
	_, err = s.cli.InsertOrUpdate(ctx, tableName, rowKey, mutates)
	if err != nil {
		return -1, err
	}

	return int(oldBitVal), nil
}

// GetBit get the bit value of the specified offset position in the value of the specified key.
func (s *Storage) GetBit(ctx context.Context, db int64, key []byte, offset int) (byte, error) {
	tableName := stringTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}

	// Get value
	selectColumns := []string{valueColumnName}
	res, err := s.cli.Get(ctx, tableName, rowKey, selectColumns)
	if err != nil {
		return 0, err
	}

	// Not exists, return 0
	value := res.Value(valueColumnName)
	if value == nil {
		return 0, nil
	}

	// Exist, return bitVal
	bitVal, err := getBit(res.Value(valueColumnName).([]byte), offset)
	if err != nil {
		return 0, err
	}

	return bitVal, nil
}

// stringExists check the number of keys that exist in string table
func (s *Storage) stringExists(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	var num int64 = 0
	values, err := s.MGet(ctx, db, keys)
	if err != nil {
		return 0, err
	}

	for _, value := range values {
		if value != nil {
			num += 1
		}
	}
	return num, nil
}

// deleteString delete string table
func (s *Storage) deleteString(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	tableName := stringTableName
	batchExecutor := s.cli.NewBatchExecutor(tableName)

	// Add delete operations
	for _, key := range keys {
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
		}

		err := batchExecutor.AddDeleteOp(rowKey)
		if err != nil {
			return 0, err
		}
	}

	// Execute
	res, err := batchExecutor.Execute(ctx)
	if err != nil {
		return 0, err
	}

	// Statistics deleted rows
	var deleteNum int64
	for i := 0; i < res.Size(); i++ {
		deleteNum += res.GetResults()[i].AffectedRows()
	}

	return deleteNum, nil
}

// expireString expire string table
func (s *Storage) expireString(ctx context.Context, db int64, key []byte, expire_ts table.TimeStamp) (int, error) {
	tableName := stringTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}

	// Set other columns
	mutates := []*table.Column{
		table.NewColumn(expireColumnName, expire_ts),
	}

	// Execute
	affectedRows, err := s.cli.Update(ctx, tableName, rowKey, mutates)
	if err != nil {
		return 0, err
	}

	return int(affectedRows), nil
}

// persistString persist string table
func (s *Storage) persistString(ctx context.Context, db int64, key []byte) (int, error) {
	tableName := stringTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}

	// Set other columns
	mutates := []*table.Column{
		table.NewColumn(expireColumnName, nil),
	}

	// Execute
	affectedRows, err := s.cli.Update(ctx, tableName, rowKey, mutates)
	if err != nil {
		return 0, err
	}

	return int(affectedRows), nil
}

// ttlString get expire time of string table
func (s *Storage) ttlString(ctx context.Context, db int64, key []byte) (time.Duration, error) {
	tableName := stringTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
	}

	// Set select columns
	selectColumns := []string{expireColumnName}

	// Execute
	res, err := s.cli.Get(ctx, tableName, rowKey, selectColumns)
	if err != nil {
		return 0, err
	}

	if res.IsEmptySet() {
		return -2, nil
	}

	if res.Value(expireColumnName) == nil {
		return -1, nil
	}

	expire := res.Value(expireColumnName)
	sub := time.Until(expire.(time.Time))
	return sub, nil
}
