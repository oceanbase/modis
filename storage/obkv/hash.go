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
	"github.com/pkg/errors"

	"github.com/oceanbase/modis/util"
)

/*
hashes table model:
	create table modis_hash_table(
	  db bigint not null,
	  rkey varbinary(1024) not null,
	  field varbinary(1024) not null,
	  value varbinary(1024) not null,
	  expire_ts timestamp(6) default null,
	  primary key(db, rkey, field)) TTL(expire_ts + INTERVAL 0 SECOND)
      partition by key(db, rkey) partitions 3;
*/

const (
	hashTableName   = "modis_hash_table"
	fieldColumnName = "field"
)

// HSet hash set
func (s *Storage) HSet(ctx context.Context, db int64, key []byte, fieldValue map[string][]byte) (int, error) {
	tableName := hashTableName

	// Create batch executor
	batchExecutor := s.cli.NewBatchExecutor(tableName)

	// Add operations
	for field, value := range fieldValue {
		// Set rowKey columns
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
			table.NewColumn(fieldColumnName, field),
		}

		// Set other columns
		mutates := []*table.Column{
			table.NewColumn(valueColumnName, value),
		}

		batchExecutor.AddInsertOrUpdateOp(rowKey, mutates)
	}

	// Execute
	res, err := batchExecutor.Execute(ctx)
	if err != nil {
		return -1, err
	}

	// Static insert size
	insertSize := 0
	for _, singleResult := range res.GetResults() {
		if !singleResult.IsInsertOrUpdateDoUpdate() {
			insertSize++
		}
	}

	return insertSize, nil
}

// HGet hash get
func (s *Storage) HGet(ctx context.Context, db int64, key []byte, field []byte) ([]byte, error) {
	tableName := hashTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(fieldColumnName, field),
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

// HDel hash multi delete
func (s *Storage) HDel(ctx context.Context, db int64, key []byte, fields [][]byte) (int64, error) {
	tableName := hashTableName

	if len(fields) == 0 {
		return 0, nil
	}

	// Create batch executor
	batchExecutor := s.cli.NewBatchExecutor(tableName)

	// Add operations
	for _, field := range fields {
		// Set rowKey columns
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
			table.NewColumn(fieldColumnName, field),
		}

		batchExecutor.AddDeleteOp(rowKey)
	}

	// Execute
	res, err := batchExecutor.Execute(ctx)
	if err != nil {
		return 0, err
	}

	// Handle result
	var deleteNum int64
	for i := 0; i < res.Size(); i++ {
		singleRes := res.GetResults()[i]
		if singleRes == nil {
			return 0, errors.Errorf("single result is null")
		}
		deleteNum += singleRes.AffectedRows()
	}

	return deleteNum, nil
}

// HGetAll hash get all
func (s *Storage) HGetAll(ctx context.Context, db int64, key []byte) ([][]byte, error) {
	tableName := hashTableName

	// Prepare key range
	startRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(fieldColumnName, table.Min),
	}
	endRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(fieldColumnName, table.Max),
	}
	keyRanges := []*table.RangePair{table.NewRangePair(startRowKey, endRowKey)}

	// Create query
	selectColumns := []string{fieldColumnName, valueColumnName}
	resSet, err := s.cli.Query(
		ctx,
		tableName,
		keyRanges,
		option.WithQuerySelectColumns(selectColumns),
	)
	if err != nil {
		return nil, err
	}
	defer resSet.Close()

	values := make([][]byte, 0, 128)

	// Get next row
	res, err := resSet.Next()
	for ; res != nil && err == nil; res, err = resSet.Next() {
		values = append(values, res.Value(fieldColumnName).([]byte))
		values = append(values, res.Value(valueColumnName).([]byte))
	}
	if err != nil {
		return nil, err
	}

	return values, nil
}

// HKeys hash keys
func (s *Storage) HKeys(ctx context.Context, db int64, key []byte) ([][]byte, error) {
	tableName := hashTableName

	// Prepare key range
	startRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(fieldColumnName, table.Min),
	}
	endRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(fieldColumnName, table.Max),
	}
	keyRanges := []*table.RangePair{table.NewRangePair(startRowKey, endRowKey)}

	// Create query
	selectColumns := []string{fieldColumnName}
	resSet, err := s.cli.Query(
		ctx,
		tableName,
		keyRanges,
		option.WithQuerySelectColumns(selectColumns),
	)
	if err != nil {
		return nil, err
	}
	defer resSet.Close()

	values := make([][]byte, 0, 128)

	// Get next row
	res, err := resSet.Next()
	for ; res != nil && err == nil; res, err = resSet.Next() {
		values = append(values, res.Value(fieldColumnName).([]byte))
	}
	if err != nil {
		return nil, err
	}

	return values, nil
}

// HVals hash values
func (s *Storage) HVals(ctx context.Context, db int64, key []byte) ([][]byte, error) {
	tableName := hashTableName

	// Prepare key range
	startRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(fieldColumnName, table.Min),
	}
	endRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(fieldColumnName, table.Max),
	}
	keyRanges := []*table.RangePair{table.NewRangePair(startRowKey, endRowKey)}

	// Create query
	selectColumns := []string{valueColumnName}
	resSet, err := s.cli.Query(
		ctx,
		tableName,
		keyRanges,
		option.WithQuerySelectColumns(selectColumns),
	)
	if err != nil {
		return nil, err
	}
	defer resSet.Close()

	values := make([][]byte, 0, 128)

	// Get next row
	res, err := resSet.Next()
	for ; res != nil && err == nil; res, err = resSet.Next() {
		values = append(values, res.Value(valueColumnName).([]byte))
	}
	if err != nil {
		return nil, err
	}

	return values, nil
}

// HLen hash length
func (s *Storage) HLen(ctx context.Context, db int64, key []byte) (int64, error) {
	tableName := hashTableName

	// Prepare key range
	startRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(fieldColumnName, table.Min),
	}
	endRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(fieldColumnName, table.Max),
	}
	keyRanges := []*table.RangePair{table.NewRangePair(startRowKey, endRowKey)}

	// Create aggregation executor
	aggExecutor := s.cli.NewAggExecutor(tableName, keyRanges).Count()

	// Execute
	res, err := aggExecutor.Execute(ctx)
	if err != nil {
		return 0, err
	}

	return res.Value("count(*)").(int64), nil
}

// HSetNx hash set if not exist
func (s *Storage) HSetNx(ctx context.Context, db int64, key []byte, field []byte, value []byte) (int, error) {
	tableName := hashTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(fieldColumnName, field),
	}

	// Set other columns
	mutates := []*table.Column{
		table.NewColumn(valueColumnName, value),
	}

	// Execute
	_, err := s.cli.Insert(ctx, tableName, rowKey, mutates)
	if err != nil {
		errString := err.Error()
		errMsg := "errCode:-5024"
		if strings.Contains(errString, errMsg) {
			return 0, nil
		} else {
			return -1, err
		}
	}
	return 1, nil
}

// HMGet hash multi get
func (s *Storage) HMGet(ctx context.Context, db int64, key []byte, fields [][]byte) ([][]byte, error) {
	tableName := hashTableName

	// Create batch executor
	batchExecutor := s.cli.NewBatchExecutor(tableName)

	// Add operations
	selectColumns := []string{valueColumnName}
	for _, field := range fields {
		// Set rowKey columns
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
			table.NewColumn(fieldColumnName, field),
		}

		batchExecutor.AddGetOp(rowKey, selectColumns)
	}

	// Execute
	res, err := batchExecutor.Execute(ctx)
	if err != nil {
		return nil, err
	}

	// Handle result
	values := make([][]byte, 0, res.Size())
	for i := 0; i < res.Size(); i++ {
		singleRes := res.GetResults()[i]
		if singleRes == nil {
			return nil, errors.Errorf("single result is null")
		}
		value := singleRes.Value(valueColumnName)
		if value == nil {
			values = append(values, nil)
		} else {
			values = append(values, value.([]byte))
		}
	}

	return values, nil
}

// HIncrBy Add value from the value of the key.
// If the key does not exist, value is written and value is returned
// Returns the value add value when key is present;
func (s *Storage) HIncrBy(ctx context.Context, db int64, key []byte, field []byte, value []byte) (int64, error) {
	tableName := hashTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(fieldColumnName, field),
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

	resByte := res.Value(valueColumnName).([]byte)
	num, err := strconv.ParseInt(string(resByte), 10, 64)
	if err != nil {
		return -1, err
	}

	return num, nil
}

// HIncrByFloat Add value from the value of the key.
// If the key does not exist, value is written and value is returned
// Returns the value add value when key is present;
func (s *Storage) HIncrByFloat(ctx context.Context, db int64, key []byte, field []byte, value []byte) (float64, error) {
	tableName := hashTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(fieldColumnName, field),
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
	f64, err := strconv.ParseFloat(util.BytesToString(res.Value(valueColumnName).([]byte)), 64)
	if err != nil {
		return -1, err
	}

	return f64, nil
}

// hashExists check the number of keys that exist in hash table
func (s *Storage) hashExists(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	var existNum int64
	for _, key := range keys {
		num, err := s.HLen(ctx, db, key)
		if err != nil {
			return 0, err
		}

		if num != 0 {
			existNum += 1
		}
	}

	return existNum, nil
}

// deleteHash delete hash table
func (s *Storage) deleteHash(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	var deleteNum int64
	for _, key := range keys {
		// Get fields by key
		fields, err := s.HKeys(ctx, db, key)
		if err != nil {
			return 0, err
		}

		// Delete
		num, err := s.HDel(ctx, db, key, fields)
		if err != nil {
			return 0, err
		}
		deleteNum += num
	}

	return deleteNum, nil
}

// expireHash expire hash table
func (s *Storage) expireHash(ctx context.Context, db int64, key []byte, expire_ts table.TimeStamp) (int, error) {
	tableName := hashTableName
	var res = 0

	// 1. Get all fields
	fields, err := s.HKeys(ctx, db, key)
	if err != nil {
		return 0, err
	}

	// 2. Expire all fields(maybe use batch is better)
	for _, field := range fields {
		// Set rowKey columns
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
			table.NewColumn(fieldColumnName, field),
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
		if affectedRows != 0 && res != 1 {
			res = 1
		}
	}

	return res, nil
}

// persistHash persist hash table
func (s *Storage) persistHash(ctx context.Context, db int64, key []byte) (int, error) {
	tableName := hashTableName
	var res = 0

	// 1. Get all fields
	fields, err := s.HKeys(ctx, db, key)
	if err != nil {
		return 0, err
	}

	// 2. Persist all fields(maybe use batch is better)
	for _, field := range fields {
		// Set rowKey columns
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
			table.NewColumn(fieldColumnName, field),
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
		if affectedRows != 0 && res != 1 {
			res = 1
		}
	}

	return res, nil
}

// ttlHash get expire time of hash table
func (s *Storage) ttlHash(ctx context.Context, db int64, key []byte) (time.Duration, error) {
	tableName := hashTableName
	batchExecutor := s.cli.NewBatchExecutor(tableName)

	// 1. Get all fields
	fields, err := s.HKeys(ctx, db, key)
	if err != nil {
		return 0, err
	}

	if len(fields) == 0 {
		return -2, nil
	}

	// 2. Get all fields expire time
	selectColumns := []string{expireColumnName}
	for _, field := range fields {
		// Set rowKey columns
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
			table.NewColumn(fieldColumnName, field),
		}

		// Add get operation
		err = batchExecutor.AddGetOp(rowKey, selectColumns)
		if err != nil {
			return 0, err
		}
	}

	// 3. Execute
	res, err := batchExecutor.Execute(ctx)
	if err != nil {
		return 0, err
	}

	if res.IsEmptySet() {
		return -2, nil
	}

	if res.GetResults()[0].Value(expireColumnName) == nil {
		return -1, nil
	}

	expire := res.GetResults()[0].Value(expireColumnName)
	sub := expire.(time.Time).Sub(time.Now())
	return sub, nil
}
