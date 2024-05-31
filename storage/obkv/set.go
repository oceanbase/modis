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
	"slices"
	"time"

	"github.com/oceanbase/obkv-table-client-go/client/option"
	"github.com/oceanbase/obkv-table-client-go/table"
	"github.com/pkg/errors"
)

/*
sets table model:
	create table modis_set_table(
	  db bigint not null,
	  rkey varbinary(1024) not null,
	  member varbinary(1024) not null,
	  expire_ts timestamp(6) default null,
	  primary key(db, rkey, member)) TTL(expire_ts + INTERVAL 0 SECOND)
	  partition by key(db, rkey) partitions 3;
*/

const (
	setTableName     = "modis_set_table"
	memberColumnName = "member"
)

// SAdd add member to set
func (s *Storage) SAdd(ctx context.Context, db int64, key []byte, members [][]byte) (int64, error) {
	tableName := setTableName

	// Create batch executor
	batchExecutor := s.cli.NewBatchExecutor(tableName)

	// Add operations
	for _, member := range members {
		// Set rowKey columns
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
			table.NewColumn(memberColumnName, member),
		}

		// Set normal columns
		mutates := []*table.Column{
			table.NewColumn(expireColumnName, nil),
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

	// todo: 返回写入的数量
	return int64(res.Size()), nil
}

// SCard get the size of the key
func (s *Storage) SCard(ctx context.Context, db int64, key []byte) (int64, error) {
	tableName := setTableName

	// Prepare key range
	startRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(memberColumnName, table.Min),
	}
	endRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(memberColumnName, table.Max),
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

// SRem remove the member from the key
func (s *Storage) SRem(ctx context.Context, db int64, key []byte, members [][]byte) (int64, error) {
	tableName := setTableName

	if len(members) == 0 {
		return 0, nil
	}

	// Create batch executor
	batchExecutor := s.cli.NewBatchExecutor(tableName)

	// Add operations
	for _, member := range members {
		// Set rowKey columns
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
			table.NewColumn(memberColumnName, member),
		}

		batchExecutor.AddDeleteOp(rowKey, nil)
	}

	// Execute
	res, err := batchExecutor.Execute(ctx)
	if err != nil {
		return -1, err
	}

	// todo: 返回写入的数量
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

// SIsmember check if is a member of the key
func (s *Storage) SIsmember(ctx context.Context, db int64, key []byte, member []byte) (int, error) {
	tableName := setTableName

	// Set rowKey columns
	rowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(memberColumnName, member),
	}

	// Execute
	selectColumns := []string{memberColumnName}
	res, err := s.cli.Get(ctx, tableName, rowKey, selectColumns)
	if err != nil {
		return 0, err
	}

	// Return 1 if exists, 0 if not exists
	if res.Value(memberColumnName) != nil {
		return 1, nil
	} else {
		return 0, nil
	}
}

// SMembers get all member
func (s *Storage) SMembers(ctx context.Context, db int64, key []byte) ([][]byte, error) {
	tableName := setTableName

	// Prepare key range
	startRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(memberColumnName, table.Min),
	}
	endRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(memberColumnName, table.Max),
	}
	keyRanges := []*table.RangePair{table.NewRangePair(startRowKey, endRowKey)}

	// Create query
	selectColumns := []string{memberColumnName}
	resSet, err := s.cli.Query(
		ctx,
		tableName,
		keyRanges,
		option.WithQuerySelectColumns(selectColumns),
		option.WithQueryScanOrder(table.KeepOrder),
	)
	if err != nil {
		return nil, err
	}
	defer resSet.Close()

	values := make([][]byte, 0, 128)

	// Get next row
	res, err := resSet.Next()
	for ; res != nil && err == nil; res, err = resSet.Next() {
		values = append(values, res.Value(memberColumnName).([]byte))
	}
	if err != nil {
		return nil, err
	}

	return values, nil
}

// Smove move member from src key to dest key
func (s *Storage) Smove(ctx context.Context, db int64, src []byte, dst []byte, member []byte) (int, error) {
	tableName := setTableName

	// 1. Delete from src key
	// Set rowKey columns
	srcRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, src),
		table.NewColumn(memberColumnName, member),
	}

	// Execute delete
	affectedRows, err := s.cli.Delete(ctx, tableName, srcRowKey)
	if err != nil {
		return 0, err
	}

	// not exist, return 0
	if affectedRows == 0 {
		return 0, nil
	}

	// 2. Insert to dest key
	// Set rowKey columns
	dstRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, dst),
		table.NewColumn(memberColumnName, member),
	}

	// Set normal columns
	mutates := []*table.Column{
		table.NewColumn(expireColumnName, nil),
	}

	// Execute insert
	_, err = s.cli.Insert(ctx, tableName, dstRowKey, mutates)
	if err != nil {
		return 0, err
	}

	return 1, nil
}

// SPop randomly delete count members
func (s *Storage) SPop(ctx context.Context, db int64, key []byte, count int) ([][]byte, error) {
	tableName := setTableName
	members, err := s.SRandMember(ctx, db, key, count)
	if err != nil {
		return nil, err
	}

	// 2. Delete
	for i := 0; i < len(members); i++ {
		// Set rowKey columns
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
			table.NewColumn(memberColumnName, members[i]),
		}

		// Execute
		_, err := s.cli.Delete(ctx, tableName, rowKey)
		if err != nil {
			return nil, err
		}
	}

	return members, nil
}

// SRandMember randomly get count members
func (s *Storage) SRandMember(ctx context.Context, db int64, key []byte, count int) ([][]byte, error) {
	members := make([][]byte, 0, count)
	if count == 0 {
		return members, nil
	}

	// Prepare key range
	startRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(memberColumnName, table.Min),
	}
	endRowKey := []*table.Column{
		table.NewColumn(dbColumnName, db),
		table.NewColumn(keyColumnName, key),
		table.NewColumn(memberColumnName, table.Max),
	}
	keyRanges := []*table.RangePair{table.NewRangePair(startRowKey, endRowKey)}

	// Create query
	tableName := setTableName
	selectColumns := []string{memberColumnName}
	resSet, err := s.cli.Query(
		ctx,
		tableName,
		keyRanges,
		option.WithQuerySelectColumns(selectColumns),
		option.WithQueryScanOrder(table.KeepOrder),
	)
	if err != nil {
		return nil, err
	}
	defer resSet.Close()

	cnt, err := s.SCard(ctx, db, key)
	if err != nil {
		return nil, err
	}
	res, err := resSet.Next()
	var idxArr []int
	if int64(count) < cnt {
		idxArr = getRandomArray(0, int(cnt), count)
		slices.Sort(idxArr)
		curTargetIdx := 0
		curIdx := 0
		for ; res != nil && err == nil && curTargetIdx < len(idxArr); res, err = resSet.Next() {
			if idxArr[curTargetIdx] == curIdx {
				members = append(members, res.Value(memberColumnName).([]byte))
				curTargetIdx++
			}
			curIdx += 1
		}
	} else {
		for ; res != nil && err == nil; res, err = resSet.Next() {
			members = append(members, res.Value(memberColumnName).([]byte))
		}
	}

	if err != nil {
		return nil, err
	}

	return members, nil
}

// setExists check the number of keys that exist in set table
func (s *Storage) setExists(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	var existNum int64
	for _, key := range keys {
		num, err := s.SCard(ctx, db, key)
		if err != nil {
			return 0, err
		}

		if num > 0 {
			existNum += 1
		}
	}

	return existNum, nil
}

// deleteSet delete set table
func (s *Storage) deleteSet(ctx context.Context, db int64, keys [][]byte) (int64, error) {
	var deleteNum int64
	for _, key := range keys {
		// Get members by key
		members, err := s.SMembers(ctx, db, key)
		if err != nil {
			return 0, err
		}

		// Delete
		num, err := s.SRem(ctx, db, key, members)
		if err != nil {
			return 0, err
		}
		deleteNum += num
	}

	return deleteNum, nil
}

// expireSet expire set table
func (s *Storage) expireSet(ctx context.Context, db int64, key []byte, expire_ts table.TimeStamp) (int, error) {
	tableName := setTableName
	var res = 0

	// 1. Get all members
	members, err := s.SMembers(ctx, db, key)
	if err != nil {
		return 0, err
	}

	// 2. Expire all members(maybe use batch is better)
	for _, member := range members {
		// Set rowKey columns
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
			table.NewColumn(memberColumnName, member),
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

// persistSet persist set table
func (s *Storage) persistSet(ctx context.Context, db int64, key []byte) (int, error) {
	tableName := setTableName
	var res = 0

	// 1. Get all members
	members, err := s.SMembers(ctx, db, key)
	if err != nil {
		return 0, err
	}

	// 2. Expire all members(maybe use batch is better)
	for _, member := range members {
		// Set rowKey columns
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
			table.NewColumn(memberColumnName, member),
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

// ttlSet get expire time of set table
func (s *Storage) ttlSet(ctx context.Context, db int64, key []byte) (time.Duration, error) {
	tableName := setTableName
	batchExecutor := s.cli.NewBatchExecutor(tableName)

	// 1. Get all members
	members, err := s.SMembers(ctx, db, key)
	if err != nil {
		return 0, err
	}

	if len(members) == 0 {
		return -2, nil
	}

	// 2. get all fields
	selectColumns := []string{expireColumnName}
	for _, member := range members {
		// Set rowKey columns
		rowKey := []*table.Column{
			table.NewColumn(dbColumnName, db),
			table.NewColumn(keyColumnName, key),
			table.NewColumn(memberColumnName, member),
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
