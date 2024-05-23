package obkv

import (
	"context"
)

type TableInfo struct {
	Keys    int64 // num of keys in db
	Expires int64 // num of keys with ttl in db
}

func (s *Storage) GetTableInfo(ctx context.Context, db int64, tableName string) (*TableInfo, error) {
	// TODO: with multi partitions
	tableInfo := &TableInfo{Keys: 0, Expires: 0}
	// // 1. count keys
	// // Prepare key range
	// startRowKey := []*table.Column{
	// 	table.NewColumn(dbColumnName, db),
	// 	table.NewColumn(keyColumnName, table.Min),
	// }
	// endRowKey := []*table.Column{
	// 	table.NewColumn(dbColumnName, db),
	// 	table.NewColumn(keyColumnName, table.Max),
	// }
	// keyRanges := []*table.RangePair{table.NewRangePair(startRowKey, endRowKey)}

	// // Create aggregation executor
	// aggExecutor := s.cli.NewAggExecutor(tableName, keyRanges).Count()

	// // Execute
	// resSet, err := aggExecutor.ExecuteWithPartition(ctx)
	// if err != nil {
	// 	return nil, err
	// }
	// res, err := resSet.NextBatch()
	// for ; res != nil && err == nil; res, err = resSet.NextBatch() {
	// 	for _, c := range res {
	// 		tableInfo.Keys += c.Value("count(*)").(int64)
	// 	}
	// }

	// // 2. count expires
	// aggOptExecutor := s.cli.NewAggExecutor(
	// 	tableName,
	// 	keyRanges,
	// 	option.WithQueryFilter(filter.CompareVal(filter.IsNotNull, ttlColumnName, nil)),
	// ).Count()
	// resSet, err = aggOptExecutor.ExecuteWithPartition(ctx)
	// if err != nil {
	// 	return nil, err
	// }
	// cnt := 0
	// res, err = resSet.NextBatch()
	// for ; res != nil && err == nil; res, err = resSet.NextBatch() {
	// 	for _, c := range res {
	// 		tableInfo.Keys += c.Value("count(*)").(int64)
	// 		cnt++
	// 	}
	// }
	return tableInfo, nil
}
