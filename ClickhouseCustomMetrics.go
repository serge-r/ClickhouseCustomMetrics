package main

import (
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	"log"
	"os"
)

type merges struct {
		Database                 string   `db:"database"`
		Table                    string   `db:"table"`
		Elapsed                  float64  `db:"elapsed"`
		Progress                 float64  `db:"progress"`
		NumParts                 string   `db:"num_parts"`
		SourcePartNames          []string `db:"source_part_names"`
		ResultPartName           string   `db:"result_part_name"`
		SourcePartPaths          []string `db:"source_part_paths"`
		ResultPartPath           string   `db:"result_part_path"`
		PartitionID              string   `db:"partition_id"`
		IsMutation               int      `db:"is_mutation"`
		TotalSizeBytesCompressed string   `db:"total_size_bytes_compressed"`
		TotalSizeMarks           string   `db:"total_size_marks"`
		BytesReadUncompressed    string   `db:"bytes_read_uncompressed"`
		RowsRead                 string   `db:"rows_read"`
		BytesWrittenUncompressed string   `db:"bytes_written_uncompressed"`
		RowsWritten              string   `db:"rows_written"`
		ColumnsWritten           string   `db:"columns_written"`
		MemoryUsage              string   `db:"memory_usage"`
		ThreadID                 string   `db:"thread_id"`
		MergeType                string   `db:"merge_type"`
		MergeAlgorithm           string   `db:"merge_algorithm"`
}

func main() {
	var data []merges
	connstring := os.Getenv("CLICKHOUSE_CONN_STRING")
	if len(connstring) == 0 {
		connstring = "tcp://127.0.0.1:9000?debug=false"
	}
	connect, err := sqlx.Open("clickhouse", connstring)
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Select(&data, "SELECT * from system.merges"); err != nil {
		log.Fatal(err)
	}

	for _, entry := range data{
		fmt.Printf("ClickHouseCustomMetrics_merge_rows_read{table=\"%s\",partid=\"%s\",mergetype=\"%s\",mergealgo=\"%s\"} %s\n",
			entry.Table,
			entry.PartitionID,
			entry.MergeType,
			entry.MergeAlgorithm,
			entry.RowsRead)
		fmt.Printf("ClickHouseCustomMetrics_merge_rows_written{table=\"%s\",partid=\"%s\",mergetype=\"%s\",mergealgo=\"%s\"} %s\n",
			entry.Table,
			entry.PartitionID,
			entry.MergeType,
			entry.MergeAlgorithm,
			entry.RowsWritten)
		fmt.Printf("ClickHouseCustomMetrics_merge_columns_written{table=\"%s\",partid=\"%s\",mergetype=\"%s\",mergealgo=\"%s\"} %s\n",
			entry.Table,
			entry.PartitionID,
			entry.MergeType,
			entry.MergeAlgorithm,
			entry.ColumnsWritten)
	}
}
