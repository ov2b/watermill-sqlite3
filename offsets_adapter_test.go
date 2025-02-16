package sqlite3

import (
	std_sql "database/sql"
	"testing"

	"github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestOffsetsAdapter(t *testing.T) {
	db, err := std_sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	sut := OffsetsAdapter{GenerateMessagesOffsetsTableName: func(topic string) string { return "topic_" + topic }}

	queries := sut.SchemaInitializingQueries("test")

	for _, q := range queries {
		_, err := db.Exec(q.Query, q.Args...)
		require.NoError(t, err)
	}

	q := sut.ConsumedMessageQuery("test", sql.Row{Offset: 1}, "test_consumer", nil)
	res, err := db.Exec(q.Query, q.Args...)
	require.NoError(t, err)
	rowsAffected, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowsAffected)

	q = sut.AckMessageQuery("test", sql.Row{Offset: 1}, "test_consumer")
	res, err = db.Exec(q.Query, q.Args...)
	require.NoError(t, err)
	rowsAffected, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowsAffected)

	q = sut.NextOffsetQuery("test", "test_consumer")
	row := db.QueryRow(q.Query, q.Args...)
	require.NoError(t, row.Err())

	var nextOffset int64
	row.Scan(&nextOffset)

	require.Equal(t, int64(1), nextOffset)

}
