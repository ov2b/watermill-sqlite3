package sqlite3

import (
	std_sql "database/sql"
	"encoding/json"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	_ "github.com/mattn/go-sqlite3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessagesAdapter(t *testing.T) {
	db, err := std_sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	sut := SchemaAdapter{}

	queries := sut.SchemaInitializingQueries("test")
	for _, q := range queries {
		_, err := db.Exec(q.Query, q.Args...)
		require.NoError(t, err)
	}

	msgs := message.Messages{
		{UUID: "test1", Metadata: message.Metadata{"foo": "bar"}, Payload: []byte("test_payload_1")},
		{UUID: "test2", Metadata: message.Metadata{"foo": "bar"}, Payload: []byte("test_payload_2")},
	}
	q, err := sut.InsertQuery("test", msgs)
	require.NoError(t, err)

	_, err = db.Exec(q.Query, q.Args...)
	require.NoError(t, err)

	offsets := OffsetsAdapter{}
	for _, q := range offsets.SchemaInitializingQueries("test") {
		db.Exec(q.Query, q.Args...)
	}

	q = sut.SelectQuery("test", "test_consumer", offsets)

	rows, err := db.Query(q.Query, q.Args...)
	require.NoError(t, err)

	for i := 0; rows.Next(); i++ {
		require.NoError(t, rows.Err())

		row, err := sut.UnmarshalMessage(rows)
		require.NoError(t, err)

		assert.Equal(t, msgs[i].UUID, string(row.UUID))
		assert.Equal(t, lo.Must(json.Marshal(msgs[i].Metadata)), row.Metadata)
		assert.Equal(t, msgs[i].Payload, message.Payload(row.Payload))
	}
}
