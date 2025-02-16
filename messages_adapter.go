package sqlite3

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	stdSQL "database/sql"
	"errors"

	"github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

func defaultInsertArgs(msgs message.Messages) ([]interface{}, error) {
	var args []interface{}
	for _, msg := range msgs {
		metadata, err := json.Marshal(msg.Metadata)
		if err != nil {
			return nil, errors.Join(err, fmt.Errorf("could not marshal metadata into JSON for message %s", msg.UUID))
		}

		args = append(args, msg.UUID, string(msg.Payload), string(metadata))
	}

	return args, nil
}

type SchemaAdapter struct {
	// GenerateMessagesTableName may be used to override how the messages table name is generated.
	GenerateMessagesTableName func(topic string) string

	// SubscribeBatchSize is the number of messages to be queried at once.
	//
	// Higher value, increases a chance of message re-delivery in case of crash or networking issues.
	// 1 is the safest value, but it may have a negative impact on performance when consuming a lot of messages.
	//
	// Default value is 100.
	SubscribeBatchSize int
}

func (s SchemaAdapter) SchemaInitializingQueries(topic string) []sql.Query {
	createMessagesTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	offset INTEGER PRIMARY KEY AUTOINCREMENT,
	uuid TEXT NOT NULL,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	payload TEXT,
	metadata TEXT
);`, s.MessagesTable(topic))

	return []sql.Query{{Query: createMessagesTable}}
}

func (s SchemaAdapter) InsertQuery(topic string, msgs message.Messages) (sql.Query, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO "%s" (uuid, payload, metadata) VALUES %s`,
		s.MessagesTable(topic),
		strings.TrimRight(strings.Repeat(`(?,?,?),`, len(msgs)), ","),
	)

	args, err := defaultInsertArgs(msgs)
	if err != nil {
		return sql.Query{}, err
	}

	return sql.Query{Query: insertQuery, Args: args}, nil
}

func (s SchemaAdapter) batchSize() int {
	if s.SubscribeBatchSize == 0 {
		return 100
	}

	return s.SubscribeBatchSize
}

func (s SchemaAdapter) SelectQuery(topic string, consumerGroup string, offsetsAdapter sql.OffsetsAdapter) sql.Query {
	nextOffsetQuery := offsetsAdapter.NextOffsetQuery(topic, consumerGroup)

	selectQuery := `SELECT offset, uuid, payload, metadata
	FROM "` + s.MessagesTable(topic) + `"
	WHERE offset > (` + nextOffsetQuery.Query + `) ORDER BY offset ASC
	LIMIT ` + strconv.Itoa(s.batchSize())

	return sql.Query{Query: selectQuery, Args: nextOffsetQuery.Args}
}

func (s SchemaAdapter) UnmarshalMessage(row sql.Scanner) (sql.Row, error) {
	r := sql.Row{}
	err := row.Scan(&r.Offset, &r.UUID, &r.Payload, &r.Metadata)
	if err != nil {
		return sql.Row{}, fmt.Errorf("could not scan message row: %w", err)
	}

	msg := message.NewMessage(string(r.UUID), []byte(r.Payload))

	if r.Metadata != nil {
		err = json.Unmarshal([]byte(r.Metadata), &msg.Metadata)
		if err != nil {
			return sql.Row{}, errors.Join(err, errors.New("could not unmarshal metadata as JSON"))
		}
	}

	r.Msg = msg

	return r, nil
}

func (s SchemaAdapter) MessagesTable(topic string) string {
	if s.GenerateMessagesTableName != nil {
		return s.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf("watermill_%s", topic)
}

func (s SchemaAdapter) SubscribeIsolationLevel() stdSQL.IsolationLevel {
	return stdSQL.LevelDefault
}
