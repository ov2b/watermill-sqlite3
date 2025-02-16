package sqlite3

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
)

type OffsetsAdapter struct {
	GenerateMessagesOffsetsTableName func(topic string) string
}

func (a OffsetsAdapter) SchemaInitializingQueries(topic string) []sql.Query {
	schemaQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
	consumer_group TEXT NOT NULL,
	offset_acked INTEGER,
	offset_consumed INTEGER NOT NULL,
	PRIMARY KEY(consumer_group)
)`, a.MessagesOffsetsTable(topic))

	return []sql.Query{
		{
			Query: schemaQuery,
		},
	}
}
func (a OffsetsAdapter) AckMessageQuery(topic string, row sql.Row, consumerGroup string) sql.Query {
	ackQuery := fmt.Sprintf(`INSERT INTO "%s" (offset_consumed, offset_acked, consumer_group)
	VALUES (?, ?, ?) 
	ON CONFLICT(consumer_group) 
	DO UPDATE SET 
		offset_consumed = excluded.offset_consumed, 
		offset_acked = excluded.offset_acked`, a.MessagesOffsetsTable(topic))

	return sql.Query{
		Query: ackQuery,
		Args:  []any{row.Offset, row.Offset, consumerGroup},
	}
}

func (a OffsetsAdapter) NextOffsetQuery(topic, consumerGroup string) sql.Query {
	nextOffsetQuery := fmt.Sprintf(`SELECT 
	COALESCE(
	(
		SELECT offset_acked
		FROM %s
		WHERE consumer_group=?
	), 0)`, a.MessagesOffsetsTable(topic))

	return sql.Query{
		Query: nextOffsetQuery,
		Args:  []any{consumerGroup},
	}
}

func (a OffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if a.GenerateMessagesOffsetsTableName != nil {
		return a.GenerateMessagesOffsetsTableName(topic)
	}
	return fmt.Sprintf("watermill_offsets_%s", topic)
}

func (a OffsetsAdapter) ConsumedMessageQuery(topic string, row sql.Row, consumerGroup string, _ []byte) sql.Query {
	ackQuery := fmt.Sprintf(`INSERT INTO "%s" (offset_consumed, consumer_group)
	VALUES (?, ?) 
	ON CONFLICT(consumer_group) 
	DO UPDATE SET 
		offset_consumed = excluded.offset_consumed`, a.MessagesOffsetsTable(topic))
	return sql.Query{
		Query: ackQuery,
		Args:  []any{row.Offset, consumerGroup},
	}
}

func (a OffsetsAdapter) BeforeSubscribingQueries(topic, consumerGroup string) []sql.Query {
	return nil
}
