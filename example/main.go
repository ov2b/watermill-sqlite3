package main

import (
	"context"
	std_sql "database/sql"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	_ "github.com/mattn/go-sqlite3"
	sqlite3 "github.com/ov2b/watermill-sqlite3"
)

func main() {

	logger := watermill.NewStdLogger(false, false)

	//db, err := std_sql.Open("sqlite3", "file:test.db?_journal=wal&_sync=normal&_busy_timeout=1000")
	db, err := std_sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(1)

	schemaAdapter := sqlite3.SchemaAdapter{
		SubscribeBatchSize: 10,
	}
	for _, q := range schemaAdapter.SchemaInitializingQueries("topic_0") {
		if _, err := db.Exec(q.Query, q.Args...); err != nil {
			panic(err)
		}
	}
	for _, q := range schemaAdapter.SchemaInitializingQueries("topic_1") {
		if _, err := db.Exec(q.Query, q.Args...); err != nil {
			panic(err)
		}
	}

	offsetsAdapter := sqlite3.OffsetsAdapter{}
	for _, q := range offsetsAdapter.SchemaInitializingQueries("topic_0") {
		if _, err := db.Exec(q.Query, q.Args...); err != nil {
			panic(err)
		}
	}
	for _, q := range offsetsAdapter.SchemaInitializingQueries("topic_1") {
		if _, err := db.Exec(q.Query, q.Args...); err != nil {
			panic(err)
		}
	}

	backoff := sqlite3.DecorateBackoffManagerWithResetLatch(sql.NewDefaultBackoffManager(10*time.Second, time.Second))

	sub1, err := sql.NewSubscriber(db, sql.SubscriberConfig{
		ConsumerGroup:    "consumer_group_1",
		SchemaAdapter:    schemaAdapter,
		OffsetsAdapter:   offsetsAdapter,
		InitializeSchema: false,
		BackoffManager:   backoff,
	}, logger)
	if err != nil {
		panic(err)
	}

	sub2, err := sql.NewSubscriber(db, sql.SubscriberConfig{
		ConsumerGroup:    "consumer_group_2",
		SchemaAdapter:    schemaAdapter,
		OffsetsAdapter:   offsetsAdapter,
		BackoffManager:   backoff,
		InitializeSchema: false,
	}, logger)
	if err != nil {
		panic(err)
	}

	var pub message.Publisher

	pub, err = sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        schemaAdapter,
		AutoInitializeSchema: false,
	}, logger)
	if err != nil {
		panic(err)
	}

	pub = sqlite3.DecoratePublisherWithCallback(func(topic string) {
		backoff.Notify(topic)
	}, pub)

	sampleSize := 1_000

	go func() {
		for i := 0; i < sampleSize; i++ {
			go func() {
				err := pub.Publish(
					fmt.Sprint("topic_", i%2),
					message.NewMessage(fmt.Sprint(i), message.Payload(fmt.Sprint("payload_", i))))
				log.Print("inserted ", i)
				if err != nil {
					panic(err)
				}
			}()
		}
	}()

	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(sampleSize * 2)

	subscribers := []message.Subscriber{sub1, sub2}

	var i atomic.Int32

	now := time.Now()

	for s, sub := range subscribers {
		go func() {
			msgs1, err := sub.Subscribe(ctx, "topic_0")
			if err != nil {
				panic(err)
			}

			msgs2, err := sub.Subscribe(ctx, "topic_1")
			if err != nil {
				panic(err)
			}

			go func() {
				for msg := range msgs1 {
					log.Print("sub", s, " topic_0 ", i.Add(1))
					msg.Ack()
					wg.Done()
				}
			}()

			go func() {
				for msg := range msgs2 {
					log.Print("sub", s, " topic_1 ", i.Add(1))
					msg.Ack()
					wg.Done()
				}
			}()
		}()
	}

	wg.Wait()

	log.Print(time.Since(now))
}
