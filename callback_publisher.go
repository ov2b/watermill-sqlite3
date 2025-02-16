package sqlite3

import "github.com/ThreeDotsLabs/watermill/message"

// DecoratePublisherWithCallback calls the provided callback, whenever a message is published
// With this decorator and [DecorateBackoffManagerWithResetLatch], you minimize the latency caused by polling.
func DecoratePublisherWithCallback(callback func(topic string), pub message.Publisher) message.Publisher {
	return &CallbackPublisher{
		callback: callback,
		pub:      pub,
	}
}

type CallbackPublisher struct {
	callback func(topic string)

	pub message.Publisher
}

// Close implements message.Publisher.
func (c *CallbackPublisher) Close() error {
	return c.pub.Close()
}

// Publish implements message.Publisher.
func (c *CallbackPublisher) Publish(topic string, messages ...*message.Message) error {
	if err := c.pub.Publish(topic, messages...); err != nil {
		return err
	}

	c.callback(topic)

	return nil
}

var _ message.Publisher = &CallbackPublisher{}
