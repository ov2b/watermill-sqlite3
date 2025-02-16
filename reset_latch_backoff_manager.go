package sqlite3

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
)

// DecorateBackoffManagerWithResetLatch decorates the provided [sql.BackoffManager] with a reset latch.
// HandleError will block, when noMsg == true, up to the duration provided by the decorated backoff manager.
// When [Notify] is called, the latch is triggered and currently blocked HandleError calls, will proceed.
// After HandleError returns, the latch is reset.
//
// With this decorator and [DecoratePublisherWithCallback], you minimize the latency caused by polling.
func DecorateBackoffManagerWithResetLatch(backoff sql.BackoffManager) *ResetLatchBackoffManager {

	return &ResetLatchBackoffManager{
		c:    make(chan string, 100),
		next: backoff,
	}
}

// ResetLatchBackoffManager is a backoff manager that notifies the subscriber when a message is received.
type ResetLatchBackoffManager struct {
	c    chan string
	next sql.BackoffManager
}

// HandleError implements sql.BackoffManager.
func (n *ResetLatchBackoffManager) HandleError(logger watermill.LoggerAdapter, noMsg bool, err error) time.Duration {
	if noMsg {
		d := n.next.HandleError(logger, noMsg, err)
		select {
		case <-n.c:
			logger.Debug("received notification", nil)
			return 0
		case <-time.After(d):
			return 0
		}
	}

	return n.next.HandleError(logger, noMsg, err)
}

func (n *ResetLatchBackoffManager) Notify(topic string) {
	select {
	case n.c <- topic:
	default:
		// TODO log?
	}

}

func (n *ResetLatchBackoffManager) Close() {
	n.Notify("")
}
