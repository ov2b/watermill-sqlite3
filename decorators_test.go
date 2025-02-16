package sqlite3_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	sqlite3 "github.com/ov2b/watermill-sqlite3"
	"github.com/stretchr/testify/require"
)

func ExampleDecorateBackoffManagerWithResetLatch() {
	backoff := sqlite3.DecorateBackoffManagerWithResetLatch(sql.NewDefaultBackoffManager(10*time.Second, time.Second))

	var pub message.Publisher

	sqlite3.DecoratePublisherWithCallback(backoff.Notify, pub)
}

func TestNotifierBackoffManager_HandleError_NoMsg(t *testing.T) {

	manager := sqlite3.DecorateBackoffManagerWithResetLatch(sql.NewDefaultBackoffManager(time.Millisecond*5, time.Second))
	logger := watermill.NewStdLogger(false, false)

	// Test fallback interval
	start := time.Now()
	duration := manager.HandleError(logger, true, nil)
	require.Equal(t, time.Duration(0), duration)
	require.WithinDuration(t, start.Add(time.Millisecond*5), time.Now(), time.Millisecond)

	start = time.Now()
	manager.Notify("")
	duration = manager.HandleError(logger, true, nil)
	require.Equal(t, time.Duration(0), duration)
	require.WithinDuration(t, start.Add(time.Microsecond), time.Now(), 10*time.Microsecond)
}

func TestNotifierBackoffManager_HandleError_WithError(t *testing.T) {
	manager := sqlite3.DecorateBackoffManagerWithResetLatch(sql.NewDefaultBackoffManager(time.Second*10, time.Millisecond))
	logger := watermill.NewStdLogger(false, false)

	// Test retry interval
	start := time.Now()
	duration := manager.HandleError(logger, false, fmt.Errorf("oopsie"))
	require.Equal(t, time.Millisecond, duration)
	require.WithinDuration(t, start.Add(time.Millisecond), time.Now(), time.Millisecond)
}

func TestNotifierBackoffManager_Notify(t *testing.T) {
	logger := watermill.NewStdLogger(false, false)
	manager := sqlite3.DecorateBackoffManagerWithResetLatch(sql.NewDefaultBackoffManager(time.Second*10, time.Second))

	manager.Notify("")
	start := time.Now()
	duration := manager.HandleError(logger, true, nil)
	require.Equal(t, time.Duration(0), duration)
	require.WithinDuration(t, start.Add(time.Microsecond), time.Now(), 20*time.Microsecond)
}

func TestNotifierBackoffManager_Close(t *testing.T) {
	logger := watermill.NewStdLogger(false, false)
	manager := sqlite3.DecorateBackoffManagerWithResetLatch(sql.NewDefaultBackoffManager(time.Second*10, time.Second))

	// Test close
	manager.Close()
	start := time.Now()
	duration := manager.HandleError(logger, true, nil)
	require.Equal(t, time.Duration(0), duration)
	require.WithinDuration(t, start.Add(1*time.Microsecond), time.Now(), 30*time.Microsecond, "should notify on close")
}
