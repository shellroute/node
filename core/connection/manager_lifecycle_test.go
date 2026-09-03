package connection

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/mysteriumnetwork/node/core/connection/connectionstate"
	"github.com/mysteriumnetwork/node/core/discovery/proposal"
	"github.com/mysteriumnetwork/node/identity"
	"github.com/mysteriumnetwork/node/mocks"
)

func TestConnectContextKeepsUnderlyingOperationTracked(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	lookupErr := errors.New("lookup stopped")
	m := &connectionManager{status: connectionstate.Status{State: connectionstate.NotConnected}}
	lookup := func() (*proposal.PricedServiceProposal, error) {
		close(started)
		<-release
		return nil, lookupErr
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- m.ConnectContext(ctx, identity.Identity{}, common.Address{}, lookup, ConnectParams{})
	}()
	<-started
	cancel()

	// Should NOT return while proposal lookup is still running
	select {
	case err := <-done:
		t.Fatalf("ConnectContext returned %v while proposal lookup was still running", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, ErrConnectionCancelled) || !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want ErrConnectionCancelled + context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ConnectContext did not finish after proposal lookup released")
	}
}

func TestDisconnectContextSharesRunningCleanup(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var cleanupCalls atomic.Int32
	m := &connectionManager{
		status:   connectionstate.Status{State: connectionstate.Connected},
		cancel:   func() {},
		eventBus: mocks.NewEventBus(),
		cleanup: []func() error{func() error {
			cleanupCalls.Add(1)
			close(started)
			<-release
			return nil
		}},
	}

	// First caller times out
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { firstDone <- m.DisconnectContext(ctx) }()
	<-started

	select {
	case err := <-firstDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			close(release)
			t.Fatalf("first caller got %v, want DeadlineExceeded", err)
		}
	case <-time.After(100 * time.Millisecond):
		close(release)
		t.Fatal("first caller did not observe deadline")
	}

	// Second caller attaches to same cleanup
	secondDone := make(chan error, 1)
	go func() { secondDone <- m.DisconnectContext(context.Background()) }()
	select {
	case err := <-secondDone:
		close(release)
		t.Fatalf("second caller returned %v before cleanup finished", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second caller got %v after cleanup completed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second caller did not observe cleanup completion")
	}

	if got := cleanupCalls.Load(); got != 1 {
		t.Fatalf("cleanup ran %d times, want exactly once", got)
	}
}

func TestConnectContextPreservesCallerCancellationFromLookup(t *testing.T) {
	// Uses a bare connectionManager with a blocking lookup to verify
	// that cancellation during establishment preserves both
	// ErrConnectionCancelled and the concrete ctx error for 503 mapping.
	started := make(chan struct{})
	release := make(chan struct{})
	m := &connectionManager{
		status:   connectionstate.Status{State: connectionstate.NotConnected},
		eventBus: mocks.NewEventBus(),
	}
	lookup := func() (*proposal.PricedServiceProposal, error) {
		close(started)
		<-release
		return &proposal.PricedServiceProposal{}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- m.ConnectContext(ctx, identity.Identity{}, common.Address{}, lookup, ConnectParams{})
	}()
	<-started
	cancel()
	close(release)

	select {
	case err := <-done:
		if !errors.Is(err, ErrConnectionCancelled) {
			t.Fatalf("got %v, want ErrConnectionCancelled", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled for HTTP 503 mapping", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ConnectContext did not return after cancellation")
	}
}
