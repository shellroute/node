package connection

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/mysteriumnetwork/node/identity"
)

// phasedReconnectManager tracks disconnect and connect phases during ReconnectContext.
// Uses channels for deterministic synchronization — no sleeps.
type phasedReconnectManager struct {
	mockManager
	disconnectEntered chan struct{} // closed when DisconnectContext blocks
	cancelObserved    chan struct{} // closed when ctx cancellation is observed
	disconnectGate    chan struct{} // safety valve — unblocks DisconnectContext if ctx cancel doesn't
	connectStarted    atomic.Int32  // incremented if fresh connect phase begins
}

func (m *phasedReconnectManager) ConnectContext(ctx context.Context, _ identity.Identity, _ common.Address, _ ProposalLookup, _ ConnectParams) error {
	m.connectStarted.Add(1)
	return m.Connect(identity.Identity{}, common.Address{}, nil, ConnectParams{})
}

func (m *phasedReconnectManager) CancelCurrentOperation() {}

func (m *phasedReconnectManager) DisconnectContext(ctx context.Context) error {
	// Signal that we've entered the disconnect phase
	select {
	case <-m.disconnectEntered:
	default:
		close(m.disconnectEntered)
	}

	// Block until context cancelled or gate opened
	select {
	case <-ctx.Done():
		close(m.cancelObserved)
		return ctx.Err()
	case <-m.disconnectGate:
		return m.Disconnect()
	}
}

func (m *phasedReconnectManager) ReconnectContext(ctx context.Context) error {
	// Phase 1: disconnect
	err := m.DisconnectContext(ctx)
	if err != nil && !errors.Is(err, ErrNoConnection) {
		return err
	}
	// Phase 2: check context — bulk must have cancelled by now
	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrConnectionCancelled, ctx.Err())
	}
	// Phase 3: fresh connect — should NOT reach here if bulk cancelled
	m.connectStarted.Add(1)
	return m.ConnectContext(ctx, identity.Identity{}, common.Address{}, nil, ConnectParams{})
}

func TestBulkCancelsReconnectBeforeFreshConnect(t *testing.T) {
	disconnectEntered := make(chan struct{})
	cancelObserved := make(chan struct{})
	disconnectGate := make(chan struct{})

	var mgr *phasedReconnectManager
	mcm := NewMultiConnectionManager(func() Manager {
		m := &phasedReconnectManager{
			disconnectEntered: disconnectEntered,
			cancelObserved:    cancelObserved,
			disconnectGate:    disconnectGate,
		}
		mgr = m
		return m
	})
	mcm.BulkTimeout = 5 * time.Second

	// Establish connection
	if err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100)); err != nil {
		t.Fatalf("connect: %v", err)
	}
	mgr.connectStarted.Store(0)

	// Start reconnect — will block in disconnect phase
	reconDone := make(chan error, 1)
	go func() {
		reconDone <- mcm.Reconnect(bg(), 100)
	}()

	// Wait for reconnect to enter DisconnectContext
	select {
	case <-disconnectEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("reconnect did not enter disconnect phase")
	}

	// Start bulk — opCancel should cancel the reconnect's operation context
	bulkDone := make(chan error, 1)
	go func() {
		bulkDone <- mcm.Disconnect(bg(), -1)
	}()

	// Wait for the mock to observe ctx cancellation — proves opCancel fired
	select {
	case <-cancelObserved:
	case <-time.After(5 * time.Second):
		// Safety: close gate to unblock and fail
		close(disconnectGate)
		t.Fatal("reconnect did not observe context cancellation from bulk")
	}

	// Close gate as safety valve for any remaining retireEntry DisconnectContext calls
	close(disconnectGate)

	// Reconnect must fail
	select {
	case err := <-reconDone:
		if err == nil {
			t.Error("reconnect should fail after bulk cancellation")
		}
		if !errors.Is(err, ErrConnectionCancelled) && !errors.Is(err, ErrLifecycleBusy) {
			t.Errorf("reconnect error should match ErrConnectionCancelled or ErrLifecycleBusy, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reconnect did not complete")
	}

	// Bulk must succeed
	select {
	case err := <-bulkDone:
		if err != nil {
			t.Errorf("bulk disconnect should succeed, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bulk disconnect did not complete")
	}

	// Key assertion: fresh connect phase must NOT have started
	if n := mgr.connectStarted.Load(); n > 0 {
		t.Errorf("bulk cancellation allowed ReconnectContext to start %d fresh connect phase(s)", n)
	}
}

// slowConnectManager blocks on connectGate during ConnectContext, allowing
// concurrent Stats calls to hit the connecting phase.
type slowConnectManager struct {
	mockManager
	connectGate    chan struct{}
	connectEntered chan struct{} // closed when ConnectContext blocks
}

func (m *slowConnectManager) ConnectContext(ctx context.Context, _ identity.Identity, _ common.Address, _ ProposalLookup, _ ConnectParams) error {
	close(m.connectEntered)
	select {
	case <-m.connectGate:
		m.mu.Lock()
		m.connected = true
		m.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *slowConnectManager) CancelCurrentOperation() {}
func (m *slowConnectManager) DisconnectContext(_ context.Context) error {
	return m.Disconnect()
}
func (m *slowConnectManager) ReconnectContext(_ context.Context) error { return nil }

func TestStatsSafeDuringConnect(t *testing.T) {
	connectGate := make(chan struct{})
	connectEntered := make(chan struct{})
	mcm := NewMultiConnectionManager(func() Manager {
		return &slowConnectManager{
			connectGate:    connectGate,
			connectEntered: connectEntered,
		}
	})

	// Start connect — blocks in ConnectContext
	connectDone := make(chan error, 1)
	go func() {
		connectDone <- mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	}()

	// Wait for connect to enter the blocking phase
	select {
	case <-connectEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("connect did not enter blocking phase")
	}

	// Concurrent Stats must return zero without race/panic
	stats := mcm.Stats(100)
	if stats.BytesReceived != 0 || stats.BytesSent != 0 {
		t.Errorf("stats during connecting should be zero, got %+v", stats)
	}

	// Status is safe during any phase
	status := mcm.Status(100)
	_ = status // no panic = pass

	// Unblock connect
	close(connectGate)
	select {
	case err := <-connectDone:
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connect did not complete")
	}

	// After active, Stats should work normally
	stats = mcm.Stats(100)
	_ = stats // no panic = pass
}
