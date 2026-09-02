package connection

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/mysteriumnetwork/node/core/connection/connectionstate"
	"github.com/mysteriumnetwork/node/core/discovery/proposal"
	"github.com/mysteriumnetwork/node/identity"
)

// --- Mock manager ---

type mockManager struct {
	mu            sync.Mutex
	connected     bool
	connectGate   chan struct{} // if set, Connect blocks until closed
	connectErr    error
	disconnectErr error
	connectCount  atomic.Int32
	disconnCount  atomic.Int32
}

func (m *mockManager) Connect(_ identity.Identity, _ common.Address, _ ProposalLookup, _ ConnectParams) error {
	if m.connectGate != nil {
		<-m.connectGate
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connectCount.Add(1)
	if m.connectErr != nil {
		return m.connectErr
	}
	if m.connected {
		return ErrAlreadyExists
	}
	m.connected = true
	return nil
}

func (m *mockManager) Status() connectionstate.Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.connected {
		return connectionstate.Status{State: connectionstate.Connected}
	}
	return connectionstate.Status{State: connectionstate.NotConnected}
}

func (m *mockManager) Stats() connectionstate.Statistics { return connectionstate.Statistics{} }

func (m *mockManager) Disconnect() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disconnCount.Add(1)
	if !m.connected {
		return ErrNoConnection
	}
	m.connected = false
	return m.disconnectErr
}

func (m *mockManager) CheckChannel(context.Context) error { return nil }
func (m *mockManager) Reconnect()                         {}

// --- Helpers ---

type managersSlice struct {
	mu sync.Mutex
	s  []*mockManager
}

func (ms *managersSlice) add(m *mockManager) {
	ms.mu.Lock()
	ms.s = append(ms.s, m)
	ms.mu.Unlock()
}

func (ms *managersSlice) len() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.s)
}

func (ms *managersSlice) get(i int) *mockManager {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.s[i]
}

func newTestMulti() (*multiConnectionManager, *managersSlice) {
	created := &managersSlice{}
	mcm := NewMultiConnectionManager(func() Manager {
		m := &mockManager{}
		created.add(m)
		return m
	})
	return mcm, created
}

func bg() context.Context                { return context.Background() }
func dummyID() identity.Identity         { return identity.Identity{Address: "0x1"} }
func dummyHermes() common.Address        { return common.HexToAddress("0x2") }
func dummyLookup() ProposalLookup {
	return func() (*proposal.PricedServiceProposal, error) { return nil, nil }
}
func dummyParams(port int) ConnectParams { return ConnectParams{ProxyPort: port} }

func registryLen(mcm *multiConnectionManager) int {
	mcm.mu.Lock()
	defer mcm.mu.Unlock()
	return len(mcm.current)
}

func retiringLen(mcm *multiConnectionManager) int {
	mcm.mu.Lock()
	defer mcm.mu.Unlock()
	return len(mcm.retiring)
}

// --- Basic lifecycle tests ---

func TestRegistryEmptyAfterDisconnect(t *testing.T) {
	mcm, _ := newTestMulti()
	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	if err := mcm.Disconnect(bg(), 100); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("registry should be empty, got %d", n)
	}
}

func TestSamePortReconnect(t *testing.T) {
	mcm, created := newTestMulti()
	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	mcm.Disconnect(bg(), 100)
	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	if created.len() != 2 {
		t.Errorf("expected 2 managers, got %d", created.len())
	}
	if s := mcm.Status(100); s.State != connectionstate.Connected {
		t.Errorf("expected Connected, got %v", s.State)
	}
}

func TestFailedConnectNotRegistered(t *testing.T) {
	created := &managersSlice{}
	mcm := NewMultiConnectionManager(func() Manager {
		m := &mockManager{connectErr: errors.New("p2p failed")}
		created.add(m)
		return m
	})
	err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	if err == nil {
		t.Fatal("expected error")
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("failed connect should not register, got %d", n)
	}
	// Wait for retirement goroutine
	time.Sleep(50 * time.Millisecond)
	if n := retiringLen(mcm); n != 0 {
		t.Errorf("failed connect should retire cleanly, got %d retiring", n)
	}
}

func TestBulkDisconnect(t *testing.T) {
	mcm, _ := newTestMulti()
	for _, port := range []int{100, 200, 300} {
		mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(port))
	}
	if err := mcm.Disconnect(bg(), -1); err != nil {
		t.Fatalf("bulk disconnect: %v", err)
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("registry should be empty, got %d", n)
	}
}

func TestBulkDisconnectJoinsErrors(t *testing.T) {
	created := &managersSlice{}
	mcm := NewMultiConnectionManager(func() Manager {
		m := &mockManager{}
		created.add(m)
		return m
	})
	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(200))

	created.get(1).mu.Lock()
	created.get(1).disconnectErr = errors.New("net error")
	created.get(1).mu.Unlock()

	err := mcm.Disconnect(bg(), -1)
	// One port fails, so bulk returns error wrapping ErrLifecycleBusy
	if err == nil {
		t.Fatal("expected error from bulk disconnect with failing manager")
	}
	if !errors.Is(err, ErrLifecycleBusy) {
		t.Errorf("expected ErrLifecycleBusy, got %v", err)
	}
}

func TestDisconnectIgnoresErrNoConnection(t *testing.T) {
	mcm, _ := newTestMulti()
	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	mcm.Disconnect(bg(), 100)
	if err := mcm.Disconnect(bg(), 100); err != nil {
		t.Errorf("second disconnect should return nil, got: %v", err)
	}
}

func TestRepeatedCyclesNoGrowth(t *testing.T) {
	mcm, _ := newTestMulti()
	for i := 0; i < 50; i++ {
		mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
		mcm.Disconnect(bg(), 100)
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("registry should be empty, got %d", n)
	}
}

func TestDisconnectUnknownPortSafe(t *testing.T) {
	mcm, _ := newTestMulti()
	for i := 0; i < 100; i++ {
		mcm.Disconnect(bg(), i)
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("registry should be empty, got %d", n)
	}
}

// --- Lifecycle-busy tests ---

func TestConnectRejectsWhenReconciling(t *testing.T) {
	mcm, _ := newTestMulti()
	mcm.mu.Lock()
	mcm.reconcileRequired = true
	mcm.mu.Unlock()

	err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	if !errors.Is(err, ErrLifecycleBusy) {
		t.Errorf("expected ErrLifecycleBusy, got %v", err)
	}
}

func TestConnectRejectsRetiringPort(t *testing.T) {
	mcm, _ := newTestMulti()
	mcm.mu.Lock()
	mcm.retiring[100] = &portEntry{port: 100}
	mcm.mu.Unlock()

	err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	if !errors.Is(err, ErrLifecycleBusy) {
		t.Errorf("expected ErrLifecycleBusy, got %v", err)
	}
}

// --- Deterministic race tests ---

func TestBlockedConnectVsBulkDisconnect(t *testing.T) {
	gate := make(chan struct{})
	created := &managersSlice{}
	mcm := NewMultiConnectionManager(func() Manager {
		m := &mockManager{connectGate: gate}
		created.add(m)
		return m
	})
	mcm.BulkTimeout = 5 * time.Second

	connectDone := make(chan error, 1)
	go func() {
		connectDone <- mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	}()

	// Wait for manager to be created
	for created.len() == 0 {
	}

	bulkDone := make(chan error, 1)
	go func() {
		bulkDone <- mcm.Disconnect(bg(), -1)
	}()

	// Let bulk advance generation, then unblock connect
	time.Sleep(50 * time.Millisecond)
	close(gate)

	connectErr := <-connectDone
	bulkErr := <-bulkDone

	// Connect should fail (superseded by bulk) or be cancelled
	if connectErr == nil {
		t.Error("connect should fail when superseded by bulk")
	}
	// Bulk should succeed (manager unblocked)
	if bulkErr != nil {
		t.Errorf("bulk disconnect should succeed, got %v", bulkErr)
	}

	// Nothing should be in current
	if n := registryLen(mcm); n != 0 {
		t.Errorf("current should be empty, got %d", n)
	}

	// Manager should be disconnected
	time.Sleep(100 * time.Millisecond)
	if created.get(0).disconnCount.Load() == 0 {
		t.Error("stale manager should have been disconnected")
	}
}

func TestBulkSingleFlight(t *testing.T) {
	mcm, _ := newTestMulti()
	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))

	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = mcm.Disconnect(bg(), -1)
		}(i)
	}
	wg.Wait()

	// All should succeed (shared operation)
	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d got error: %v", i, err)
		}
	}
}

func TestTwoUnrelatedPortsConcurrent(t *testing.T) {
	mcm, _ := newTestMulti()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	}()
	go func() {
		defer wg.Done()
		mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(200))
	}()
	wg.Wait()

	if n := registryLen(mcm); n != 2 {
		t.Errorf("expected 2 current entries, got %d", n)
	}
}

func TestConnectAfterBulkSurvives(t *testing.T) {
	mcm, _ := newTestMulti()
	for i := 0; i < 5; i++ {
		mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(i))
	}
	mcm.Disconnect(bg(), -1)

	// Connect after reconciliation
	if err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(999)); err != nil {
		t.Fatalf("connect after bulk: %v", err)
	}
	if s := mcm.Status(999); s.State != connectionstate.Connected {
		t.Errorf("port 999 should be connected, got %v", s.State)
	}
}

func TestConnectCancellation(t *testing.T) {
	gate := make(chan struct{})
	mcm := NewMultiConnectionManager(func() Manager {
		return &mockManager{connectGate: gate}
	})

	ctx, cancel := context.WithCancel(context.Background())

	connectDone := make(chan error, 1)
	go func() {
		connectDone <- mcm.Connect(ctx, dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	err := <-connectDone
	close(gate)

	if !errors.Is(err, ErrConnectionCancelled) {
		t.Errorf("expected ErrConnectionCancelled, got %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("should wrap context.Canceled, got %v", err)
	}

	// Should not be in current
	if n := registryLen(mcm); n != 0 {
		t.Errorf("cancelled connect should not be in current, got %d", n)
	}
}

func TestBulkTimeoutReturnsFalse(t *testing.T) {
	// Manager that never returns from Disconnect
	gate := make(chan struct{})
	mcm := NewMultiConnectionManager(func() Manager {
		return &mockManager{}
	})
	mcm.BulkTimeout = 100 * time.Millisecond

	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))

	// Replace the manager's Disconnect with one that blocks
	mcm.mu.Lock()
	e := mcm.current[100]
	e.manager = &mockManager{connectGate: gate} // reuse gate to block Disconnect
	mcm.mu.Unlock()

	// Wrap the blocking manager
	blockingMgr := &blockingDisconnectManager{gate: gate}
	mcm.mu.Lock()
	e.manager = blockingMgr
	mcm.mu.Unlock()

	err := mcm.Disconnect(bg(), -1)
	close(gate)

	if err == nil {
		t.Error("bulk timeout should return error")
	}

	// reconcileRequired should remain true
	mcm.mu.Lock()
	reconcileRequired := mcm.reconcileRequired
	mcm.mu.Unlock()
	if !reconcileRequired {
		t.Error("reconcileRequired should remain true after timeout")
	}
}

// blockingDisconnectManager blocks on Disconnect until gate is closed
type blockingDisconnectManager struct {
	mockManager
	gate chan struct{}
}

func (m *blockingDisconnectManager) Disconnect() error {
	<-m.gate
	return nil
}

// --- Reconnect tests ---

// reconnectableManager satisfies lifecycleManager so multi.Reconnect uses ReconnectContext.
type reconnectableManager struct {
	mockManager
	reconnectErr error
}

func (m *reconnectableManager) ConnectContext(_ context.Context, _ identity.Identity, _ common.Address, _ ProposalLookup, _ ConnectParams) error {
	return m.Connect(identity.Identity{}, common.Address{}, nil, ConnectParams{})
}
func (m *reconnectableManager) CancelCurrentOperation() {}
func (m *reconnectableManager) DisconnectContext(_ context.Context) error {
	return m.Disconnect()
}
func (m *reconnectableManager) ReconnectContext(_ context.Context) error {
	return m.reconnectErr
}

func TestReconnectPropagatesError(t *testing.T) {
	wantErr := errors.New("reconnect failed")
	created := &managersSlice{}
	mcm := NewMultiConnectionManager(func() Manager {
		m := &reconnectableManager{}
		created.add(&m.mockManager)
		return m
	})

	if err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100)); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Inject reconnect error
	mcm.mu.Lock()
	rm := mcm.current[100].manager.(*reconnectableManager)
	rm.reconnectErr = wantErr
	mcm.mu.Unlock()

	err := mcm.Reconnect(bg(), 100)
	if err == nil {
		t.Fatal("expected error from failed reconnect")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("expected %v, got %v", wantErr, err)
	}

	// Failed reconnect should retire the entry
	time.Sleep(50 * time.Millisecond)
	if n := registryLen(mcm); n != 0 {
		t.Errorf("failed reconnect should remove from current, got %d", n)
	}
}

func TestReconnectSucceeds(t *testing.T) {
	mcm := NewMultiConnectionManager(func() Manager {
		return &reconnectableManager{}
	})

	if err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100)); err != nil {
		t.Fatalf("connect: %v", err)
	}

	if err := mcm.Reconnect(bg(), 100); err != nil {
		t.Errorf("reconnect should succeed, got %v", err)
	}

	if n := registryLen(mcm); n != 1 {
		t.Errorf("expected 1 current entry, got %d", n)
	}
}

// --- Gateway-style retry test ---

func TestGatewayRetryAfterTimeout(t *testing.T) {
	// Simulate: first bulk request times out, cleanup completes later, second bulk succeeds
	gate := make(chan struct{})
	mcm := NewMultiConnectionManager(func() Manager {
		return &blockingDisconnectManager{gate: gate}
	})
	mcm.BulkTimeout = 100 * time.Millisecond

	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))

	// First bulk — will timeout because disconnect blocks
	err1 := mcm.Disconnect(bg(), -1)
	if err1 == nil {
		t.Fatal("first bulk should timeout")
	}
	if !errors.Is(err1, ErrLifecycleBusy) {
		t.Errorf("expected ErrLifecycleBusy, got %v", err1)
	}

	// New connects should fail while reconciling
	err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(200))
	if !errors.Is(err, ErrLifecycleBusy) {
		t.Errorf("connect during unresolved reconciliation should return ErrLifecycleBusy, got %v", err)
	}

	// Unblock the stuck disconnect
	close(gate)

	// Second bulk — should succeed now that disconnect completed
	err2 := mcm.Disconnect(bg(), -1)
	if err2 != nil {
		t.Errorf("second bulk should succeed, got %v", err2)
	}

	if n := registryLen(mcm); n != 0 {
		t.Errorf("current should be empty, got %d", n)
	}
	if n := retiringLen(mcm); n != 0 {
		t.Errorf("retiring should be empty, got %d", n)
	}

	// Connect after successful reconciliation should work
	if err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(300)); err != nil {
		t.Errorf("connect after resolved reconciliation should work, got %v", err)
	}
}

// --- Stress test ---

func TestRaceStressConnectDisconnect(t *testing.T) {
	mcm, _ := newTestMulti()

	var wg sync.WaitGroup
	// 10 goroutines each doing 20 connect/disconnect cycles
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(port int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(port))
				mcm.Disconnect(bg(), port)
			}
		}(g * 100)
	}

	// Concurrent bulk disconnects
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			mcm.Disconnect(bg(), -1)
			time.Sleep(10 * time.Millisecond)
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("stress test did not complete within 10s")
	}

	// Final cleanup
	mcm.Disconnect(bg(), -1)

	if n := registryLen(mcm); n != 0 {
		t.Errorf("current should be empty after stress, got %d", n)
	}
	if n := retiringLen(mcm); n != 0 {
		t.Errorf("retiring should be empty after stress, got %d", n)
	}
}
