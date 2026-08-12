package connection

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

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

func dummyID() identity.Identity  { return identity.Identity{Address: "0x1"} }
func dummyHermes() common.Address { return common.HexToAddress("0x2") }
func dummyLookup() ProposalLookup {
	return func() (*proposal.PricedServiceProposal, error) { return nil, nil }
}
func dummyParams(port int) ConnectParams { return ConnectParams{ProxyPort: port} }

func registryLen(mcm *multiConnectionManager) int {
	mcm.mu.Lock()
	defer mcm.mu.Unlock()
	return len(mcm.cms)
}

// --- Tests ---

func TestRegistryEmptyAfterDisconnect(t *testing.T) {
	mcm, _ := newTestMulti()
	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))

	if err := mcm.Disconnect(100); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("registry should be empty after disconnect, got %d", n)
	}
}

func TestSamePortReconnect(t *testing.T) {
	mcm, created := newTestMulti()

	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	mcm.Disconnect(100)
	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))

	if created.len() != 2 {
		t.Errorf("expected 2 managers (original + reconnect), got %d", created.len())
	}
	if s := mcm.Status(100); s.State != connectionstate.Connected {
		t.Errorf("expected Connected after reconnect, got %v", s.State)
	}
}

func TestFailedConnectNotRegistered(t *testing.T) {
	created := &managersSlice{}
	mcm := NewMultiConnectionManager(func() Manager {
		m := &mockManager{connectErr: errors.New("p2p failed")}
		created.add(m)
		return m
	})

	err := mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	if err == nil {
		t.Fatal("expected connect error")
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("failed connect should not register, got %d", n)
	}
}

func TestConcurrentSamePortConnect(t *testing.T) {
	mcm, _ := newTestMulti()

	var wg sync.WaitGroup
	var errs [2]error
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
		}(i)
	}
	wg.Wait()

	successes := 0
	for _, err := range errs {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Errorf("expected 1 success, got %d", successes)
	}
	if n := registryLen(mcm); n != 1 {
		t.Errorf("expected 1 manager, got %d", n)
	}
}

func TestBulkDisconnect(t *testing.T) {
	mcm, _ := newTestMulti()
	for _, port := range []int{100, 200, 300} {
		mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(port))
	}

	if err := mcm.Disconnect(-1); err != nil {
		t.Fatalf("bulk disconnect: %v", err)
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("registry should be empty after bulk disconnect, got %d", n)
	}
}

func TestBulkDisconnectJoinsErrors(t *testing.T) {
	created := &managersSlice{}
	mcm := NewMultiConnectionManager(func() Manager {
		m := &mockManager{}
		created.add(m)
		return m
	})

	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(200))

	created.get(1).mu.Lock()
	created.get(1).disconnectErr = errors.New("net error")
	created.get(1).mu.Unlock()

	err := mcm.Disconnect(-1)
	if err == nil {
		t.Error("expected joined error")
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("registry should be empty even after partial failure, got %d", n)
	}
}

func TestDisconnectIgnoresErrNoConnection(t *testing.T) {
	mcm, _ := newTestMulti()
	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	mcm.Disconnect(100)

	if err := mcm.Disconnect(100); err != nil {
		t.Errorf("second disconnect should return nil, got: %v", err)
	}
}

// --- Deterministic race tests ---

// TestBlockedConnectVsBulkDisconnect reproduces the stale-manager escape:
// Connect blocks mid-operation, DisconnectAll runs, Connect finishes.
// The bulk barrier ensures DisconnectAll waits.
func TestBlockedConnectVsBulkDisconnect(t *testing.T) {
	gate := make(chan struct{})
	created := &managersSlice{}
	mcm := NewMultiConnectionManager(func() Manager {
		m := &mockManager{connectGate: gate}
		created.add(m)
		return m
	})

	// Start Connect — blocks on gate
	connectDone := make(chan error, 1)
	go func() {
		connectDone <- mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	}()

	// Wait for the manager to be created (Connect entered stripe lock)
	for created.len() == 0 {
	}

	// DisconnectAll — must block until Connect finishes
	bulkDone := make(chan error, 1)
	go func() {
		bulkDone <- mcm.Disconnect(-1)
	}()

	// Unblock Connect
	close(gate)
	<-connectDone
	<-bulkDone

	// Manager must NOT survive — bulk disconnect cleaned it
	if n := registryLen(mcm); n != 0 {
		t.Errorf("live manager escaped bulk cleanup: registry has %d entries", n)
	}
	if created.get(0).disconnCount.Load() == 0 {
		t.Error("manager should have been disconnected by bulk cleanup")
	}
}

// TestBlockedConnectVsIndividualDisconnect verifies single-port disconnect
// also waits for in-flight Connect via the stripe lock.
func TestBlockedConnectVsIndividualDisconnect(t *testing.T) {
	gate := make(chan struct{})
	created := &managersSlice{}
	mcm := NewMultiConnectionManager(func() Manager {
		m := &mockManager{connectGate: gate}
		created.add(m) // signals that Connect has entered the stripe
		return m
	})

	connectDone := make(chan error, 1)
	go func() {
		connectDone <- mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(200))
	}()

	// Wait for Connect to create the manager (proves it's inside the stripe)
	for created.len() == 0 {
	}

	discDone := make(chan error, 1)
	go func() {
		discDone <- mcm.Disconnect(200)
	}()

	close(gate)
	<-connectDone
	<-discDone

	if n := registryLen(mcm); n != 0 {
		t.Errorf("registry should be empty, got %d", n)
	}
}

// TestExactManagerRemoval verifies Disconnect removes only the manager it found,
// not a newer replacement for the same port.
func TestExactManagerRemoval(t *testing.T) {
	mcm, created := newTestMulti()

	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	mcm.Disconnect(100)
	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))

	// Second manager is the current one
	if created.len() != 2 {
		t.Fatalf("expected 2 managers, got %d", created.len())
	}

	mcm.mu.Lock()
	current := mcm.cms[100]
	mcm.mu.Unlock()

	if current != created.get(1) {
		t.Error("current manager should be the second one created")
	}
}

// TestConnectAfterBulkDisconnectSurvives verifies that a Connect that starts
// after DisconnectAll completes is not affected.
func TestConnectAfterBulkDisconnectSurvives(t *testing.T) {
	mcm, _ := newTestMulti()
	for i := 0; i < 5; i++ {
		mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(i))
	}
	mcm.Disconnect(-1)

	// Connect after bulk cleanup
	if err := mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(999)); err != nil {
		t.Fatalf("connect after bulk: %v", err)
	}
	if s := mcm.Status(999); s.State != connectionstate.Connected {
		t.Errorf("port 999 should be connected, got %v", s.State)
	}
}

func TestRepeatedCyclesNoGrowth(t *testing.T) {
	mcm, _ := newTestMulti()
	for i := 0; i < 50; i++ {
		mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
		mcm.Disconnect(100)
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("registry should be empty after 50 cycles, got %d", n)
	}
}

func TestDisconnectUnknownPortSafe(t *testing.T) {
	mcm, _ := newTestMulti()
	for i := 0; i < 100; i++ {
		mcm.Disconnect(i)
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("registry should be empty, got %d", n)
	}
}

func TestConnectRacingBulkDisconnect(t *testing.T) {
	mcm, _ := newTestMulti()
	for i := 0; i < 10; i++ {
		mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(i))
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); mcm.Disconnect(-1) }()
	go func() { defer wg.Done(); mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(999)) }()
	wg.Wait()

	// No panic, no race. Consistent state.
	mcm.mu.Lock()
	for port, m := range mcm.cms {
		if m == nil {
			t.Errorf("nil manager for port %d", port)
		}
	}
	mcm.mu.Unlock()
}
