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

// mockManager is a minimal Manager for testing the multi registry.
type mockManager struct {
	mu            sync.Mutex
	connected     bool
	connectDelay  chan struct{} // if set, Connect blocks until closed
	connectErr    error        // if set, Connect returns this error
	disconnectErr error
	connectCount  atomic.Int32
	disconnCount  atomic.Int32
}

func (m *mockManager) Connect(_ identity.Identity, _ common.Address, _ ProposalLookup, _ ConnectParams) error {
	if m.connectDelay != nil {
		<-m.connectDelay
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

func (m *mockManager) Stats() connectionstate.Statistics {
	return connectionstate.Statistics{}
}

func (m *mockManager) Disconnect() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disconnCount.Add(1)
	if !m.connected {
		return ErrNoConnection
	}
	m.connected = false
	if m.disconnectErr != nil {
		return m.disconnectErr
	}
	return nil
}

func (m *mockManager) CheckChannel(context.Context) error { return nil }
func (m *mockManager) Reconnect()                         {}

func newTestMulti() (*multiConnectionManager, *[]*mockManager) {
	var created []*mockManager
	mcm := NewMultiConnectionManager(func() Manager {
		m := &mockManager{}
		created = append(created, m)
		return m
	})
	return mcm, &created
}

func dummyID() identity.Identity      { return identity.Identity{Address: "0x1"} }
func dummyHermes() common.Address     { return common.HexToAddress("0x2") }
func dummyLookup() ProposalLookup {
	return func() (*proposal.PricedServiceProposal, error) { return nil, nil }
}
func dummyParams(port int) ConnectParams { return ConnectParams{ProxyPort: port} }

func TestRegistryEmptyAfterDisconnect(t *testing.T) {
	mcm, _ := newTestMulti()

	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))

	if err := mcm.Disconnect(100); err != nil {
		t.Fatalf("disconnect: %v", err)
	}

	mcm.mu.Lock()
	n := len(mcm.cms)
	mcm.mu.Unlock()

	if n != 0 {
		t.Errorf("registry should be empty after disconnect, got %d", n)
	}
}

func TestSamePortReconnect(t *testing.T) {
	mcm, created := newTestMulti()

	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	mcm.Disconnect(100)

	// Reconnect on same port — should create a new manager
	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))

	if len(*created) != 2 {
		t.Errorf("expected 2 managers created (original + reconnect), got %d", len(*created))
	}

	s := mcm.Status(100)
	if s.State != connectionstate.Connected {
		t.Errorf("expected Connected after reconnect, got %v", s.State)
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

	mcm.mu.Lock()
	n := len(mcm.cms)
	mcm.mu.Unlock()

	if n != 0 {
		t.Errorf("registry should be empty after bulk disconnect, got %d", n)
	}
}

func TestBulkDisconnectJoinsErrors(t *testing.T) {
	mcm, created := newTestMulti()

	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(200))

	(*created)[1].mu.Lock()
	(*created)[1].disconnectErr = errors.New("network error")
	(*created)[1].mu.Unlock()

	err := mcm.Disconnect(-1)
	if err == nil {
		t.Error("expected error from partial failure")
	}

	// Registry should still be cleared
	mcm.mu.Lock()
	n := len(mcm.cms)
	mcm.mu.Unlock()

	if n != 0 {
		t.Errorf("registry should be empty even after partial failure, got %d", n)
	}
}

func TestDisconnectIgnoresErrNoConnection(t *testing.T) {
	mcm, _ := newTestMulti()

	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	mcm.Disconnect(100)

	err := mcm.Disconnect(100) // not in registry anymore
	if err != nil {
		t.Errorf("second disconnect should return nil, got: %v", err)
	}
}

func TestFailedConnectNotRegistered(t *testing.T) {
	var created []*mockManager
	mcm := NewMultiConnectionManager(func() Manager {
		m := &mockManager{connectErr: errors.New("p2p failed")}
		created = append(created, m)
		return m
	})

	err := mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	if err == nil {
		t.Fatal("expected connect error")
	}

	mcm.mu.Lock()
	n := len(mcm.cms)
	mcm.mu.Unlock()

	if n != 0 {
		t.Errorf("failed connect should not register manager, got %d", n)
	}
}

func TestConcurrentSamePortConnect(t *testing.T) {
	// Two goroutines connect on the same port. Only one manager should exist.
	// The second connect gets ErrAlreadyExists (handled by Manager internals).
	mcm, created := newTestMulti()

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

	// One should succeed, one should get ErrAlreadyExists
	successes := 0
	for _, err := range errs {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Errorf("expected exactly 1 success, got %d (errs: %v, %v)", successes, errs[0], errs[1])
	}

	// Only one manager in registry
	mcm.mu.Lock()
	n := len(mcm.cms)
	mcm.mu.Unlock()
	if n != 1 {
		t.Errorf("expected 1 manager, got %d (created: %d)", n, len(*created))
	}
}

func TestConnectRacingBulkDisconnect(t *testing.T) {
	mcm, _ := newTestMulti()

	for i := 0; i < 10; i++ {
		mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(i))
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		mcm.Disconnect(-1)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(999))
	}()

	wg.Wait()

	// No panic, no race. Registry is in consistent state.
	mcm.mu.Lock()
	for port, m := range mcm.cms {
		if m == nil {
			t.Errorf("nil manager for port %d", port)
		}
	}
	mcm.mu.Unlock()
}

func TestRepeatedCyclesNoRegistryGrowth(t *testing.T) {
	mcm, _ := newTestMulti()

	for cycle := 0; cycle < 50; cycle++ {
		mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
		mcm.Disconnect(100)
	}

	mcm.mu.Lock()
	n := len(mcm.cms)
	mcm.mu.Unlock()

	if n != 0 {
		t.Errorf("registry should be empty after 50 connect/disconnect cycles, got %d", n)
	}
}

func TestDisconnectUnknownPortNoLeak(t *testing.T) {
	mcm, _ := newTestMulti()

	for i := 0; i < 100; i++ {
		mcm.Disconnect(i)
	}

	mcm.mu.Lock()
	n := len(mcm.cms)
	mcm.mu.Unlock()

	if n != 0 {
		t.Errorf("registry should be empty, got %d", n)
	}
}

// TestBulkDisconnectWaitsForInFlightConnect is the deterministic reproduction
// of the stale-manager race: Connect blocks, DisconnectAll runs, Connect finishes.
// Without the bulk barrier, the connected manager escapes cleanup.
func TestBulkDisconnectWaitsForInFlightConnect(t *testing.T) {
	connectGate := make(chan struct{})
	var created []*mockManager
	mcm := NewMultiConnectionManager(func() Manager {
		m := &mockManager{connectDelay: connectGate}
		created = append(created, m)
		return m
	})

	// Start Connect — blocks on connectGate
	connectDone := make(chan error, 1)
	go func() {
		connectDone <- mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	}()

	// Give Connect time to acquire locks and start m.Connect
	for {
		mcm.mu.Lock()
		_, hasLock := mcm.portLock[100]
		mcm.mu.Unlock()
		if hasLock {
			break
		}
	}

	// DisconnectAll should block until Connect finishes
	bulkDone := make(chan error, 1)
	go func() {
		bulkDone <- mcm.Disconnect(-1)
	}()

	// Unblock Connect
	close(connectGate)
	<-connectDone

	// Wait for DisconnectAll to finish
	<-bulkDone

	// The manager must NOT be in the registry — bulk disconnect cleaned it
	mcm.mu.Lock()
	n := len(mcm.cms)
	mcm.mu.Unlock()

	if n != 0 {
		t.Errorf("live manager escaped bulk cleanup and is no longer tracked: registry has %d entries", n)
	}

	// Verify the manager was actually disconnected
	if created[0].disconnCount.Load() == 0 {
		t.Error("manager should have been disconnected by bulk cleanup")
	}
}

// TestIndividualDisconnectWaitsForConnect verifies single-port disconnect
// also waits for in-flight Connect before removing.
func TestIndividualDisconnectWaitsForConnect(t *testing.T) {
	connectGate := make(chan struct{})
	var created []*mockManager
	mcm := NewMultiConnectionManager(func() Manager {
		m := &mockManager{connectDelay: connectGate}
		created = append(created, m)
		return m
	})

	connectDone := make(chan error, 1)
	go func() {
		connectDone <- mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(200))
	}()

	// Wait for Connect to be in-flight
	for {
		mcm.mu.Lock()
		_, hasLock := mcm.portLock[200]
		mcm.mu.Unlock()
		if hasLock {
			break
		}
	}

	discDone := make(chan error, 1)
	go func() {
		discDone <- mcm.Disconnect(200)
	}()

	close(connectGate)
	<-connectDone
	<-discDone

	mcm.mu.Lock()
	n := len(mcm.cms)
	mcm.mu.Unlock()

	if n != 0 {
		t.Errorf("registry should be empty after disconnect waited for connect, got %d", n)
	}
}
