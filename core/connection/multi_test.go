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
	disconnectErr error
	connectCount  atomic.Int32
	disconnCount  atomic.Int32
}

func (m *mockManager) Connect(_ identity.Identity, _ common.Address, _ ProposalLookup, _ ConnectParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.connected {
		return ErrAlreadyExists
	}
	m.connected = true
	m.connectCount.Add(1)
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
func dummyLookup() ProposalLookup     { return func() (*proposal.PricedServiceProposal, error) { return nil, nil } }
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

	// New manager should be connected
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

func TestBulkDisconnectPartialFailure(t *testing.T) {
	mcm, created := newTestMulti()

	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(200))

	// Make second manager fail on disconnect
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

	// Disconnect twice — second should not error (ErrNoConnection suppressed)
	mcm.Disconnect(100)
	err := mcm.Disconnect(100) // not in registry anymore
	if err != nil {
		t.Errorf("second disconnect should return nil, got: %v", err)
	}
}

func TestConnectRacingBulkDisconnect(t *testing.T) {
	mcm, _ := newTestMulti()

	// Connect some initial ports
	for i := 0; i < 10; i++ {
		mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(i))
	}

	var wg sync.WaitGroup

	// Bulk disconnect
	wg.Add(1)
	go func() {
		defer wg.Done()
		mcm.Disconnect(-1)
	}()

	// Concurrent connect on new port
	wg.Add(1)
	go func() {
		defer wg.Done()
		mcm.Connect(dummyID(), dummyHermes(), dummyLookup(), dummyParams(999))
	}()

	wg.Wait()

	// Port 999 should still be connected (connected after bulk detach)
	s := mcm.Status(999)
	if s.State != connectionstate.Connected {
		t.Errorf("port 999 should survive bulk disconnect, got %v", s.State)
	}
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
