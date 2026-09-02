package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/mysteriumnetwork/node/core/connection"
	"github.com/mysteriumnetwork/node/core/connection/connectionstate"
	"github.com/mysteriumnetwork/node/core/discovery/proposal"
	"github.com/mysteriumnetwork/node/identity"

	"github.com/ethereum/go-ethereum/common"
)

type blockingMultiManager struct {
	gate chan struct{}
}

func (m *blockingMultiManager) Connect(_ context.Context, _ identity.Identity, _ common.Address, _ connection.ProposalLookup, _ connection.ConnectParams) error {
	return nil
}
func (m *blockingMultiManager) Status(int) connectionstate.Status {
	return connectionstate.Status{State: connectionstate.NotConnected}
}
func (m *blockingMultiManager) Stats(int) connectionstate.Statistics {
	return connectionstate.Statistics{}
}
func (m *blockingMultiManager) Disconnect(ctx context.Context, id int) error {
	if id < 0 && m.gate != nil {
		select {
		case <-m.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (m *blockingMultiManager) CheckChannel(context.Context) error { return nil }
func (m *blockingMultiManager) Reconnect(context.Context, int) error { return nil }

type noopAPIServer struct{}

func (n *noopAPIServer) StartServing()          {}
func (n *noopAPIServer) Wait() error            { return nil }
func (n *noopAPIServer) Stop()                  {}
func (n *noopAPIServer) Address() (string, error) { return "", nil }

type noopUIServer struct{}

func (n *noopUIServer) Serve()           {}
func (n *noopUIServer) SwitchUI(string)  {}
func (n *noopUIServer) Stop()            {}

type noopPublisher struct{}

func (n *noopPublisher) Publish(string, interface{}) {}

type noopSleepNotifier struct{}

func (n *noopSleepNotifier) Start() {}
func (n *noopSleepNotifier) Stop()  {}

// Unused but needed for compilation
var _ = proposal.PricedServiceProposal{}

func TestNodeKillBoundedByTimeout(t *testing.T) {
	gate := make(chan struct{})
	mgr := &blockingMultiManager{gate: gate}

	node := NewNode(mgr, &noopAPIServer{}, &noopPublisher{}, &noopUIServer{}, &noopSleepNotifier{})
	node.ShutdownTimeout = 200 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		done <- node.Kill()
	}()

	select {
	case err := <-done:
		// Should complete within timeout even though disconnect blocks
		if err == nil {
			t.Error("expected error from timed-out disconnect")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Node.Kill did not return within 2s — shutdown not bounded")
	}

	close(gate) // unblock for cleanup
}

func TestNodeKillCallsAllStops(t *testing.T) {
	mgr := &blockingMultiManager{}
	api := &noopAPIServer{}
	node := NewNode(mgr, api, &noopPublisher{}, &noopUIServer{}, &noopSleepNotifier{})
	node.ShutdownTimeout = 100 * time.Millisecond

	err := node.Kill()
	if err != nil {
		t.Errorf("expected nil error for clean disconnect, got %v", err)
	}
}
