/*
 * Copyright (C) 2024 The "MysteriumNetwork/node" Authors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cmd

import (
	"context"
	"errors"
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
func (m *blockingMultiManager) CheckChannel(context.Context) error   { return nil }
func (m *blockingMultiManager) Reconnect(context.Context, int) error { return nil }

type recordingAPIServer struct {
	stopCount int
}

func (n *recordingAPIServer) StartServing()            {}
func (n *recordingAPIServer) Wait() error              { return nil }
func (n *recordingAPIServer) Stop()                    { n.stopCount++ }
func (n *recordingAPIServer) Address() (string, error) { return "", nil }

type recordingUIServer struct {
	stopCount int
}

func (n *recordingUIServer) Serve()          {}
func (n *recordingUIServer) SwitchUI(string) {}
func (n *recordingUIServer) Stop()           { n.stopCount++ }

type noopPublisher struct{}

func (n *noopPublisher) Publish(string, interface{}) {}

type recordingSleepNotifier struct {
	stopCount int
}

func (n *recordingSleepNotifier) Start() {}
func (n *recordingSleepNotifier) Stop()  { n.stopCount++ }

// Unused but needed for compilation
var _ = proposal.PricedServiceProposal{}

func TestNodeKillBoundedAndCallsAllStops(t *testing.T) {
	gate := make(chan struct{})
	mgr := &blockingMultiManager{gate: gate}
	api := &recordingAPIServer{}
	ui := &recordingUIServer{}
	sleep := &recordingSleepNotifier{}

	node := NewNode(mgr, api, &noopPublisher{}, ui, sleep)
	node.ShutdownTimeout = 200 * time.Millisecond

	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- node.Kill()
	}()

	select {
	case err := <-done:
		elapsed := time.Since(start)
		// Should complete within ~300ms (200ms timeout + margin)
		if elapsed > 1*time.Second {
			t.Errorf("Kill took %s, expected < 1s", elapsed)
		}
		// Should return deadline error
		if err == nil {
			t.Error("expected error from timed-out disconnect")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected DeadlineExceeded, got %v", err)
		}
		// All stops must be called even on error
		if api.stopCount != 1 {
			t.Errorf("API Stop called %d times, want 1", api.stopCount)
		}
		if ui.stopCount != 1 {
			t.Errorf("UI Stop called %d times, want 1", ui.stopCount)
		}
		if sleep.stopCount != 1 {
			t.Errorf("Sleep Stop called %d times, want 1", sleep.stopCount)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Node.Kill did not return within 2s — shutdown not bounded")
	}

	close(gate) // unblock for cleanup
}
