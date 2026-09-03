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

package connection

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"

	"github.com/mysteriumnetwork/node/core/connection/connectionstate"
	"github.com/mysteriumnetwork/node/core/discovery/proposal"
	"github.com/mysteriumnetwork/node/eventbus"
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

func TestConnectContextPreservesCallerCancellationFromWait(t *testing.T) {
	tc := &testContext{}
	tc.SetT(t)
	tc.SetupTest()
	// Empty states — Start returns but waitForConnectedState blocks on open channel
	tc.fakeConnectionFactory.mockConnection.onStartReportStates = []fakeState{}

	// Use real eventbus so Subscribe actually fires
	bus := eventbus.New()
	tc.connManager.eventBus = bus

	connecting := make(chan struct{}, 1)
	require.NoError(t, bus.Subscribe(connectionstate.AppTopicConnectionState, func(ev connectionstate.AppEventConnectionState) {
		if ev.State == connectionstate.Connecting {
			select {
			case connecting <- struct{}{}:
			default:
			}
		}
	}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- tc.connManager.ConnectContext(ctx, consumerID, hermesID, activeProposalLookup, ConnectParams{})
	}()

	select {
	case <-connecting:
	case err := <-done:
		t.Fatalf("ConnectContext returned before Connecting: %v", err)
	case <-time.After(time.Second):
		t.Fatal("never entered connecting state")
	}
	cancel()

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
