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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/mysteriumnetwork/node/core/connection/connectionstate"
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
// concurrent Stats calls to hit the connecting phase. Stats panics unless
// the manager has been explicitly activated — proves the phase gate works.
type slowConnectManager struct {
	mockManager
	connectGate    chan struct{}
	connectEntered chan struct{} // closed when ConnectContext blocks
	activated      atomic.Bool   // set after connect succeeds
	statsCalls     atomic.Int32  // counts Stats invocations
}

func (m *slowConnectManager) ConnectContext(ctx context.Context, _ identity.Identity, _ common.Address, _ ProposalLookup, _ ConnectParams) error {
	close(m.connectEntered)
	select {
	case <-m.connectGate:
		m.mu.Lock()
		m.connected = true
		m.mu.Unlock()
		m.activated.Store(true)
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

func (m *slowConnectManager) Stats() connectionstate.Statistics {
	if !m.activated.Load() {
		panic("Stats called on non-activated manager")
	}
	m.statsCalls.Add(1)
	return connectionstate.Statistics{BytesReceived: 42}
}

func TestStatsSafeDuringConnect(t *testing.T) {
	connectGate := make(chan struct{})
	connectEntered := make(chan struct{})
	var mgr *slowConnectManager
	mcm := NewMultiConnectionManager(func() Manager {
		m := &slowConnectManager{
			connectGate:    connectGate,
			connectEntered: connectEntered,
		}
		mgr = m
		return m
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

	// Stats during connecting must NOT reach the manager (would panic)
	stats := mcm.Stats(100)
	if stats.BytesReceived != 0 || stats.BytesSent != 0 {
		t.Errorf("stats during connecting should be zero, got %+v", stats)
	}
	if n := mgr.statsCalls.Load(); n != 0 {
		t.Errorf("Stats should not have been called during connecting, got %d calls", n)
	}

	// Status is safe during any phase
	status := mcm.Status(100)
	_ = status

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

	// After active, Stats reaches the manager and returns real data
	stats = mcm.Stats(100)
	if stats.BytesReceived != 42 {
		t.Errorf("stats after active should return manager data, got %+v", stats)
	}
	if n := mgr.statsCalls.Load(); n != 1 {
		t.Errorf("Stats should have been called once after active, got %d", n)
	}
}

// --- Restored pre-plan tests (adapted for context-aware API) ---

func TestConcurrentSamePortConnect(t *testing.T) {
	mcm, _ := newTestMulti()

	var wg sync.WaitGroup
	var errs [2]error
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
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
		t.Errorf("expected 1 entry, got %d", n)
	}
}

func TestBlockedConnectVsIndividualDisconnect(t *testing.T) {
	gate := make(chan struct{})
	connectEntered := make(chan struct{})
	created := &managersSlice{}
	mcm := NewMultiConnectionManager(func() Manager {
		m := &mockManager{connectGate: gate, connectEntered: connectEntered}
		created.add(m)
		return m
	})

	connectDone := make(chan error, 1)
	go func() {
		connectDone <- mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(200))
	}()
	// Wait for Manager.Connect to be entered (not just factory)
	select {
	case <-connectEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("Connect not entered")
	}

	discDone := make(chan error, 1)
	go func() {
		discDone <- mcm.Disconnect(bg(), 200)
	}()

	// Wait until entry is in retiring before releasing gate
	deadline := time.After(5 * time.Second)
	for {
		wait := mcm.waitNotify()
		mcm.mu.Lock()
		_, retiring := mcm.retiring[200]
		mcm.mu.Unlock()
		if retiring {
			break
		}
		select {
		case <-wait:
		case <-deadline:
			t.Fatal("entry never reached retiring")
		}
	}
	close(gate)

	select {
	case connectErr := <-connectDone:
		if connectErr == nil {
			t.Error("connect should fail when disconnect is pending")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connect did not complete")
	}

	select {
	case discErr := <-discDone:
		if discErr != nil {
			t.Errorf("disconnect should succeed, got %v", discErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("disconnect did not complete")
	}

	if n := created.get(0).disconnCount.Load(); n != 1 {
		t.Errorf("cleanup should run exactly once, got %d", n)
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("current should be empty, got %d", n)
	}
}

func TestExactManagerRemoval(t *testing.T) {
	mcm, created := newTestMulti()

	if err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100)); err != nil {
		t.Fatalf("first connect: %v", err)
	}
	if err := mcm.Disconnect(bg(), 100); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100)); err != nil {
		t.Fatalf("second connect: %v", err)
	}

	if created.len() != 2 {
		t.Fatalf("expected 2 managers, got %d", created.len())
	}

	mcm.mu.Lock()
	e := mcm.current[100]
	mcm.mu.Unlock()

	if e == nil || e.manager != created.get(1) {
		t.Error("current manager should be the second one created")
	}
}

func TestConnectRacingBulkDisconnect(t *testing.T) {
	mcm, _ := newTestMulti()
	for i := 0; i < 10; i++ {
		if err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(i)); err != nil {
			t.Fatalf("setup connect port %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = mcm.Disconnect(bg(), -1) }()
	go func() {
		defer wg.Done()
		errs[1] = mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(999))
	}()
	wg.Wait()

	// Bulk must succeed or be retryable
	if errs[0] != nil && !errors.Is(errs[0], ErrLifecycleBusy) {
		t.Errorf("bulk disconnect: unexpected error %v", errs[0])
	}
	// Connect may fail with ErrLifecycleBusy or succeed if it ran first
	if errs[1] != nil && !errors.Is(errs[1], ErrLifecycleBusy) {
		t.Errorf("concurrent connect: unexpected error %v", errs[1])
	}

	// Final reconciliation
	if err := mcm.Disconnect(bg(), -1); err != nil {
		t.Errorf("final bulk should succeed, got %v", err)
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("current should be empty after reconciliation, got %d", n)
	}
}

// --- Stress test ---

func TestRaceStressConnectDisconnect(t *testing.T) {
	mcm, _ := newTestMulti()

	var mu sync.Mutex
	var allErrs []error
	collectErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		allErrs = append(allErrs, err)
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(port int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				collectErr(mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(port)))
				collectErr(mcm.Disconnect(bg(), port))
			}
		}(g * 100)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			collectErr(mcm.Disconnect(bg(), -1))
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

	for _, err := range allErrs {
		if !errors.Is(err, ErrLifecycleBusy) && !errors.Is(err, ErrAlreadyExists) &&
			!errors.Is(err, ErrConnectionCancelled) && !errors.Is(err, ErrNoConnection) {
			t.Errorf("unexpected error: %v", err)
		}
	}

	if err := mcm.Disconnect(bg(), -1); err != nil {
		t.Errorf("final bulk should succeed, got %v", err)
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("current should be empty after stress, got %d", n)
	}
	if n := retiringLen(mcm); n != 0 {
		t.Errorf("retiring should be empty after stress, got %d", n)
	}
}

// --- Reconnect tests ---

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

	mcm.Disconnect(bg(), -1)
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
