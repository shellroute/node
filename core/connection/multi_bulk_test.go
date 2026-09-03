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

	"github.com/mysteriumnetwork/node/identity"
)

// blockingDisconnectManager blocks on Disconnect until gate is closed.
// Optional disconnectEntered is closed when Disconnect enters.
// Optional disconnectErrFn returns the error for each call.
type blockingDisconnectManager struct {
	mockManager
	gate              chan struct{}
	disconnectEntered chan struct{}
	disconnectErrFn   func(n int32) error
}

func (m *blockingDisconnectManager) Disconnect() error {
	n := m.disconnCount.Add(1)
	if m.disconnectEntered != nil {
		select {
		case <-m.disconnectEntered:
		default:
			close(m.disconnectEntered)
		}
	}
	<-m.gate
	if m.disconnectErrFn != nil {
		return m.disconnectErrFn(n)
	}
	return nil
}

func TestBulkTimeoutReturnsFalse(t *testing.T) {
	gate := make(chan struct{})
	mcm := NewMultiConnectionManager(func() Manager {
		return &mockManager{}
	})
	mcm.BulkTimeout = 100 * time.Millisecond

	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))

	blockingMgr := &blockingDisconnectManager{gate: gate}
	mcm.mu.Lock()
	mcm.current[100].manager = blockingMgr
	mcm.mu.Unlock()

	err := mcm.Disconnect(bg(), -1)
	close(gate)

	if err == nil {
		t.Error("bulk timeout should return error")
	}

	mcm.mu.Lock()
	reconcileRequired := mcm.reconcileRequired
	mcm.mu.Unlock()
	if !reconcileRequired {
		t.Error("reconcileRequired should remain true after timeout")
	}
}

func TestGatewayRetryAfterTimeout(t *testing.T) {
	gate := make(chan struct{})
	disconnectEntered := make(chan struct{})
	var mgr *blockingDisconnectManager
	mcm := NewMultiConnectionManager(func() Manager {
		m := &blockingDisconnectManager{gate: gate, disconnectEntered: disconnectEntered}
		mgr = m
		return m
	})
	mcm.BulkTimeout = 100 * time.Millisecond

	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))

	err1 := mcm.Disconnect(bg(), -1)
	if err1 == nil {
		t.Fatal("first bulk should timeout")
	}
	if !errors.Is(err1, ErrLifecycleBusy) {
		t.Errorf("expected ErrLifecycleBusy, got %v", err1)
	}

	select {
	case <-disconnectEntered:
	case <-time.After(time.Second):
		t.Fatal("Disconnect never entered")
	}

	// Connect should fail while unresolved
	err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(200))
	if !errors.Is(err, ErrLifecycleBusy) {
		t.Errorf("connect during unresolved reconciliation should return ErrLifecycleBusy, got %v", err)
	}

	// Retry — wait for activeBulk to be installed before releasing
	retryDone := make(chan error, 1)
	go func() { retryDone <- mcm.Disconnect(bg(), -1) }()

	deadline := time.After(5 * time.Second)
	for {
		wait := mcm.waitNotify()
		mcm.mu.Lock()
		hasBulk := mcm.activeBulk != nil
		mcm.mu.Unlock()
		if hasBulk {
			break
		}
		select {
		case <-wait:
		case <-deadline:
			t.Fatal("retry did not install activeBulk")
		}
	}

	// Assert no duplicate cleanup before release
	if n := mgr.disconnCount.Load(); n != 1 {
		t.Errorf("before release: expected 1 cleanup call, got %d", n)
	}
	close(gate)

	select {
	case retryErr := <-retryDone:
		if retryErr != nil {
			t.Errorf("retry should succeed, got %v", retryErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retry did not complete")
	}

	if n := mgr.disconnCount.Load(); n != 1 {
		t.Errorf("expected exactly 1 cleanup call, got %d", n)
	}
	if n := registryLen(mcm); n != 0 {
		t.Errorf("current should be empty, got %d", n)
	}
	if n := retiringLen(mcm); n != 0 {
		t.Errorf("retiring should be empty, got %d", n)
	}

	// Connect should succeed after reconciliation
	if err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(300)); err != nil {
		t.Errorf("connect after reconciliation should work, got %v", err)
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

// --- Invariant 7: individual disconnect retries failed cleanup ---

type failOnceManager struct {
	mockManager
	calls atomic.Int32
}

func (m *failOnceManager) Disconnect() error {
	n := m.calls.Add(1)
	if n == 1 {
		return errors.New("transient cleanup error")
	}
	m.mu.Lock()
	m.connected = false
	m.mu.Unlock()
	return nil
}

func TestIndividualDisconnectRetriesFailedCleanup(t *testing.T) {
	var mgr *failOnceManager
	mcm := NewMultiConnectionManager(func() Manager {
		m := &failOnceManager{}
		mgr = m
		return m
	})

	if err := mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100)); err != nil {
		t.Fatalf("connect: %v", err)
	}

	err1 := mcm.Disconnect(bg(), 100)
	if err1 == nil {
		t.Fatal("first disconnect should fail")
	}
	if mgr.calls.Load() != 1 {
		t.Fatalf("expected 1 cleanup call, got %d", mgr.calls.Load())
	}
	if n := retiringLen(mcm); n != 1 {
		t.Fatalf("entry should remain retiring, got %d", n)
	}

	err2 := mcm.Disconnect(bg(), 100)
	if err2 != nil {
		t.Errorf("second disconnect should succeed, got %v", err2)
	}
	if mgr.calls.Load() != 2 {
		t.Errorf("expected 2 cleanup calls, got %d", mgr.calls.Load())
	}
	if n := retiringLen(mcm); n != 0 {
		t.Errorf("retiring should be empty, got %d", n)
	}
}

// --- Pre-canceled context tests ---

func TestPreCanceledConnectWrapsError(t *testing.T) {
	mcm, created := newTestMulti()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := mcm.Connect(ctx, dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))
	if !errors.Is(err, ErrConnectionCancelled) {
		t.Errorf("expected ErrConnectionCancelled, got %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if created.len() != 0 {
		t.Errorf("no manager should be created, got %d", created.len())
	}
}

func TestPreCanceledReconnectWrapsError(t *testing.T) {
	mcm, _ := newTestMulti()
	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := mcm.Reconnect(ctx, 100)
	if !errors.Is(err, ErrConnectionCancelled) {
		t.Errorf("expected ErrConnectionCancelled, got %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestReconnectRejectsRetiringPort(t *testing.T) {
	mcm, _ := newTestMulti()
	mcm.mu.Lock()
	mcm.retiring[100] = &portEntry{port: 100}
	mcm.mu.Unlock()

	err := mcm.Reconnect(bg(), 100)
	if !errors.Is(err, ErrLifecycleBusy) {
		t.Errorf("expected ErrLifecycleBusy for retiring port, got %v", err)
	}
}

// --- Bulk cleanup-error retry ---

func TestBulkCleanupErrorRetrySucceeds(t *testing.T) {
	gate := make(chan struct{})
	callCount := &atomic.Int32{}
	mcm, _ := newTestMulti()
	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))

	mgr := &blockingDisconnectManager{
		gate: gate,
		disconnectErrFn: func(n int32) error {
			callCount.Store(n)
			if n == 1 {
				return errors.New("transient error")
			}
			return nil
		},
	}
	mcm.mu.Lock()
	mcm.current[100].manager = mgr
	mcm.mu.Unlock()

	close(gate)
	err1 := mcm.Disconnect(bg(), -1)
	if err1 == nil {
		t.Fatal("first bulk should fail")
	}
	if !errors.Is(err1, ErrLifecycleBusy) {
		t.Errorf("expected ErrLifecycleBusy, got %v", err1)
	}

	mcm.mu.Lock()
	gen1 := mcm.generation
	mcm.mu.Unlock()

	err2 := mcm.Disconnect(bg(), -1)
	if err2 != nil {
		t.Errorf("second bulk should succeed, got %v", err2)
	}

	mcm.mu.Lock()
	gen2 := mcm.generation
	reconcile := mcm.reconcileRequired
	mcm.mu.Unlock()

	if gen1 != gen2 {
		t.Errorf("generation should not advance on retry: %d vs %d", gen1, gen2)
	}
	if reconcile {
		t.Error("reconcileRequired should be cleared after success")
	}
	if n := callCount.Load(); n != 2 {
		t.Errorf("expected 2 cleanup calls, got %d", n)
	}
}

// --- Caller-deadline continuation ---

func TestCallerDeadlineServerContinues(t *testing.T) {
	gate := make(chan struct{})
	disconnectEntered := make(chan struct{})
	mcm, _ := newTestMulti()
	mcm.Connect(bg(), dummyID(), dummyHermes(), dummyLookup(), dummyParams(100))

	mgr := &blockingDisconnectManager{gate: gate, disconnectEntered: disconnectEntered}
	mcm.mu.Lock()
	mcm.current[100].manager = mgr
	mcm.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	leaderDone := make(chan error, 1)
	go func() { leaderDone <- mcm.Disconnect(ctx, -1) }()

	select {
	case <-disconnectEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("Disconnect not entered")
	}

	select {
	case err := <-leaderDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("leader should get DeadlineExceeded, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("leader did not time out")
	}

	// Retry attaches to server-side cleanup that's still running.
	// Wait for retry to observe activeBulk before releasing gate.
	retryDone := make(chan error, 1)
	go func() { retryDone <- mcm.Disconnect(bg(), -1) }()

	deadline := time.After(5 * time.Second)
	for {
		wait := mcm.waitNotify()
		mcm.mu.Lock()
		hasBulk := mcm.activeBulk != nil
		mcm.mu.Unlock()
		if hasBulk {
			break
		}
		select {
		case <-wait:
		case <-deadline:
			t.Fatal("retry did not install activeBulk")
		}
	}

	close(gate)

	select {
	case err := <-retryDone:
		if err != nil {
			t.Errorf("retry should succeed, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retry did not complete")
	}

	if n := mgr.disconnCount.Load(); n != 1 {
		t.Errorf("expected exactly 1 cleanup call, got %d", n)
	}
}
