/*
 * Copyright (C) 2022 The "MysteriumNetwork/node" Authors.
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
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog/log"

	"github.com/mysteriumnetwork/node/core/connection/connectionstate"
	"github.com/mysteriumnetwork/node/identity"
)

type multiConnectionManager struct {
	// mu protects all mutable state. Never held during Manager network calls.
	mu sync.Mutex

	// current entries by port — only connected/connecting/reconnecting managers.
	current map[int]*portEntry
	// retiring entries by port — managers being cleaned up. May coexist with
	// a current entry on the same port (the current one is newer).
	retiring map[int]*portEntry

	generation        uint64
	reconcileRequired bool

	// activeBulk is the in-flight shared bulk operation, if any.
	activeBulk *bulkOp

	// notify is closed and replaced whenever state changes, so waiters
	// can re-check without polling.
	notify chan struct{}

	// BulkTimeout is the hard deadline for authoritative bulk cleanup.
	// Configurable for tests; default 30s.
	BulkTimeout time.Duration

	// dumpedGen tracks which generation already produced a goroutine dump.
	dumpedGen uint64

	newConnectionManager func() Manager
}

// NewMultiConnectionManager creates a wrapper around connection manager
// to support multiple connections with lifecycle coordination.
func NewMultiConnectionManager(newConnectionManager func() Manager) *multiConnectionManager {
	return &multiConnectionManager{
		current:              make(map[int]*portEntry),
		retiring:             make(map[int]*portEntry),
		notify:               make(chan struct{}),
		BulkTimeout:          30 * time.Second,
		newConnectionManager: newConnectionManager,
	}
}

// Connect creates a new connection on the given port.
// The port is reserved atomically before creating the manager. The worker
// owns finalization — cleanup waits for the operation to finish first.
func (mcm *multiConnectionManager) Connect(ctx context.Context, consumerID identity.Identity, hermesID common.Address, proposalLookup ProposalLookup, params ConnectParams) (retErr error) {
	start := time.Now()
	defer func() {
		mcm.mu.Lock()
		gen := mcm.generation
		mcm.mu.Unlock()
		log.Debug().Int("port", params.ProxyPort).Str("op", "connect").
			Uint64("generation", gen).Err(retErr).
			Dur("elapsed", time.Since(start)).Msg("Connect finished")
	}()
	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrConnectionCancelled, ctx.Err())
	}

	// Reserve port atomically before creating manager
	mcm.mu.Lock()
	if mcm.reconcileRequired {
		mcm.mu.Unlock()
		return ErrLifecycleBusy
	}
	if _, retiring := mcm.retiring[params.ProxyPort]; retiring {
		mcm.mu.Unlock()
		return ErrLifecycleBusy
	}
	if _, exists := mcm.current[params.ProxyPort]; exists {
		mcm.mu.Unlock()
		return ErrAlreadyExists
	}
	// Reserve with nil manager — filled after construction.
	// Create coordinator-owned operation context derived from caller ctx.
	opCtx, opCancel := context.WithCancel(ctx)
	opDone := make(chan struct{})
	entry := &portEntry{
		port:          params.ProxyPort,
		generation:    mcm.generation,
		phase:         phaseConnecting,
		operationDone: opDone,
		opCancel:      opCancel,
	}
	mcm.current[params.ProxyPort] = entry
	gen := mcm.generation
	mcm.stateChanged()
	mcm.mu.Unlock()

	// Create manager after reservation is held
	m := mcm.newConnectionManager()
	mcm.mu.Lock()
	entry.manager = m
	// Check if superseded during factory construction (bulk/ctx may have advanced)
	if entry.phase == phaseRetiring || mcm.generation != gen || mcm.reconcileRequired || opCtx.Err() != nil {
		entry.retire()
		entry.opCancel = nil
		if mcm.current[params.ProxyPort] == entry {
			delete(mcm.current, params.ProxyPort)
		}
		mcm.retiring[params.ProxyPort] = entry
		close(opDone)
		mcm.stateChanged()
		mcm.mu.Unlock()
		opCancel()
		go mcm.retireEntry(entry)
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %w: %w", ErrConnectionCancelled, ErrLifecycleBusy, ctx.Err())
		}
		return fmt.Errorf("%w: %w: superseded during construction", ErrConnectionCancelled, ErrLifecycleBusy)
	}
	mcm.mu.Unlock()

	// resultErr is the error returned to the caller. Set by worker before
	// closing opDone. Distinct from the raw connect error when superseded.
	var resultErr error
	go func() {
		var connectErr error
		if lm, ok := m.(lifecycleManager); ok {
			connectErr = lm.ConnectContext(opCtx, consumerID, hermesID, proposalLookup, params)
		} else {
			connectErr = m.Connect(consumerID, hermesID, proposalLookup, params)
		}

		mcm.mu.Lock()
		entry.opCancel = nil // worker done — clear cancel

		if entry.phase == phaseRetiring {
			// Caller or bulk cancelled — we own cleanup
			resultErr = fmt.Errorf("%w: %w", ErrConnectionCancelled, ErrLifecycleBusy)
			close(opDone)
			mcm.mu.Unlock()
			mcm.retireEntry(entry)
			return
		}

		if connectErr == nil && mcm.current[params.ProxyPort] == entry &&
			mcm.generation == gen && !mcm.reconcileRequired && opCtx.Err() == nil {
			entry.phase = phaseActive
			resultErr = nil
			close(opDone)
			mcm.stateChanged()
			mcm.mu.Unlock()
			return
		}

		// Failed, stale, or superseded — retire
		if mcm.current[params.ProxyPort] == entry {
			delete(mcm.current, params.ProxyPort)
		}
		entry.retire()
		mcm.retiring[params.ProxyPort] = entry
		if connectErr != nil {
			resultErr = connectErr
		} else {
			resultErr = fmt.Errorf("%w: connect superseded", ErrLifecycleBusy)
		}
		close(opDone)
		mcm.stateChanged()
		mcm.mu.Unlock()
		mcm.retireEntry(entry)
	}()

	select {
	case <-opDone:
		return resultErr
	case <-ctx.Done():
		mcm.mu.Lock()
		// Worker may have already completed — check before retiring
		if entry.phase == phaseActive {
			// Worker already published success — linearize to success
			mcm.mu.Unlock()
			return nil
		}
		alreadyRetiring := entry.phase == phaseRetiring
		if mcm.current[params.ProxyPort] == entry && entry.phase == phaseConnecting {
			delete(mcm.current, params.ProxyPort)
			entry.retire()
			mcm.retiring[params.ProxyPort] = entry
			mcm.stateChanged()
		}
		cancelFn := entry.opCancel
		entry.opCancel = nil
		mcm.mu.Unlock()
		if cancelFn != nil {
			cancelFn()
		}
		mcm.cancelManager(m)
		if alreadyRetiring {
			return fmt.Errorf("%w: %w: %w", ErrConnectionCancelled, ErrLifecycleBusy, ctx.Err())
		}
		return fmt.Errorf("%w: %w", ErrConnectionCancelled, ctx.Err())
	}
}

// Status queries current status of connection.
// Safe during any phase — manager.Status() uses its own statusLock.
func (mcm *multiConnectionManager) Status(id int) connectionstate.Status {
	mcm.mu.Lock()
	e, ok := mcm.current[id]
	mcm.mu.Unlock()
	if ok && e.manager != nil {
		return e.manager.Status()
	}
	return connectionstate.Status{State: connectionstate.NotConnected}
}

// Stats provides connection statistics information.
// Only returns stats from active entries — connecting/reconnecting managers
// may not have initialized statsTracker yet (plan invariant 8).
func (mcm *multiConnectionManager) Stats(id int) connectionstate.Statistics {
	mcm.mu.Lock()
	e, ok := mcm.current[id]
	var m Manager
	if ok && e.phase == phaseActive && e.manager != nil {
		m = e.manager
	}
	mcm.mu.Unlock()
	if m != nil {
		return m.Stats()
	}
	return connectionstate.Statistics{}
}

// Disconnect closes an established connection. id < 0 triggers authoritative
// bulk cleanup. Returns nil only after cleanup completes successfully.
func (mcm *multiConnectionManager) Disconnect(ctx context.Context, id int) (retErr error) {
	if id < 0 {
		return mcm.disconnectAll(ctx)
	}
	start := time.Now()
	defer func() {
		mcm.mu.Lock()
		gen := mcm.generation
		mcm.mu.Unlock()
		log.Debug().Int("port", id).Str("op", "disconnect").
			Uint64("generation", gen).Err(retErr).
			Dur("elapsed", time.Since(start)).Msg("Disconnect finished")
	}()

	mcm.mu.Lock()
	e, ok := mcm.current[id]
	if !ok {
		// Check if already retiring
		if re, rok := mcm.retiring[id]; rok {
			mcm.mu.Unlock()
			return mcm.waitRetirement(ctx, re)
		}
		mcm.mu.Unlock()
		return nil
	}

	delete(mcm.current, id)
	e.retire()
	mcm.retiring[id] = e
	cancelFn := e.opCancel
	e.opCancel = nil
	mcm.stateChanged()
	mcm.mu.Unlock()

	// Cancel coordinator-owned operation context + manager
	if cancelFn != nil {
		cancelFn()
	}
	mcm.cancelManager(e.manager)

	return mcm.waitRetirement(ctx, e)
}

// Reconnect disconnects and reconnects on the given port.
// Uses the same worker-owned pattern as Connect: new operationDone,
// ctx select, cleanup waits for operation completion.
func (mcm *multiConnectionManager) Reconnect(ctx context.Context, id int) (retErr error) {
	start := time.Now()
	defer func() {
		mcm.mu.Lock()
		gen := mcm.generation
		mcm.mu.Unlock()
		log.Debug().Int("port", id).Str("op", "reconnect").
			Uint64("generation", gen).Err(retErr).
			Dur("elapsed", time.Since(start)).Msg("Reconnect finished")
	}()
	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrConnectionCancelled, ctx.Err())
	}

	mcm.mu.Lock()
	if mcm.reconcileRequired {
		mcm.mu.Unlock()
		return ErrLifecycleBusy
	}
	e, ok := mcm.current[id]
	if !ok {
		if _, retiring := mcm.retiring[id]; retiring {
			mcm.mu.Unlock()
			return ErrLifecycleBusy
		}
		mcm.mu.Unlock()
		return ErrNoConnection
	}
	if e.manager == nil || e.phase == phaseReconnecting || e.phase == phaseConnecting {
		mcm.mu.Unlock()
		return ErrLifecycleBusy
	}
	gen := mcm.generation
	m := e.manager
	e.phase = phaseReconnecting
	// Coordinator-owned operation context for this reconnect
	opCtx, opCancel := context.WithCancel(ctx)
	opDone := make(chan struct{})
	e.operationDone = opDone
	e.opCancel = opCancel
	mcm.stateChanged()
	mcm.mu.Unlock()

	var resultErr error
	go func() {
		var reconErr error
		if lm, ok := m.(lifecycleManager); ok {
			reconErr = lm.ReconnectContext(opCtx)
		} else {
			m.Reconnect()
		}

		mcm.mu.Lock()
		e.opCancel = nil // worker done — clear cancel

		if e.phase == phaseRetiring {
			resultErr = fmt.Errorf("%w: %w", ErrConnectionCancelled, ErrLifecycleBusy)
			close(opDone)
			mcm.mu.Unlock()
			mcm.retireEntry(e)
			return
		}

		if reconErr == nil && mcm.generation == gen && !mcm.reconcileRequired && opCtx.Err() == nil {
			e.phase = phaseActive
			resultErr = nil
			close(opDone)
			mcm.stateChanged()
			mcm.mu.Unlock()
			return
		}

		// Failed, superseded, or stale — retire
		if mcm.current[id] == e {
			delete(mcm.current, id)
		}
		e.retire()
		mcm.retiring[id] = e
		if reconErr != nil {
			resultErr = reconErr
		} else {
			resultErr = ErrLifecycleBusy
		}
		close(opDone)
		mcm.stateChanged()
		mcm.mu.Unlock()
		mcm.retireEntry(e)
	}()

	select {
	case <-opDone:
		return resultErr
	case <-ctx.Done():
		mcm.mu.Lock()
		if e.phase == phaseActive {
			mcm.mu.Unlock()
			return nil
		}
		if mcm.current[id] == e && e.phase == phaseReconnecting {
			delete(mcm.current, id)
			e.retire()
			mcm.retiring[id] = e
			mcm.stateChanged()
		}
		cancelFn := e.opCancel
		e.opCancel = nil
		mcm.mu.Unlock()
		if cancelFn != nil {
			cancelFn()
		}
		mcm.cancelManager(m)
		return fmt.Errorf("%w: %w", ErrConnectionCancelled, ctx.Err())
	}
}

// CheckChannel checks if current session channel is alive.
func (mcm *multiConnectionManager) CheckChannel(ctx context.Context) error { return nil }
