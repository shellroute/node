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
	"errors"
	"fmt"
	"runtime/pprof"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog/log"

	"github.com/mysteriumnetwork/node/core/connection/connectionstate"
	"github.com/mysteriumnetwork/node/identity"
)

// entryPhase tracks the lifecycle state of a managed connection.
type entryPhase int

const (
	phaseConnecting   entryPhase = iota
	phaseActive
	phaseReconnecting
	phaseRetiring
)

func (p entryPhase) String() string {
	switch p {
	case phaseConnecting:
		return "connecting"
	case phaseActive:
		return "active"
	case phaseReconnecting:
		return "reconnecting"
	case phaseRetiring:
		return "retiring"
	default:
		return "unknown"
	}
}

// portEntry tracks one managed connection's lifecycle.
// All fields are protected by the coordinator's mu.
type portEntry struct {
	port       int
	generation uint64
	manager    Manager
	phase      entryPhase

	// operationDone is closed when the Connect/Reconnect worker finishes.
	// Cleanup must wait for this before starting Disconnect.
	operationDone chan struct{}

	// cleanupRunning is true while a Disconnect call is in-flight on the manager.
	cleanupRunning bool
	// cleanupAttempt points to the current cleanup attempt. Nil until cleanup starts.
	// Each attempt is immutable once done is closed — retries create a new attempt.
	cleanupAttempt *cleanupAttempt
}

// cleanupAttempt is an immutable result container for one cleanup try.
// Once done is closed, err is final and never mutated.
type cleanupAttempt struct {
	done chan struct{}
	err  error
}

// bulkOp represents a shared authoritative bulk cleanup operation.
type bulkOp struct {
	done chan struct{} // closed when the operation completes
	err  error        // result
}

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

// stateChanged closes the current notify channel and replaces it.
// Must be called under mu.
func (mcm *multiConnectionManager) stateChanged() {
	close(mcm.notify)
	mcm.notify = make(chan struct{})
}

// waitNotify returns a channel that will be closed on the next state change.
func (mcm *multiConnectionManager) waitNotify() <-chan struct{} {
	mcm.mu.Lock()
	ch := mcm.notify
	mcm.mu.Unlock()
	return ch
}

// Connect creates a new connection on the given port.
// The port is reserved atomically before creating the manager. The worker
// owns finalization — cleanup waits for the operation to finish first.
func (mcm *multiConnectionManager) Connect(ctx context.Context, consumerID identity.Identity, hermesID common.Address, proposalLookup ProposalLookup, params ConnectParams) error {
	if ctx.Err() != nil {
		return ctx.Err()
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
	// Reserve with nil manager — filled after construction
	opDone := make(chan struct{})
	entry := &portEntry{
		port:          params.ProxyPort,
		generation:    mcm.generation,
		phase:         phaseConnecting,
		operationDone: opDone,
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
	if entry.phase == phaseRetiring || mcm.generation != gen || mcm.reconcileRequired || ctx.Err() != nil {
		entry.phase = phaseRetiring
		if mcm.current[params.ProxyPort] == entry {
			delete(mcm.current, params.ProxyPort)
		}
		mcm.retiring[params.ProxyPort] = entry
		close(opDone)
		mcm.stateChanged()
		mcm.mu.Unlock()
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
		connectErr := m.Connect(consumerID, hermesID, proposalLookup, params)

		mcm.mu.Lock()

		if entry.phase == phaseRetiring {
			// Caller or bulk cancelled — we own cleanup
			resultErr = fmt.Errorf("%w: %w", ErrConnectionCancelled, ErrLifecycleBusy)
			close(opDone)
			mcm.mu.Unlock()
			mcm.retireEntry(entry)
			return
		}

		if connectErr == nil && mcm.current[params.ProxyPort] == entry &&
			mcm.generation == gen && !mcm.reconcileRequired && ctx.Err() == nil {
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
		entry.phase = phaseRetiring
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
			entry.phase = phaseRetiring
			mcm.retiring[params.ProxyPort] = entry
			mcm.stateChanged()
		}
		mcm.mu.Unlock()
		mcm.cancelManager(m)
		if alreadyRetiring {
			return fmt.Errorf("%w: %w: %w", ErrConnectionCancelled, ErrLifecycleBusy, ctx.Err())
		}
		return fmt.Errorf("%w: %w", ErrConnectionCancelled, ctx.Err())
	}
}

// Status queries current status of connection.
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
func (mcm *multiConnectionManager) Stats(id int) connectionstate.Statistics {
	mcm.mu.Lock()
	e, ok := mcm.current[id]
	mcm.mu.Unlock()
	if ok && e.manager != nil {
		return e.manager.Stats()
	}
	return connectionstate.Statistics{}
}

// Disconnect closes an established connection. id < 0 triggers authoritative
// bulk cleanup. Returns nil only after cleanup completes successfully.
func (mcm *multiConnectionManager) Disconnect(ctx context.Context, id int) error {
	if id < 0 {
		return mcm.disconnectAll(ctx)
	}

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
	e.phase = phaseRetiring
	mcm.retiring[id] = e
	mcm.stateChanged()
	mcm.mu.Unlock()

	// Cancel in-flight connect/reconnect
	mcm.cancelManager(e.manager)

	return mcm.waitRetirement(ctx, e)
}

// waitRetirement waits for an entry's cleanup to complete or ctx to expire.
func (mcm *multiConnectionManager) waitRetirement(ctx context.Context, entry *portEntry) error {
	mcm.mu.Lock()
	if entry.cleanupAttempt == nil {
		go mcm.retireEntry(entry)
	}
	att := entry.cleanupAttempt
	mcm.mu.Unlock()

	// Wait for attempt to exist if it didn't yet
	if att == nil {
		for {
			wait := mcm.waitNotify()
			mcm.mu.Lock()
			att = entry.cleanupAttempt
			mcm.mu.Unlock()
			if att != nil {
				break
			}
			select {
			case <-wait:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// Wait on the captured attempt — immutable once done is closed
	select {
	case <-att.done:
		return att.err // immutable: never overwritten by retry
	case <-ctx.Done():
		return ctx.Err()
	}
}

// retireEntry runs cleanup on an entry's manager. Waits for any in-flight
// operation (Connect/Reconnect) to finish first. Only one cleanup per entry.
func (mcm *multiConnectionManager) retireEntry(entry *portEntry) {
	// Claim cleanup ownership under lock first — prevents double cleanup
	mcm.mu.Lock()
	if entry.cleanupRunning || entry.cleanupAttempt != nil {
		mcm.mu.Unlock()
		return // already started or completed
	}
	entry.cleanupRunning = true
	attempt := &cleanupAttempt{done: make(chan struct{})}
	entry.cleanupAttempt = attempt
	mcm.stateChanged()
	mcm.mu.Unlock()

	// Wait for the operation (Connect/factory) to finish before disconnecting
	if entry.operationDone != nil {
		<-entry.operationDone
	}

	// After operation finished, check if manager was ever created
	mcm.mu.Lock()
	m := entry.manager
	mcm.mu.Unlock()
	if m == nil {
		// Factory never completed or entry was a bare reservation — nothing to disconnect
		mcm.mu.Lock()
		delete(mcm.retiring, entry.port)
		entry.cleanupRunning = false
		close(attempt.done)
		mcm.stateChanged()
		mcm.mu.Unlock()
		return
	}

	err := m.Disconnect()

	mcm.mu.Lock()
	if err == nil || errors.Is(err, ErrNoConnection) {
		attempt.err = nil
		delete(mcm.retiring, entry.port)
	} else {
		attempt.err = err
		log.Error().Err(err).Int("port", entry.port).Msg("Connection cleanup failed")
	}
	entry.cleanupRunning = false
	close(attempt.done)
	mcm.stateChanged()
	mcm.mu.Unlock()
}

// disconnectAll performs authoritative bulk cleanup. Concurrent callers
// share the same operation. The bulk worker runs in its own server-owned
// goroutine — HTTP callers only wait on the result channel.
func (mcm *multiConnectionManager) disconnectAll(ctx context.Context) error {
	mcm.mu.Lock()

	// Attach to existing bulk operation if one is active
	if mcm.activeBulk != nil {
		bulk := mcm.activeBulk
		mcm.mu.Unlock()
		select {
		case <-bulk.done:
			return bulk.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Start new bulk operation — server-owned goroutine
	// Only advance generation on first reconcile; retries keep the same generation
	if !mcm.reconcileRequired {
		mcm.reconcileRequired = true
		mcm.generation++
	}
	gen := mcm.generation

	// Move current entries to retiring
	for port, e := range mcm.current {
		e.phase = phaseRetiring
		mcm.retiring[port] = e
		delete(mcm.current, port)
	}

	// Clear prior failed attempts so retireEntry can create a new one
	for _, e := range mcm.retiring {
		if e.cleanupAttempt != nil && e.cleanupAttempt.err != nil && !e.cleanupRunning {
			e.cleanupAttempt = nil // old attempt stays immutable; new one created on retry
		}
	}

	bulk := &bulkOp{done: make(chan struct{})}
	mcm.activeBulk = bulk
	mcm.stateChanged()
	mcm.mu.Unlock()

	// Launch server-owned bulk worker
	go mcm.bulkWorker(bulk, gen)

	// Caller waits for result or ctx cancellation
	select {
	case <-bulk.done:
		return bulk.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// bulkWorker is the server-owned goroutine that drives bulk cleanup to
// completion. It cancels in-flight ops, starts missing cleanups, and waits
// for all retiring entries to resolve. Never exits until done or timed out.
func (mcm *multiConnectionManager) bulkWorker(bulk *bulkOp, gen uint64) {
	log.Info().Uint64("generation", gen).Msg("Bulk disconnect started")

	// Collect managers under lock, cancel outside
	mcm.mu.Lock()
	var toCancel []Manager
	for _, e := range mcm.retiring {
		if e.manager != nil {
			toCancel = append(toCancel, e.manager)
		}
	}
	// Start missing cleanup attempts
	for _, e := range mcm.retiring {
		if !e.cleanupRunning && e.cleanupAttempt == nil {
			go mcm.retireEntry(e)
		}
	}
	mcm.mu.Unlock()
	for _, m := range toCancel {
		mcm.cancelManager(m)
	}

	deadline := time.After(mcm.BulkTimeout)
	for {
		mcm.mu.Lock()
		if len(mcm.retiring) == 0 {
			mcm.reconcileRequired = false
			mcm.activeBulk = nil
			bulk.err = nil
			close(bulk.done)
			mcm.stateChanged()
			mcm.mu.Unlock()
			log.Info().Uint64("generation", gen).Msg("Bulk disconnect completed successfully")
			return
		}

		// Check for cleanup errors — fail the bulk attempt immediately,
		// retain reconcileRequired so next request retries
		var pendingPorts []int
		var cleanupErrs []error
		for port, e := range mcm.retiring {
			if e.cleanupAttempt != nil && e.cleanupAttempt.err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("port %d: %w", port, e.cleanupAttempt.err))
			}
			if e.cleanupRunning || e.cleanupAttempt == nil || (e.cleanupAttempt != nil && e.cleanupAttempt.err != nil) {
				pendingPorts = append(pendingPorts, port)
			}
		}
		if len(cleanupErrs) > 0 {
			// End this bulk attempt — keep reconcileRequired, don't advance generation
			err := fmt.Errorf("%w: bulk cleanup failed: %w", ErrLifecycleBusy, errors.Join(cleanupErrs...))
			mcm.activeBulk = nil
			bulk.err = err
			close(bulk.done)
			mcm.stateChanged()
			mcm.mu.Unlock()
			log.Error().Err(err).Uint64("generation", gen).Msg("Bulk disconnect failed with cleanup error")
			return
		}

		wait := mcm.notify
		mcm.mu.Unlock()

		select {
		case <-wait:
			continue
		case <-deadline:
			mcm.mu.Lock()
			if mcm.dumpedGen != gen {
				mcm.dumpedGen = gen
				mcm.mu.Unlock()
				sort.Ints(pendingPorts)
				log.Warn().Uint64("generation", gen).Ints("pending_ports", pendingPorts).
					Msg("Bulk disconnect timeout — dumping goroutines")
				pprof.Lookup("goroutine").WriteTo(log.Logger, 2)
			} else {
				mcm.mu.Unlock()
			}

			sort.Ints(pendingPorts)
			err := fmt.Errorf("%w: bulk disconnect timeout after %s, pending ports: %v", ErrLifecycleBusy, mcm.BulkTimeout, pendingPorts)
			mcm.mu.Lock()
			mcm.activeBulk = nil
			bulk.err = err
			close(bulk.done)
			mcm.stateChanged()
			mcm.mu.Unlock()
			log.Error().Err(err).Uint64("generation", gen).Msg("Bulk disconnect failed")
			return
		}
	}
}

// Reconnect disconnects and reconnects on the given port.
// Rejects if reconciliation is active (prevents restoring a superseded connection).
func (mcm *multiConnectionManager) Reconnect(ctx context.Context, id int) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	mcm.mu.Lock()
	if mcm.reconcileRequired {
		mcm.mu.Unlock()
		return ErrLifecycleBusy
	}
	e, ok := mcm.current[id]
	if !ok {
		mcm.mu.Unlock()
		return ErrNoConnection
	}
	if e.manager == nil {
		mcm.mu.Unlock()
		return ErrNoConnection
	}
	gen := mcm.generation
	m := e.manager
	e.phase = phaseReconnecting
	mcm.stateChanged()
	mcm.mu.Unlock()

	m.Reconnect()

	// After reconnect, verify generation hasn't advanced (bulk may have run)
	mcm.mu.Lock()
	if mcm.generation != gen || mcm.reconcileRequired {
		// Superseded — retire this entry
		if mcm.current[id] == e {
			delete(mcm.current, id)
			e.phase = phaseRetiring
			mcm.retiring[id] = e
			mcm.stateChanged()
		}
		mcm.mu.Unlock()
		go mcm.retireEntry(e)
		return ErrLifecycleBusy
	}
	e.phase = phaseActive
	mcm.stateChanged()
	mcm.mu.Unlock()
	return nil
}

// CheckChannel checks if current session channel is alive.
func (mcm *multiConnectionManager) CheckChannel(ctx context.Context) error { return nil }

// cancelManager requests non-blocking cancellation on a manager.
func (mcm *multiConnectionManager) cancelManager(m Manager) {
	if m == nil {
		return
	}
	type canceller interface {
		CancelCurrentOperation()
	}
	if c, ok := m.(canceller); ok {
		c.CancelCurrentOperation()
	}
}
