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
	"time"

	"github.com/rs/zerolog/log"
)

// entryPhase tracks the lifecycle state of a managed connection.
type entryPhase int

const (
	phaseConnecting entryPhase = iota
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
	priorPhase entryPhase // phase before retirement, for diagnostics

	// operationDone is closed when the Connect/Reconnect worker finishes.
	// Cleanup must wait for this before starting Disconnect.
	operationDone chan struct{}

	// opCancel cancels the coordinator-owned operation context passed to
	// the lifecycleManager. Extracted under mu, invoked outside mu by
	// individual/bulk retirement. Nil after worker finalizes.
	opCancel context.CancelFunc

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
	err  error         // result
}

// retire transitions an entry to phaseRetiring, preserving the prior phase
// for diagnostics. Must be called under mu.
func (e *portEntry) retire() {
	e.priorPhase = e.phase
	e.phase = phaseRetiring
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

	// Use DisconnectContext with hard deadline to prevent unbounded hangs
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), mcm.BulkTimeout)
	defer cleanupCancel()
	var err error
	if lm, ok := m.(lifecycleManager); ok {
		err = lm.DisconnectContext(cleanupCtx)
	} else {
		err = m.Disconnect()
	}

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

// disconnectAll performs authoritative bulk cleanup. Concurrent callers
// share the same operation. The bulk worker runs in its own server-owned
// goroutine — HTTP callers only wait on the result channel.
func (mcm *multiConnectionManager) disconnectAll(ctx context.Context) error {
	mcm.mu.Lock()

	// Attach to existing bulk operation if one is active
	if mcm.activeBulk != nil {
		bulk := mcm.activeBulk
		gen := mcm.generation
		mcm.mu.Unlock()
		select {
		case <-bulk.done:
			return bulk.err
		case <-ctx.Done():
			log.Warn().Uint64("generation", gen).
				Msg("Caller timed out while attached to shared bulk cleanup")
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
		e.retire()
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
		log.Warn().Uint64("generation", gen).
			Msg("Caller timed out while shared bulk cleanup continues server-side")
		return ctx.Err()
	}
}

// bulkWorker is the server-owned goroutine that drives bulk cleanup to
// completion. It cancels in-flight ops, starts missing cleanups, and waits
// for all retiring entries to resolve. Never exits until done or timed out.
func (mcm *multiConnectionManager) bulkWorker(bulk *bulkOp, gen uint64) {
	bulkStart := time.Now()
	log.Info().Uint64("generation", gen).Msg("Bulk disconnect started")

	// Collect managers and opCancels under lock, cancel outside
	mcm.mu.Lock()
	var toCancel []Manager
	var opCancels []context.CancelFunc
	for _, e := range mcm.retiring {
		if e.manager != nil {
			toCancel = append(toCancel, e.manager)
		}
		if e.opCancel != nil {
			opCancels = append(opCancels, e.opCancel)
			e.opCancel = nil
		}
	}
	// Start missing cleanup attempts
	for _, e := range mcm.retiring {
		if !e.cleanupRunning && e.cleanupAttempt == nil {
			go mcm.retireEntry(e)
		}
	}
	mcm.mu.Unlock()
	for _, cancel := range opCancels {
		cancel()
	}
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
			log.Info().Uint64("generation", gen).Dur("elapsed", time.Since(bulkStart)).
				Msg("Bulk disconnect completed successfully")
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
			nRetiring := len(mcm.retiring)
			mcm.activeBulk = nil
			bulk.err = err
			close(bulk.done)
			mcm.stateChanged()
			mcm.mu.Unlock()
			sort.Ints(pendingPorts)
			log.Error().Err(err).Uint64("generation", gen).
				Dur("elapsed", time.Since(bulkStart)).
				Int("retiring_count", nRetiring).Ints("pending_ports", pendingPorts).
				Msg("Bulk disconnect failed with cleanup error")
			return
		}

		wait := mcm.notify
		mcm.mu.Unlock()

		select {
		case <-wait:
			continue
		case <-deadline:
			mcm.mu.Lock()
			// Snapshot state for diagnostics — includes prior phase identity
			var cleanupRunningPorts, cleanupPendingPorts []int
			var wasConnecting, wasReconnecting, wasActive []int
			for port, e := range mcm.retiring {
				if e.cleanupRunning {
					cleanupRunningPorts = append(cleanupRunningPorts, port)
				} else {
					cleanupPendingPorts = append(cleanupPendingPorts, port)
				}
				switch e.priorPhase {
				case phaseConnecting:
					wasConnecting = append(wasConnecting, port)
				case phaseReconnecting:
					wasReconnecting = append(wasReconnecting, port)
				case phaseActive:
					wasActive = append(wasActive, port)
				}
			}
			nCurrent := len(mcm.current)
			nRetiring := len(mcm.retiring)

			if mcm.dumpedGen != gen {
				mcm.dumpedGen = gen
				mcm.mu.Unlock()
				sort.Ints(pendingPorts)
				sort.Ints(cleanupRunningPorts)
				sort.Ints(cleanupPendingPorts)
				sort.Ints(wasConnecting)
				sort.Ints(wasReconnecting)
				sort.Ints(wasActive)
				log.Warn().Uint64("generation", gen).
					Ints("pending_ports", pendingPorts).
					Ints("cleanup_running", cleanupRunningPorts).
					Ints("cleanup_pending", cleanupPendingPorts).
					Ints("was_connecting", wasConnecting).
					Ints("was_reconnecting", wasReconnecting).
					Ints("was_active", wasActive).
					Int("current_count", nCurrent).Int("retiring_count", nRetiring).
					Dur("elapsed", time.Since(bulkStart)).
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
			log.Error().Err(err).Uint64("generation", gen).
				Dur("elapsed", time.Since(bulkStart)).
				Msg("Bulk disconnect failed")
			return
		}
	}
}
