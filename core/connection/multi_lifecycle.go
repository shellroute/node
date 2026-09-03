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
