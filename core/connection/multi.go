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

	// cleanupRunning is true while a Disconnect call is in-flight on the manager.
	cleanupRunning bool
	// cleanupDone is closed when cleanup completes. Nil until cleanup starts.
	cleanupDone chan struct{}
	// cleanupErr holds the last cleanup error (nil = success or ErrNoConnection).
	cleanupErr error
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
// Returns ErrLifecycleBusy if reconciliation is active or the port is retiring.
func (mcm *multiConnectionManager) Connect(ctx context.Context, consumerID identity.Identity, hermesID common.Address, proposalLookup ProposalLookup, params ConnectParams) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	m := mcm.newConnectionManager()

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

	entry := &portEntry{
		port:       params.ProxyPort,
		generation: mcm.generation,
		manager:    m,
		phase:      phaseConnecting,
	}
	mcm.current[params.ProxyPort] = entry
	gen := mcm.generation
	mcm.stateChanged()
	mcm.mu.Unlock()

	// Run connect outside lock — cancellable via context
	type connectResult struct{ err error }
	resultCh := make(chan connectResult, 1)
	go func() {
		resultCh <- connectResult{m.Connect(consumerID, hermesID, proposalLookup, params)}
	}()

	var err error
	select {
	case r := <-resultCh:
		err = r.err
	case <-ctx.Done():
		// Caller cancelled — retire the entry
		mcm.mu.Lock()
		if e, ok := mcm.current[params.ProxyPort]; ok && e == entry {
			delete(mcm.current, params.ProxyPort)
			entry.phase = phaseRetiring
			mcm.retiring[params.ProxyPort] = entry
			mcm.stateChanged()
		}
		mcm.mu.Unlock()
		// Request cancellation on the manager
		mcm.cancelManager(m)
		// Start cleanup in background
		go mcm.retireEntry(entry)
		return fmt.Errorf("%w: %w", ErrConnectionCancelled, ctx.Err())
	}

	mcm.mu.Lock()
	e, stillCurrent := mcm.current[params.ProxyPort]
	if err == nil && stillCurrent && e == entry && mcm.generation == gen && !mcm.reconcileRequired && ctx.Err() == nil {
		entry.phase = phaseActive
		mcm.stateChanged()
		mcm.mu.Unlock()
		return nil
	}
	// Stale, superseded, or failed — retire
	if stillCurrent && e == entry {
		delete(mcm.current, params.ProxyPort)
	}
	entry.phase = phaseRetiring
	mcm.retiring[params.ProxyPort] = entry
	mcm.stateChanged()
	mcm.mu.Unlock()
	go mcm.retireEntry(entry)

	if err != nil {
		return err
	}
	return fmt.Errorf("%w: connect superseded by bulk cleanup", ErrLifecycleBusy)
}

// Status queries current status of connection.
func (mcm *multiConnectionManager) Status(id int) connectionstate.Status {
	mcm.mu.Lock()
	e, ok := mcm.current[id]
	mcm.mu.Unlock()
	if ok {
		return e.manager.Status()
	}
	return connectionstate.Status{State: connectionstate.NotConnected}
}

// Stats provides connection statistics information.
func (mcm *multiConnectionManager) Stats(id int) connectionstate.Statistics {
	mcm.mu.Lock()
	e, ok := mcm.current[id]
	mcm.mu.Unlock()
	if ok {
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
	if entry.cleanupDone == nil {
		// Start cleanup
		go mcm.retireEntry(entry)
	}
	done := entry.cleanupDone
	mcm.mu.Unlock()

	// Wait for cleanup channel to exist if it didn't yet
	if done == nil {
		for {
			wait := mcm.waitNotify()
			mcm.mu.Lock()
			done = entry.cleanupDone
			mcm.mu.Unlock()
			if done != nil {
				break
			}
			select {
			case <-wait:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	select {
	case <-done:
		mcm.mu.Lock()
		err := entry.cleanupErr
		mcm.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// retireEntry runs cleanup on an entry's manager. Only one cleanup per entry.
func (mcm *multiConnectionManager) retireEntry(entry *portEntry) {
	mcm.mu.Lock()
	if entry.cleanupRunning {
		mcm.mu.Unlock()
		return
	}
	entry.cleanupRunning = true
	done := make(chan struct{})
	entry.cleanupDone = done
	mcm.stateChanged()
	mcm.mu.Unlock()

	err := entry.manager.Disconnect()

	mcm.mu.Lock()
	if err == nil || errors.Is(err, ErrNoConnection) {
		entry.cleanupErr = nil
		delete(mcm.retiring, entry.port)
	} else {
		entry.cleanupErr = err
		log.Error().Err(err).Int("port", entry.port).Msg("Connection cleanup failed")
	}
	entry.cleanupRunning = false
	close(done)
	mcm.stateChanged()
	mcm.mu.Unlock()
}

// disconnectAll performs authoritative bulk cleanup. Concurrent callers
// share the same operation. Returns nil only when all covered entries
// retired successfully.
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

	// Start new bulk operation
	mcm.reconcileRequired = true
	mcm.generation++
	gen := mcm.generation

	// Move all current entries to retiring
	for port, e := range mcm.current {
		e.phase = phaseRetiring
		mcm.retiring[port] = e
		delete(mcm.current, port)
	}

	bulk := &bulkOp{done: make(chan struct{})}
	mcm.activeBulk = bulk
	mcm.stateChanged()
	mcm.mu.Unlock()

	log.Info().Uint64("generation", gen).Msg("Bulk disconnect started")

	// Cancel in-flight operations
	mcm.mu.Lock()
	var managers []Manager
	for _, e := range mcm.retiring {
		managers = append(managers, e.manager)
	}
	mcm.mu.Unlock()
	for _, m := range managers {
		mcm.cancelManager(m)
	}

	// Start missing cleanup attempts
	mcm.mu.Lock()
	for _, e := range mcm.retiring {
		if !e.cleanupRunning && e.cleanupDone == nil {
			go mcm.retireEntry(e)
		}
	}
	mcm.mu.Unlock()

	// Wait for all retiring entries to complete
	deadline := time.After(mcm.BulkTimeout)
	for {
		mcm.mu.Lock()
		allDone := true
		var pendingPorts []int
		for port, e := range mcm.retiring {
			if e.cleanupErr != nil || e.cleanupRunning || e.cleanupDone == nil {
				allDone = false
				pendingPorts = append(pendingPorts, port)
			} else {
				// Check if done channel is closed
				select {
				case <-e.cleanupDone:
					if e.cleanupErr != nil {
						allDone = false
						pendingPorts = append(pendingPorts, port)
					}
				default:
					allDone = false
					pendingPorts = append(pendingPorts, port)
				}
			}
		}

		if len(mcm.retiring) == 0 {
			allDone = true
		}

		if allDone {
			mcm.reconcileRequired = false
			mcm.activeBulk = nil
			bulk.err = nil
			close(bulk.done)
			mcm.stateChanged()
			mcm.mu.Unlock()
			log.Info().Uint64("generation", gen).Msg("Bulk disconnect completed successfully")
			return nil
		}

		wait := mcm.notify
		mcm.mu.Unlock()

		select {
		case <-wait:
			continue
		case <-deadline:
			// Timeout — dump goroutines once per generation
			mcm.mu.Lock()
			if mcm.dumpedGen != gen {
				mcm.dumpedGen = gen
				mcm.mu.Unlock()
				log.Warn().Uint64("generation", gen).Ints("pending_ports", pendingPorts).
					Msg("Bulk disconnect timeout — dumping goroutines")
				pprof.Lookup("goroutine").WriteTo(log.Logger, 2)
			} else {
				mcm.mu.Unlock()
			}

			sort.Ints(pendingPorts)
			err := fmt.Errorf("bulk disconnect timeout after %s, pending ports: %v", mcm.BulkTimeout, pendingPorts)
			mcm.mu.Lock()
			mcm.activeBulk = nil
			bulk.err = err
			close(bulk.done)
			mcm.stateChanged()
			mcm.mu.Unlock()
			log.Error().Err(err).Uint64("generation", gen).Msg("Bulk disconnect failed")
			return err
		case <-ctx.Done():
			// Caller gave up but cleanup continues server-side
			mcm.mu.Lock()
			mcm.mu.Unlock()
			return ctx.Err()
		}
	}
}

// Reconnect disconnects and reconnects on the given port.
func (mcm *multiConnectionManager) Reconnect(ctx context.Context, id int) error {
	mcm.mu.Lock()
	e, ok := mcm.current[id]
	if !ok {
		mcm.mu.Unlock()
		return ErrNoConnection
	}
	m := e.manager
	mcm.mu.Unlock()

	m.Reconnect()
	return nil
}

// CheckChannel checks if current session channel is alive.
func (mcm *multiConnectionManager) CheckChannel(ctx context.Context) error { return nil }

// cancelManager requests non-blocking cancellation on a manager.
func (mcm *multiConnectionManager) cancelManager(m Manager) {
	type canceller interface {
		CancelCurrentOperation()
	}
	if c, ok := m.(canceller); ok {
		c.CancelCurrentOperation()
	}
}
