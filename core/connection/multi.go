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
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog/log"

	"github.com/mysteriumnetwork/node/core/connection/connectionstate"
	"github.com/mysteriumnetwork/node/identity"
)

// stripeCount is the fixed number of port-striped mutexes. Using a fixed
// array avoids unbounded growth from arbitrary port values. 64 stripes
// give low contention even under high parallelism.
const stripeCount = 64

type multiConnectionManager struct {
	// mu protects the cms map only. Never held during Manager network calls.
	mu  sync.Mutex
	cms map[int]Manager

	// bulkMu serializes individual operations (RLock) against bulk disconnect
	// (Lock). DisconnectAll acquires the write lock, which blocks until every
	// in-flight Connect/Disconnect releases its read lock. This prevents a
	// connected manager from escaping bulk cleanup.
	bulkMu sync.RWMutex

	// stripes serialize Connect and Disconnect for the same port. Fixed-size
	// array: the stripe index is port % stripeCount. Two different ports that
	// hash to the same stripe serialize against each other — acceptable for
	// correctness, minor throughput cost.
	stripes [stripeCount]sync.Mutex

	newConnectionManager func() Manager
}

// NewMultiConnectionManager create a wrapper around connection manager to support multiple connections.
func NewMultiConnectionManager(newConnectionManager func() Manager) *multiConnectionManager {
	return &multiConnectionManager{
		cms:                  make(map[int]Manager),
		newConnectionManager: newConnectionManager,
	}
}

func (mcm *multiConnectionManager) stripe(port int) *sync.Mutex {
	idx := port % stripeCount
	if idx < 0 {
		idx += stripeCount
	}
	return &mcm.stripes[idx]
}

// Connect creates new connection from given consumer to provider.
//
// Lifecycle:
//  1. Acquire bulk read lock (blocks during DisconnectAll).
//  2. Acquire port stripe (serializes with Disconnect on the same port).
//  3. Look up or create a Manager — do NOT publish to cms yet.
//  4. Call Manager.Connect (network call, may block).
//  5. On success: publish to cms. On failure of a new manager: discard.
func (mcm *multiConnectionManager) Connect(consumerID identity.Identity, hermesID common.Address, proposalLookup ProposalLookup, params ConnectParams) error {
	mcm.bulkMu.RLock()
	defer mcm.bulkMu.RUnlock()

	st := mcm.stripe(params.ProxyPort)
	st.Lock()
	defer st.Unlock()

	mcm.mu.Lock()
	m, existed := mcm.cms[params.ProxyPort]
	if !existed {
		m = mcm.newConnectionManager()
	}
	mcm.mu.Unlock()

	err := m.Connect(consumerID, hermesID, proposalLookup, params)
	if err == nil && !existed {
		mcm.mu.Lock()
		mcm.cms[params.ProxyPort] = m
		mcm.mu.Unlock()
	}
	return err
}

// Status queries current status of connection.
func (mcm *multiConnectionManager) Status(id int) connectionstate.Status {
	mcm.mu.Lock()
	m, ok := mcm.cms[id]
	mcm.mu.Unlock()

	if ok {
		return m.Status()
	}
	return connectionstate.Status{State: connectionstate.NotConnected}
}

// Stats provides connection statistics information.
func (mcm *multiConnectionManager) Stats(id int) connectionstate.Statistics {
	mcm.mu.Lock()
	m, ok := mcm.cms[id]
	mcm.mu.Unlock()

	if ok {
		return m.Stats()
	}
	return connectionstate.Statistics{}
}

// Disconnect closes established connection and removes it from the registry.
// id < 0 disconnects all managers atomically.
//
// Lifecycle:
//  1. Acquire bulk read lock.
//  2. Acquire port stripe (waits for in-flight Connect on same port).
//  3. Remove the exact manager from cms.
//  4. Call Manager.Disconnect (network call).
func (mcm *multiConnectionManager) Disconnect(id int) error {
	if id < 0 {
		return mcm.disconnectAll()
	}

	mcm.bulkMu.RLock()
	defer mcm.bulkMu.RUnlock()

	// Always acquire stripe — serializes with in-flight Connect on this port.
	// Without this, Disconnect could see an empty cms entry, return early,
	// while Connect is about to publish and become orphaned.
	st := mcm.stripe(id)
	st.Lock()
	defer st.Unlock()

	mcm.mu.Lock()
	m, ok := mcm.cms[id]
	mcm.mu.Unlock()

	if !ok {
		return nil
	}

	err := m.Disconnect()

	// Remove after cleanup — not before
	mcm.mu.Lock()
	if mcm.cms[id] == m {
		delete(mcm.cms, id)
	}
	mcm.mu.Unlock()

	if errors.Is(err, ErrNoConnection) {
		return nil
	}
	return err
}

// disconnectAll acquires the bulk write lock, which blocks until all
// in-flight Connect/Disconnect calls release their read locks. Then it
// atomically detaches the entire registry and disconnects every manager.
// Managers published by Connect calls that start after the write lock
// is held will go into the fresh empty registry.
func (mcm *multiConnectionManager) disconnectAll() error {
	mcm.bulkMu.Lock()
	defer mcm.bulkMu.Unlock()

	mcm.mu.Lock()
	old := mcm.cms
	mcm.cms = make(map[int]Manager)
	mcm.mu.Unlock()

	var errs []error
	for _, m := range old {
		if err := m.Disconnect(); err != nil && !errors.Is(err, ErrNoConnection) {
			log.Error().Err(err).Msg("Failed to disconnect active connection")
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// CheckChannel checks if current session channel is alive, returns error on failed keep-alive ping.
func (mcm *multiConnectionManager) CheckChannel(context.Context) error { return nil }

// Reconnect reconnects current session. Holds bulkMu to prevent
// disconnectAll from racing the reconnect cycle.
func (mcm *multiConnectionManager) Reconnect(id int) {
	mcm.bulkMu.RLock()
	defer mcm.bulkMu.RUnlock()

	mcm.mu.Lock()
	m, ok := mcm.cms[id]
	mcm.mu.Unlock()

	if ok {
		m.Reconnect()
	}
}
