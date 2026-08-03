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

type multiConnectionManager struct {
	mu       sync.Mutex
	cms      map[int]Manager
	portLock map[int]*sync.Mutex

	// bulkMu serializes individual ops (RLock) vs bulk disconnect (Lock).
	// DisconnectAll takes a write lock, which blocks until all in-flight
	// Connect/Disconnect calls release their read locks. This prevents
	// a connected manager from escaping bulk cleanup.
	bulkMu sync.RWMutex

	newConnectionManager func() Manager
}

// NewMultiConnectionManager create a wrapper around connection manager to support multiple connections.
func NewMultiConnectionManager(newConnectionManager func() Manager) *multiConnectionManager {
	return &multiConnectionManager{
		cms:                  make(map[int]Manager),
		portLock:             make(map[int]*sync.Mutex),
		newConnectionManager: newConnectionManager,
	}
}

// portMu returns the per-port mutex, creating it if needed. Must be called under mu.
func (mcm *multiConnectionManager) portMu(port int) *sync.Mutex {
	mu, ok := mcm.portLock[port]
	if !ok {
		mu = &sync.Mutex{}
		mcm.portLock[port] = mu
	}
	return mu
}

// Connect creates new connection from given consumer to provider.
// The manager is published only after m.Connect succeeds.
// Per-port lock prevents two same-port connects from racing.
// Bulk barrier (bulkMu) ensures DisconnectAll waits for in-flight connects.
func (mcm *multiConnectionManager) Connect(consumerID identity.Identity, hermesID common.Address, proposalLookup ProposalLookup, params ConnectParams) error {
	mcm.bulkMu.RLock()
	defer mcm.bulkMu.RUnlock()

	mcm.mu.Lock()
	pmu := mcm.portMu(params.ProxyPort)
	mcm.mu.Unlock()

	pmu.Lock()
	defer pmu.Unlock()

	// Re-check under port lock — another Connect may have published
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

	return connectionstate.Status{
		State: connectionstate.NotConnected,
	}
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
func (mcm *multiConnectionManager) Disconnect(id int) error {
	if id < 0 {
		return mcm.disconnectAll()
	}

	mcm.bulkMu.RLock()
	defer mcm.bulkMu.RUnlock()

	mcm.mu.Lock()
	pmu, hasPmu := mcm.portLock[id]
	mcm.mu.Unlock()

	if hasPmu {
		pmu.Lock()
		defer pmu.Unlock()
	}

	mcm.mu.Lock()
	m, ok := mcm.cms[id]
	if ok {
		delete(mcm.cms, id)
		delete(mcm.portLock, id)
	}
	mcm.mu.Unlock()

	if !ok {
		return nil
	}

	err := m.Disconnect()
	if errors.Is(err, ErrNoConnection) {
		return nil
	}
	return err
}

// disconnectAll waits for all in-flight operations via bulkMu, then
// atomically detaches and disconnects everything. Managers created by
// concurrent Connect calls after the bulk lock is acquired are not affected
// because Connect blocks on bulkMu.RLock.
func (mcm *multiConnectionManager) disconnectAll() error {
	mcm.bulkMu.Lock()
	defer mcm.bulkMu.Unlock()

	mcm.mu.Lock()
	old := mcm.cms
	mcm.cms = make(map[int]Manager)
	mcm.portLock = make(map[int]*sync.Mutex)
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

// Reconnect reconnects current session.
func (mcm *multiConnectionManager) Reconnect(id int) {
	mcm.mu.Lock()
	m, ok := mcm.cms[id]
	mcm.mu.Unlock()

	if ok {
		m.Reconnect()
	}
}
