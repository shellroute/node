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
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/mysteriumnetwork/node/identity"
)

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
