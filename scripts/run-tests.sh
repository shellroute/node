#!/usr/bin/env bash
set -uo pipefail

cd "$(dirname "$0")/.."

failures=()
logdir=$(mktemp -d)
trap 'rm -rf "$logdir"' EXIT

run_stage() {
    local name="$1"
    shift
    local log="$logdir/${name// /_}.log"
    echo "=== $name ==="
    if "$@" 2>&1 | tee "$log"; then
        echo "--- $name: PASS ---"
    else
        echo "--- $name: FAIL ---"
        failures+=("$name|$log")
    fi
    echo ""
}

# Our modified packages
run_stage "proxyclient" go test -race ./services/wireguard/endpoint/proxyclient/ -v -timeout 60s -count=1
run_stage "balance tracker" go test -race ./session/pingpong/ -v -timeout 60s -count=1
run_stage "connection manager" go test -race ./core/connection/ -v -timeout 60s -count=1

# Build check — catches compile errors across the whole tree
run_stage "build wireguard" go build ./services/wireguard/...
run_stage "build connection" go build ./core/connection/...
run_stage "build pingpong" go build ./session/pingpong/...

echo "=============================="
if [ ${#failures[@]} -eq 0 ]; then
    echo "=== All tests passed ==="
else
    echo "=== ${#failures[@]} stage(s) FAILED ==="
    for entry in "${failures[@]}"; do
        name="${entry%%|*}"
        log="${entry##*|}"
        echo ""
        echo "  ✗ $name"
        grep -E '(--- FAIL:|FAIL\t|_test\.go:[0-9]+:)' "$log" 2>/dev/null | sed 's/^/    /'
    done
    exit 1
fi
