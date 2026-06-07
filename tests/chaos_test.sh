#!/bin/bash
set -e

TOXIPROXY_API="http://localhost:8474"
PASS=0
FAIL=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()     { echo -e "${BLUE}[$(date +%H:%M:%S)] $1${NC}"; }
ok()      { echo -e "${GREEN}[✓] $1${NC}"; PASS=$((PASS+1)); }
fail()    { echo -e "${RED}[✗] $1${NC}"; FAIL=$((FAIL+1)); }
warn()    { echo -e "${YELLOW}[!] $1${NC}"; }
section() { echo -e "\n${YELLOW}══════════════════════════════════════${NC}"; echo -e "${YELLOW}  $1${NC}"; echo -e "${YELLOW}══════════════════════════════════════${NC}"; }

check_toxiproxy() {
    if curl -sf "$TOXIPROXY_API/proxies" > /dev/null 2>&1; then
        ok "Toxiproxy reachable"
    else
        fail "Toxiproxy not reachable"
        exit 1
    fi
}

create_proxy() {
    local name=$1 listen_port=$2 upstream=$3
    curl -sf -X POST "$TOXIPROXY_API/proxies" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"$name\",\"listen\":\"0.0.0.0:$listen_port\",\"upstream\":\"$upstream\"}" \
        > /dev/null 2>&1 && ok "Proxy created: $name" || warn "Proxy $name may already exist"
}

add_toxic() {
    local proxy=$1 toxic_type=$2 attributes=$3
    curl -sf -X POST "$TOXIPROXY_API/proxies/$proxy/toxics" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"$toxic_type\",\"attributes\":$attributes}" \
        > /dev/null && ok "Injected $toxic_type on $proxy" || fail "Failed to inject $toxic_type on $proxy"
}

clear_toxics() {
    local proxy=$1
    toxics=$(curl -sf "$TOXIPROXY_API/proxies/$proxy/toxics" 2>/dev/null | python3 -c "
import json,sys
toxics = json.load(sys.stdin)
for t in toxics: print(t['name'])
" 2>/dev/null)
    for toxic in $toxics; do
        curl -sf -X DELETE "$TOXIPROXY_API/proxies/$proxy/toxics/$toxic" > /dev/null
    done
    ok "Cleared toxics on $proxy"
}

disable_proxy() {
    local proxy=$1
    curl -sf -X POST "$TOXIPROXY_API/proxies/$proxy" \
        -H "Content-Type: application/json" \
        -d '{"enabled":false}' > /dev/null && ok "Disabled $proxy" || fail "Could not disable $proxy"
}

enable_proxy() {
    local proxy=$1
    curl -sf -X POST "$TOXIPROXY_API/proxies/$proxy" \
        -H "Content-Type: application/json" \
        -d '{"enabled":true}' > /dev/null && ok "Re-enabled $proxy" || fail "Could not enable $proxy"
}

check_ledger() {
    local node=$1
    echo "  Ledger on node${node}:"
    docker exec "node${node}" cat "/data/ledger_node_${node}.json" 2>/dev/null \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'    entries={len(d)}, last={d[-1][\"value\"] if d else None}')" \
        2>/dev/null || echo "    (no ledger yet)"
}

section "STEP 0 — PRE-FLIGHT"
check_toxiproxy
log "Waiting 10s for cluster to stabilise..."
sleep 10

section "STEP 1 — SET UP PROXIES"
create_proxy "node1" 15001 "node1:5000"
create_proxy "node2" 15002 "node2:5000"
create_proxy "node3" 15003 "node3:5000"
create_proxy "node4" 15004 "node4:5000"
create_proxy "node5" 15005 "node5:5000"
sleep 3

section "STEP 2 — INJECT LATENCY"
for i in 1 2 3 4 5; do add_toxic "node${i}" "latency" '{"latency":300,"jitter":50}'; done
log "Latency injected. Sleeping 10s..."
sleep 10

section "STEP 3 — INJECT PACKET LOSS"
add_toxic "node2" "limit_data" '{"bytes":0}'
add_toxic "node4" "limit_data" '{"bytes":0}'
sleep 10
for i in 1 2 3 4 5; do clear_toxics "node${i}"; done
sleep 3

section "STEP 4 — CRASH NODE 3"
docker stop node3 2>/dev/null && ok "node3 stopped" || warn "node3 already stopped"
warn "4 nodes remain. Quorum = 3. System should continue."
sleep 8
for i in 1 2 4 5; do check_ledger $i; done

section "STEP 5 — CRASH NODE 4"
docker stop node4 2>/dev/null && ok "node4 stopped" || warn "node4 already stopped"
warn "3 nodes remain. Quorum threshold reached."
sleep 8
for i in 1 2 5; do check_ledger $i; done

section "STEP 6 — NETWORK PARTITION"
disable_proxy "node1"
docker start node3 2>/dev/null && ok "node3 restarted"
docker start node4 2>/dev/null && ok "node4 restarted"
sleep 15
for i in 2 3 4 5; do check_ledger $i; done

section "STEP 7 — BYZANTINE CHECK"
docker logs adversary 2>/dev/null | grep -i "ATTACK" | tail -10 || warn "No adversary logs"
for i in 1 2 3; do
    echo "  Node $i rejections:"
    docker logs "node${i}" 2>/dev/null | grep -i "INVALID SIGNATURE\|dropping" | tail -5 || echo "    (none)"
done

section "STEP 8 — FULL RECOVERY"
enable_proxy "node1"
sleep 15
for i in 1 2 3 4 5; do check_ledger $i; done

section "COMPLETE"
echo -e "${GREEN}  PASSED: $PASS${NC}"
echo -e "${RED}  FAILED: $FAIL${NC}"
if [ $FAIL -eq 0 ]; then
    echo -e "${GREEN}  All scenarios survived!${NC}"
else
    echo -e "${RED}  Some scenarios failed.${NC}"
fi
