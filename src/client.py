import asyncio
import json
import logging
import os
import random
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CLIENT] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("client")

NODES = json.loads(os.environ.get("NODES", json.dumps([
    {"id": 1, "host": "node1", "port": 5000},
    {"id": 2, "host": "node2", "port": 5000},
    {"id": 3, "host": "node3", "port": 5000},
    {"id": 4, "host": "node4", "port": 5000},
    {"id": 5, "host": "node5", "port": 5000},
])))

TRANSACTION_INTERVAL = float(os.environ.get("TX_INTERVAL", "2.0"))
MAX_TRANSACTIONS     = int(os.environ.get("MAX_TX", "0"))
ACCOUNTS             = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
ACTIONS              = ["transfer", "deposit", "withdraw"]

def generate_transaction(tx_id):
    sender   = random.choice(ACCOUNTS)
    receiver = random.choice([a for a in ACCOUNTS if a != sender])
    amount   = random.randint(10, 500)
    return {
        "tx_id"    : tx_id,
        "action"   : random.choice(ACTIONS),
        "from"     : sender,
        "to"       : receiver,
        "amount"   : amount,
        "timestamp": time.time(),
    }

async def send_to_node(node, msg, timeout=2.0):
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(node["host"], node["port"]), timeout=timeout
        )
        writer.write(json.dumps(msg).encode())
        await writer.drain()
        writer.close()
        return True
    except Exception:
        return False

async def send_transaction(tx):
    msg   = {"type": "transaction", "data": tx}
    nodes = list(NODES)
    random.shuffle(nodes)
    for node in nodes:
        success = await send_to_node(node, msg)
        if success:
            log.info(f"TX-{tx['tx_id']} sent to node {node['id']} ({tx['action']} {tx['amount']} {tx['from']}→{tx['to']})")
            return True
    log.error(f"TX-{tx['tx_id']} FAILED — no nodes reachable!")
    return False

class Stats:
    def __init__(self):
        self.sent   = 0
        self.failed = 0
        self.start  = time.time()

    def record_sent(self):   self.sent   += 1
    def record_failed(self): self.failed += 1

    def report(self):
        elapsed = time.time() - self.start
        rate    = self.sent / elapsed if elapsed > 0 else 0
        log.info(f"STATS sent={self.sent} failed={self.failed} elapsed={elapsed:.1f}s rate={rate:.2f} tx/s")

async def main():
    log.info("Client starting")
    await asyncio.sleep(5)
    stats = Stats()
    tx_id = 1
    while True:
        tx      = generate_transaction(tx_id)
        success = await send_transaction(tx)
        if success: stats.record_sent()
        else:       stats.record_failed()
        if tx_id % 10 == 0:
            stats.report()
        tx_id += 1
        if MAX_TRANSACTIONS > 0 and tx_id > MAX_TRANSACTIONS:
            stats.report()
            break
        await asyncio.sleep(TRANSACTION_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
