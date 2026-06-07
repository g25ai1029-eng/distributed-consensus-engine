import asyncio
import json
import logging
import os
import sys
import time
from enum import Enum
from crypto_utils import sign_message, verify_message, load_keys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [NODE-%(name)s] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

class Role(Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"

class Mode(Enum):
    PAXOS = "paxos"
    PBFT  = "pbft"

class PaxosState(Enum):
    IDLE     = "idle"
    PREPARED = "prepared"
    ACCEPTED = "accepted"

HEARTBEAT_INTERVAL  = 1.0
ELECTION_TIMEOUT    = 3.0
PAXOS_QUORUM        = 3
PBFT_QUORUM         = 3

class Node:
    def __init__(self, node_id, host, port, peers, mode):
        self.node_id   = node_id
        self.host      = host
        self.port      = port
        self.peers     = peers
        self.mode      = mode
        self.log       = logging.getLogger(str(node_id))

        self.role           = Role.FOLLOWER
        self.current_term   = 0
        self.voted_for      = None
        self.leader_id      = None
        self.last_heartbeat = time.time()

        self.ledger = []

        self.paxos_state  = PaxosState.IDLE
        self.proposal_num = 0
        self.promised_num = 0
        self.accepted_val = None
        self.promise_votes = {}
        self.accept_votes  = {}

        self.pbft_view      = 0
        self.pbft_sequence  = 0
        self.pbft_log       = {}
        self.pbft_committed = set()

        self.private_key, self.public_keys = load_keys(node_id)
        self.log.info(f"Node {node_id} initialised | mode={mode.value} | port={port}")

    async def start_server(self):
        server = await asyncio.start_server(self.handle_connection, self.host, self.port)
        self.log.info(f"Listening on {self.host}:{self.port}")
        async with server:
            await server.serve_forever()

    async def handle_connection(self, reader, writer):
        try:
            raw = await reader.read(65536)
            if not raw:
                return
            msg = json.loads(raw.decode())
            await self.route_message(msg)
        except Exception as e:
            self.log.warning(f"Connection error: {e}")
        finally:
            writer.close()

    async def route_message(self, msg):
        t = msg.get("type")
        if   t == "heartbeat":    await self.on_heartbeat(msg)
        elif t == "vote_request": await self.on_vote_request(msg)
        elif t == "vote_reply":   await self.on_vote_reply(msg)
        elif t == "prepare":      await self.on_prepare(msg)
        elif t == "promise":      await self.on_promise(msg)
        elif t == "accept":       await self.on_accept(msg)
        elif t == "accepted":     await self.on_accepted(msg)
        elif t == "commit":       await self.on_paxos_commit(msg)
        elif t == "pre_prepare":  await self.on_pre_prepare(msg)
        elif t == "prepare_pbft": await self.on_prepare_pbft(msg)
        elif t == "commit_pbft":  await self.on_commit_pbft(msg)
        elif t == "transaction":  await self.on_client_transaction(msg)
        else: self.log.warning(f"Unknown message type: {t}")

    async def send(self, peer, msg):
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(peer["host"], peer["port"]), timeout=1.0
            )
            writer.write(json.dumps(msg).encode())
            await writer.drain()
            writer.close()
        except Exception:
            pass

    async def broadcast(self, msg):
        await asyncio.gather(*[self.send(p, msg) for p in self.peers])

    async def heartbeat_loop(self):
        while True:
            if self.role == Role.LEADER:
                await self.broadcast({
                    "type"  : "heartbeat",
                    "term"  : self.current_term,
                    "leader": self.node_id,
                })
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def election_timeout_loop(self):
        while True:
            await asyncio.sleep(0.5)
            if self.role == Role.FOLLOWER:
                silence = time.time() - self.last_heartbeat
                if silence > ELECTION_TIMEOUT:
                    self.log.warning("Leader timeout — starting election")
                    await self.start_election()

    async def on_heartbeat(self, msg):
        sender_term = msg["term"]
        if sender_term >= self.current_term:
            self.current_term   = sender_term
            self.leader_id      = msg["leader"]
            self.role           = Role.FOLLOWER
            self.last_heartbeat = time.time()

    async def start_election(self):
        self.role         = Role.CANDIDATE
        self.current_term += 1
        self.voted_for    = self.node_id
        self.vote_count   = 1
        self.log.info(f"Election started | term={self.current_term}")
        await self.broadcast({
            "type"     : "vote_request",
            "term"     : self.current_term,
            "candidate": self.node_id,
        })
        await asyncio.sleep(2.0)
        if self.role == Role.CANDIDATE:
            self.log.warning("Election failed — returning to follower")
            self.role           = Role.FOLLOWER
            self.last_heartbeat = time.time()

    async def on_vote_request(self, msg):
        term      = msg["term"]
        candidate = msg["candidate"]
        grant     = False
        if term > self.current_term:
            self.current_term = term
            self.voted_for    = None
            self.role         = Role.FOLLOWER
        if term >= self.current_term and (self.voted_for is None or self.voted_for == candidate):
            self.voted_for = candidate
            grant          = True
            self.log.info(f"Voted for node {candidate} in term {term}")
        for peer in self.peers:
            if peer["id"] == candidate:
                await self.send(peer, {
                    "type"   : "vote_reply",
                    "term"   : self.current_term,
                    "granted": grant,
                    "voter"  : self.node_id,
                })
                break

    async def on_vote_reply(self, msg):
        if self.role != Role.CANDIDATE:
            return
        if msg["term"] != self.current_term:
            return
        if not msg["granted"]:
            return
        self.vote_count += 1
        self.log.info(f"Got vote from node {msg['voter']} | total={self.vote_count}")
        majority = (len(self.peers) + 1) // 2 + 1
        if self.vote_count >= majority:
            await self.become_leader()

    async def become_leader(self):
        self.role      = Role.LEADER
        self.leader_id = self.node_id
        self.log.info(f"*** BECAME LEADER *** term={self.current_term}")
        await self.broadcast({
            "type"  : "heartbeat",
            "term"  : self.current_term,
            "leader": self.node_id,
        })

    async def on_client_transaction(self, msg):
        if self.role != Role.LEADER:
            return
        transaction = msg["data"]
        self.log.info(f"Received transaction: {transaction}")
        if self.mode == Mode.PAXOS:
            await self.paxos_propose(transaction)
        else:
            await self.pbft_request(transaction)

    async def paxos_propose(self, value):
        self.proposal_num += 1
        n = self.proposal_num
        self.promise_votes[n] = []
        self.accept_votes[n]  = []
        self.accepted_val     = value
        self.log.info(f"[PAXOS] Sending Prepare(n={n})")
        await self.broadcast({
            "type"    : "prepare",
            "n"       : n,
            "proposer": self.node_id,
        })

    async def on_prepare(self, msg):
        n = msg["n"]
        if n > self.promised_num:
            self.promised_num = n
            self.log.info(f"[PAXOS] Promising n={n}")
            for peer in self.peers:
                if peer["id"] == msg["proposer"]:
                    await self.send(peer, {
                        "type"        : "promise",
                        "n"           : n,
                        "accepted_val": self.accepted_val,
                        "voter"       : self.node_id,
                    })
                    break

    async def on_promise(self, msg):
        n = msg["n"]
        if n not in self.promise_votes:
            return
        self.promise_votes[n].append(msg)
        if len(self.promise_votes[n]) >= PAXOS_QUORUM:
            existing = [m for m in self.promise_votes[n] if m["accepted_val"] is not None]
            value    = max(existing, key=lambda m: m["n"])["accepted_val"] if existing else self.accepted_val
            self.log.info(f"[PAXOS] Got majority promises — sending Accept(n={n}, v={value})")
            await self.broadcast({
                "type"    : "accept",
                "n"       : n,
                "value"   : value,
                "proposer": self.node_id,
            })

    async def on_accept(self, msg):
        n     = msg["n"]
        value = msg["value"]
        if n >= self.promised_num:
            self.promised_num = n
            self.accepted_val = value
            self.log.info(f"[PAXOS] Accepted value={value} at n={n}")
            for peer in self.peers:
                if peer["id"] == msg["proposer"]:
                    await self.send(peer, {
                        "type" : "accepted",
                        "n"    : n,
                        "value": value,
                        "voter": self.node_id,
                    })
                    break

    async def on_accepted(self, msg):
        n = msg["n"]
        if n not in self.accept_votes:
            return
        self.accept_votes[n].append(msg)
        if len(self.accept_votes[n]) >= PAXOS_QUORUM:
            value = msg["value"]
            self.log.info(f"[PAXOS] *** CONSENSUS REACHED *** value={value}")
            self._write_to_ledger(value)
            await self.broadcast({"type": "commit", "value": value, "n": n})

    async def on_paxos_commit(self, msg):
        self.log.info(f"[PAXOS] Committing value={msg['value']}")
        self._write_to_ledger(msg["value"])

    async def pbft_request(self, value):
        self.pbft_sequence += 1
        seq = self.pbft_sequence
        msg = {
            "type"  : "pre_prepare",
            "view"  : self.pbft_view,
            "seq"   : seq,
            "value" : value,
            "leader": self.node_id,
        }
        msg["signature"] = sign_message(self.private_key, msg)
        self.log.info(f"[PBFT] Pre-Prepare seq={seq} value={value}")
        await self.broadcast(msg)

    async def on_pre_prepare(self, msg):
        if not verify_message(self.public_keys.get(msg["leader"]), msg):
            self.log.warning("[PBFT] INVALID SIGNATURE on pre_prepare — dropping")
            return
        seq   = msg["seq"]
        value = msg["value"]
        view  = msg["view"]
        if view != self.pbft_view:
            return
        if seq not in self.pbft_log:
            self.pbft_log[seq] = {"prepare": [], "commit": [], "value": value}
        prepare_msg = {
            "type"  : "prepare_pbft",
            "view"  : view,
            "seq"   : seq,
            "value" : value,
            "sender": self.node_id,
        }
        prepare_msg["signature"] = sign_message(self.private_key, prepare_msg)
        await self.broadcast(prepare_msg)

    async def on_prepare_pbft(self, msg):
        sender = msg["sender"]
        if not verify_message(self.public_keys.get(sender), msg):
            self.log.warning(f"[PBFT] INVALID SIGNATURE from node {sender} — dropping")
            return
        seq   = msg["seq"]
        value = msg["value"]
        if seq not in self.pbft_log:
            self.pbft_log[seq] = {"prepare": [], "commit": [], "value": value}
        self.pbft_log[seq]["prepare"].append(msg)
        matching = [m for m in self.pbft_log[seq]["prepare"] if m["value"] == value]
        self.log.info(f"[PBFT] seq={seq} prepare votes={len(matching)}/{PBFT_QUORUM}")
        if len(matching) >= PBFT_QUORUM and seq not in self.pbft_committed:
            commit_msg = {
                "type"  : "commit_pbft",
                "view"  : self.pbft_view,
                "seq"   : seq,
                "value" : value,
                "sender": self.node_id,
            }
            commit_msg["signature"] = sign_message(self.private_key, commit_msg)
            await self.broadcast(commit_msg)

    async def on_commit_pbft(self, msg):
        sender = msg["sender"]
        if not verify_message(self.public_keys.get(sender), msg):
            self.log.warning(f"[PBFT] INVALID SIGNATURE on commit from {sender} — dropping")
            return
        seq   = msg["seq"]
        value = msg["value"]
        if seq not in self.pbft_log:
            self.pbft_log[seq] = {"prepare": [], "commit": [], "value": value}
        self.pbft_log[seq]["commit"].append(msg)
        matching = [m for m in self.pbft_log[seq]["commit"] if m["value"] == value]
        self.log.info(f"[PBFT] seq={seq} commit votes={len(matching)}/{PBFT_QUORUM}")
        if len(matching) >= PBFT_QUORUM and seq not in self.pbft_committed:
            self.pbft_committed.add(seq)
            self.log.info(f"[PBFT] *** COMMITTED *** seq={seq} value={value}")
            self._write_to_ledger(value)

    def _write_to_ledger(self, value):
        entry = {"index": len(self.ledger), "value": value, "timestamp": time.time()}
        self.ledger.append(entry)
        self.log.info(f"LEDGER [{entry['index']}] = {value}")
        ledger_path = f"/data/ledger_node_{self.node_id}.json"
        try:
            with open(ledger_path, "w") as f:
                json.dump(self.ledger, f, indent=2)
        except Exception as e:
            self.log.warning(f"Could not write ledger: {e}")

    async def run(self):
        self.log.info(f"Node {self.node_id} starting in {self.mode.value} mode")
        await asyncio.gather(
            self.start_server(),
            self.heartbeat_loop(),
            self.election_timeout_loop(),
        )

if __name__ == "__main__":
    NODE_ID = int(os.environ["NODE_ID"])
    HOST    = os.environ.get("HOST", "0.0.0.0")
    PORT    = int(os.environ.get("PORT", 5000))
    MODE    = Mode(os.environ.get("MODE", "paxos").lower())
    PEERS   = json.loads(os.environ.get("PEERS", "[]"))
    node    = Node(NODE_ID, HOST, PORT, PEERS, MODE)
    asyncio.run(node.run())
