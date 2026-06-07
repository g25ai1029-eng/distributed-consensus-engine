import asyncio
import json
import logging
import os
import random
import time
import sys

sys.path.insert(0, os.path.dirname(__file__))

from node import Node, Role, Mode
from crypto_utils import sign_message, load_keys

log = logging.getLogger("ADVERSARY")

class AdversaryNode(Node):
    def __init__(self, node_id, host, port, peers, mode):
        super().__init__(node_id, host, port, peers, mode)
        self.drop_probability       = 0.4
        self.equivocate_probability = 0.6
        self.replay_probability     = 0.3
        self.seen_messages          = []
        self.fake_value             = "FORGED_TX_999"
        self.log.warning("=== ADVERSARY NODE ACTIVE ===")

    async def broadcast_equivocate(self, honest_msg, fake_msg):
        peers_list = list(self.peers)
        half       = len(peers_list) // 2
        for i, peer in enumerate(peers_list):
            if i < half:
                await self.send(peer, honest_msg)
            else:
                await self.send(peer, fake_msg)

    def should_drop(self, msg_type):
        if random.random() < self.drop_probability:
            self.log.warning(f"[ATTACK] Dropping message type={msg_type}")
            return True
        return False

    async def replay_old_message(self):
        if not self.seen_messages:
            return
        old_msg = random.choice(self.seen_messages)
        self.log.warning(f"[ATTACK] Replaying old message: {old_msg.get('type')}")
        await self.broadcast(old_msg)

    async def fake_heartbeat(self):
        self.log.warning("[ATTACK] Sending fake leader heartbeat")
        await self.broadcast({
            "type"  : "heartbeat",
            "term"  : self.current_term + 999,
            "leader": self.node_id,
        })

    async def on_prepare(self, msg):
        self.seen_messages.append(msg)
        if self.should_drop("prepare"):
            return
        if random.random() < self.equivocate_probability:
            self.log.warning("[ATTACK] Sending equivocated Promise")
            for peer in self.peers:
                if peer["id"] == msg["proposer"]:
                    await self.send(peer, {
                        "type"        : "promise",
                        "n"           : msg["n"],
                        "accepted_val": self.fake_value,
                        "voter"       : self.node_id,
                    })
                    return
        await super().on_prepare(msg)

    async def on_accept(self, msg):
        self.seen_messages.append(msg)
        if self.should_drop("accept"):
            return
        if random.random() < self.equivocate_probability:
            self.log.warning("[ATTACK] Sending fake Accepted")
            for peer in self.peers:
                if peer["id"] == msg["proposer"]:
                    await self.send(peer, {
                        "type" : "accepted",
                        "n"    : msg["n"],
                        "value": self.fake_value,
                        "voter": self.node_id,
                    })
                    return
        await super().on_accept(msg)

    async def on_paxos_commit(self, msg):
        if self.should_drop("commit"):
            return
        await super().on_paxos_commit(msg)

    async def on_pre_prepare(self, msg):
        self.seen_messages.append(msg)
        if self.should_drop("pre_prepare"):
            return
        seq   = msg["seq"]
        value = msg["value"]
        view  = msg["view"]
        honest_prepare = {
            "type"  : "prepare_pbft",
            "view"  : view,
            "seq"   : seq,
            "value" : value,
            "sender": self.node_id,
        }
        honest_prepare["signature"] = sign_message(self.private_key, honest_prepare)
        fake_prepare = {
            "type"  : "prepare_pbft",
            "view"  : view,
            "seq"   : seq,
            "value" : self.fake_value,
            "sender": self.node_id,
        }
        fake_prepare["signature"] = sign_message(self.private_key, fake_prepare)
        await self.broadcast_equivocate(honest_prepare, fake_prepare)

    async def on_prepare_pbft(self, msg):
        self.seen_messages.append(msg)
        if self.should_drop("prepare_pbft"):
            return
        if random.random() < self.equivocate_probability:
            seq = msg["seq"]
            fake_commit = {
                "type"  : "commit_pbft",
                "view"  : self.pbft_view,
                "seq"   : seq,
                "value" : self.fake_value,
                "sender": self.node_id,
            }
            fake_commit["signature"] = sign_message(self.private_key, fake_commit)
            await self.broadcast(fake_commit)
            return
        await super().on_prepare_pbft(msg)

    async def on_commit_pbft(self, msg):
        if self.should_drop("commit_pbft"):
            return
        await super().on_commit_pbft(msg)

    async def adversary_attack_loop(self):
        await asyncio.sleep(5)
        while True:
            await asyncio.sleep(random.uniform(3, 8))
            attack = random.choice(["replay", "fake_heartbeat", "nothing"])
            if attack == "replay" and self.seen_messages:
                await self.replay_old_message()
            elif attack == "fake_heartbeat":
                await self.fake_heartbeat()

    async def run(self):
        self.log.warning("=== ADVERSARY NODE RUNNING ===")
        await asyncio.gather(
            self.start_server(),
            self.heartbeat_loop(),
            self.election_timeout_loop(),
            self.adversary_attack_loop(),
        )

if __name__ == "__main__":
    NODE_ID = int(os.environ["NODE_ID"])
    HOST    = os.environ.get("HOST", "0.0.0.0")
    PORT    = int(os.environ.get("PORT", 5000))
    MODE    = Mode(os.environ.get("MODE", "pbft").lower())
    PEERS   = json.loads(os.environ.get("PEERS", "[]"))
    node    = AdversaryNode(NODE_ID, HOST, PORT, PEERS, MODE)
    asyncio.run(node.run())
