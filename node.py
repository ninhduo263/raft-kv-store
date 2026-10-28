"""A small, standard-library-only Raft key-value store for classroom demos."""

from __future__ import annotations

import json
import os
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


HEARTBEAT_SECONDS = 0.4
ELECTION_TIMEOUT_SECONDS = (1.5, 3.0)
RPC_TIMEOUT_SECONDS = 0.8
CLIENT_WRITE_TIMEOUT_SECONDS = 5.0
MAX_REQUEST_BYTES = 1024 * 1024


class Blocked(Exception):
    """Raised when an RPC is rejected by the local partition simulator."""


class Node:
    def __init__(
        self,
        node_id: str,
        peers: dict[str, str],
        data_dir: str | Path,
        *,
        heartbeat_seconds: float = HEARTBEAT_SECONDS,
        election_timeout: tuple[float, float] = ELECTION_TIMEOUT_SECONDS,
        rpc_timeout: float = RPC_TIMEOUT_SECONDS,
        write_timeout: float = CLIENT_WRITE_TIMEOUT_SECONDS,
    ) -> None:
        if node_id not in peers:
            raise ValueError(f"NODE_ID {node_id!r} is missing from PEERS")
        if not peers or any(not name or not url.startswith(("http://", "https://")) for name, url in peers.items()):
            raise ValueError("PEERS must contain node names and HTTP(S) URLs")

        self.node_id = node_id
        self.all_peers = peers.copy()
        self.peers = {name: url for name, url in peers.items() if name != node_id}
        self.majority = len(peers) // 2 + 1
        self.heartbeat_seconds = heartbeat_seconds
        self.election_timeout = election_timeout
        self.rpc_timeout = rpc_timeout
        self.write_timeout = write_timeout

        self.lock = threading.RLock()
        self.cv = threading.Condition(self.lock)
        self.term = 0
        self.voted_for: str | None = None
        self.log: list[dict[str, Any]] = []
        self.role = "follower"
        self.leader: str | None = None
        self.commit = 0
        self.applied = 0
        self.kv: dict[str, str] = {}
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self.votes_received: set[str] = set()
        self.blocked: set[str] = set()
        self._replicating: set[str] = set()
        self._stop = threading.Event()

        self.data_path = Path(data_dir) / f"{node_id}.json"
        self.data_path.parent.mkdir(parents=True, exist_ok=True)
        self._load()
        self.deadline = self._new_deadline()
        self.next_heartbeat = 0.0

    def _log(self, message: str) -> None:
        print(f"{time.strftime('%H:%M:%S')} [{self.node_id}] {message}", flush=True)

    def _new_deadline(self) -> float:
        return time.time() + random.uniform(*self.election_timeout)

    def _load(self) -> None:
        if not self.data_path.exists():
            return
        with self.data_path.open(encoding="utf-8") as state_file:
            state = json.load(state_file)
        if not isinstance(state, dict) or not isinstance(state.get("log"), list):
            raise ValueError(f"invalid persistent state in {self.data_path}")
        term = state.get("term")
        log_entries = state["log"]
        if not isinstance(term, int) or term < 0:
            raise ValueError(f"invalid term in {self.data_path}")
        if any(not isinstance(entry, dict) or "term" not in entry or "cmd" not in entry for entry in log_entries):
            raise ValueError(f"invalid log entry in {self.data_path}")
        self.term = term
        self.voted_for = state.get("voted_for")
        self.log = log_entries
        self._log(f"restored: term={self.term}, log={len(self.log)} entries")

    def _persist(self) -> None:
        temporary_path = self.data_path.with_suffix(".json.tmp")
        with temporary_path.open("w", encoding="utf-8") as state_file:
            json.dump(
                {"term": self.term, "voted_for": self.voted_for, "log": self.log},
                state_file,
                ensure_ascii=False,
            )
            state_file.flush()
            os.fsync(state_file.fileno())
        os.replace(temporary_path, self.data_path)

    def _last_log_term(self) -> int:
        return self.log[-1]["term"] if self.log else 0

    def _become_follower(self, term: int, leader: str | None = None) -> None:
        if term > self.term:
            self.term = term
            self.voted_for = None
            self._persist()
        if self.role != "follower":
            self._log(f"-> follower (term {self.term})")
        self.role = "follower"
        if leader is not None:
            self.leader = leader
        self.votes_received.clear()
        self.cv.notify_all()

    def run(self) -> None:
        while not self._stop.wait(0.05):
            with self.lock:
                now = time.time()
                if self.role == "leader":
                    if now >= self.next_heartbeat:
                        self.next_heartbeat = now + self.heartbeat_seconds
                        for peer in self.peers:
                            self._start_replication(peer)
                elif now >= self.deadline:
                    self._start_election()

    def stop(self) -> None:
        self._stop.set()
        with self.cv:
            self.cv.notify_all()

    def _start_election(self) -> None:
        self.term += 1
        self.role = "candidate"
        self.voted_for = self.node_id
        self.leader = None
        self.votes_received = {self.node_id}
        self._persist()
        self.deadline = self._new_deadline()
        election_term = self.term
        self._log(f"election timeout -> candidate, term {election_term}")
        if len(self.votes_received) >= self.majority:
            self._become_leader()
            return

        request = {
            "from": self.node_id,
            "term": election_term,
            "last_log_index": len(self.log),
            "last_log_term": self._last_log_term(),
        }
        for peer in self.peers:
            if peer not in self.blocked:
                threading.Thread(target=self._ask_vote, args=(peer, request), daemon=True).start()

    def _ask_vote(self, peer: str, request: dict[str, Any]) -> None:
        response = self._rpc(peer, "/raft/request_vote", request)
        if response is None:
            return
        with self.lock:
            if response["term"] > self.term:
                self._become_follower(response["term"])
            elif self.role == "candidate" and self.term == request["term"] and response["granted"]:
                self.votes_received.add(peer)
                if len(self.votes_received) >= self.majority:
                    self._become_leader()

    def _become_leader(self) -> None:
        self.role = "leader"
        self.leader = self.node_id
        self._log(f"*** became LEADER, term {self.term} ***")
        for peer in self.peers:
            self.next_index[peer] = len(self.log) + 1
            self.match_index[peer] = 0
        self.log.append({"term": self.term, "cmd": None})
        self._persist()
        self.match_index[self.node_id] = len(self.log)
        self.next_heartbeat = 0.0
        self._advance_commit()

    def on_request_vote(self, request: dict[str, Any]) -> dict[str, Any]:
        candidate = request.get("from")
        term = request.get("term")
        last_index = request.get("last_log_index")
        last_term = request.get("last_log_term")
        if (
            candidate not in self.peers
            or not isinstance(term, int)
            or not isinstance(last_index, int)
            or not isinstance(last_term, int)
        ):
            raise ValueError("invalid RequestVote request")

        with self.lock:
            if candidate in self.blocked:
                raise Blocked()
            if term > self.term:
                self._become_follower(term)
            up_to_date = (last_term, last_index) >= (self._last_log_term(), len(self.log))
            granted = term == self.term and self.voted_for in (None, candidate) and up_to_date
            if granted:
                self.voted_for = candidate
                self._persist()
                self.deadline = self._new_deadline()
                self._log(f"vote granted to {candidate} (term {self.term})")
            return {"term": self.term, "granted": granted}

    def _start_replication(self, peer: str) -> None:
        if peer in self.blocked or peer in self._replicating:
            return
        self._replicating.add(peer)
        threading.Thread(target=self._replicate, args=(peer,), daemon=True).start()

    def _replicate(self, peer: str) -> None:
        try:
            with self.lock:
                if self.role != "leader" or peer in self.blocked:
                    return
                next_index = self.next_index[peer]
                previous_index = next_index - 1
                request = {
                    "from": self.node_id,
                    "term": self.term,
                    "prev_index": previous_index,
                    "prev_term": self.log[previous_index - 1]["term"] if previous_index else 0,
                    "entries": self.log[next_index - 1 : next_index - 1 + 50],
                    "commit": self.commit,
                }

            response = self._rpc(peer, "/raft/append_entries", request)
            if response is None:
                return
            with self.lock:
                if response["term"] > self.term:
                    self._become_follower(response["term"])
                    return
                if self.role != "leader" or self.term != request["term"]:
                    return
                if response["success"]:
                    replicated_index = request["prev_index"] + len(request["entries"])
                    self.match_index[peer] = max(self.match_index[peer], replicated_index)
                    self.next_index[peer] = self.match_index[peer] + 1
                    self._advance_commit()
                else:
                    self.next_index[peer] = max(1, next_index - 1)
        finally:
            with self.lock:
                self._replicating.discard(peer)

    def _advance_commit(self) -> None:
        for index in range(len(self.log), self.commit, -1):
            if self.log[index - 1]["term"] != self.term:
                continue
            replicated = sum(
                self.match_index.get(peer, 0) >= index for peer in self.all_peers
            )
            if replicated >= self.majority:
                self.commit = index
                self._apply()
                break

    def _apply(self) -> None:
        while self.applied < self.commit:
            self.applied += 1
            command = self.log[self.applied - 1]["cmd"]
            if command is None:
                continue
            if command["op"] == "put":
                self.kv[command["key"]] = command["value"]
            elif command["op"] == "del":
                self.kv.pop(command["key"], None)
            else:
                raise ValueError(f"unsupported command: {command['op']}")
        self.cv.notify_all()

    def on_append_entries(self, request: dict[str, Any]) -> dict[str, Any]:
        leader = request.get("from")
        term = request.get("term")
        previous_index = request.get("prev_index")
        previous_term = request.get("prev_term")
        entries = request.get("entries")
        leader_commit = request.get("commit")
        if (
            leader not in self.peers
            or not isinstance(term, int)
            or not isinstance(previous_index, int)
            or not isinstance(previous_term, int)
            or not isinstance(entries, list)
            or not isinstance(leader_commit, int)
        ):
            raise ValueError("invalid AppendEntries request")
        for entry in entries:
            if (
                not isinstance(entry, dict)
                or not isinstance(entry.get("term"), int)
                or "cmd" not in entry
            ):
                raise ValueError("invalid AppendEntries log entry")

        with self.lock:
            if leader in self.blocked:
                raise Blocked()
            if term < self.term:
                return {"term": self.term, "success": False}
            self._become_follower(term, leader)
            self.deadline = self._new_deadline()

            if previous_index > len(self.log) or (
                previous_index > 0
                and self.log[previous_index - 1]["term"] != previous_term
            ):
                return {"term": self.term, "success": False}

            index = previous_index
            changed = False
            for entry in entries:
                index += 1
                if index <= len(self.log) and self.log[index - 1]["term"] != entry["term"]:
                    del self.log[index - 1 :]
                    changed = True
                if index > len(self.log):
                    self.log.append(entry)
                    changed = True
            if changed:
                self._persist()

            if leader_commit > self.commit:
                self.commit = min(leader_commit, index)
                self._apply()
            return {"term": self.term, "success": True}

    def client_write(self, command: dict[str, str]) -> tuple[int, dict[str, Any]]:
        with self.cv:
            if self.role != "leader":
                return 421, {"error": "not leader", "leader": self.leader}
            self.log.append({"term": self.term, "cmd": command})
            self._persist()
            index, write_term = len(self.log), self.term
            self.match_index[self.node_id] = index
            for peer in self.peers:
                self._start_replication(peer)

            deadline = time.time() + self.write_timeout
            while self.applied < index:
                if self.role != "leader" or self.term != write_term:
                    return 503, {"error": "mất quyền leader, hãy thử lại"}
                remaining = deadline - time.time()
                if remaining <= 0:
                    return 504, {"error": "không đủ đa số xác nhận (timeout)"}
                self.cv.wait(min(remaining, 0.2))
            return 200, {"ok": True, "index": index, "term": write_term}

    def status(self) -> dict[str, Any]:
        with self.lock:
            return {
                "id": self.node_id,
                "role": self.role,
                "term": self.term,
                "leader": self.leader,
                "log_len": len(self.log),
                "commit": self.commit,
                "kv": self.kv.copy(),
                "blocked": sorted(self.blocked),
            }

    def _rpc(self, peer: str, path: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        request = urllib.request.Request(
            self.peers[peer] + path,
            json.dumps(payload).encode(),
            {"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.rpc_timeout) as response:
                result = json.loads(response.read())
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
            return None
        if not isinstance(result, dict):
            return None
        return result


class RaftHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], node: Node) -> None:
        self.node = node
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    server: RaftHTTPServer

    def log_message(self, _format: str, *_args: Any) -> None:
        pass

    def _reply(self, status: int, value: dict[str, Any]) -> None:
        body = json.dumps(value, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> bytes:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ValueError("invalid Content-Length") from exc
        if length < 0 or length > MAX_REQUEST_BYTES:
            raise ValueError(f"request body must be between 0 and {MAX_REQUEST_BYTES} bytes")
        return self.rfile.read(length) if length else b""

    def _forward(self, method: str, leader: str, body: bytes) -> None:
        request = urllib.request.Request(
            self.server.node.all_peers[leader] + self.path,
            data=body if body else None,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=7) as response:
                result = json.loads(response.read())
                self._reply(response.status, result)
        except urllib.error.HTTPError as error:
            try:
                result = json.loads(error.read())
            except json.JSONDecodeError:
                result = {"error": f"leader returned HTTP {error.code}"}
            self._reply(error.code, result)
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
            self._reply(503, {"error": f"không liên lạc được leader {leader}"})

    def _route(self, method: str) -> None:
        node = self.server.node
        parsed_url = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parsed_url.query)
        try:
            body = self._read_body()
            if parsed_url.path == "/status" and method == "GET":
                self._reply(200, node.status())
                return
            if parsed_url.path == "/raft/request_vote" and method == "POST":
                self._reply(200, node.on_request_vote(json.loads(body)))
                return
            if parsed_url.path == "/raft/append_entries" and method == "POST":
                self._reply(200, node.on_append_entries(json.loads(body)))
                return
            if parsed_url.path == "/admin/block" and method == "POST":
                request = json.loads(body)
                blocked = request.get("peers") if isinstance(request, dict) else None
                if not isinstance(blocked, list) or any(peer not in node.all_peers for peer in blocked):
                    raise ValueError("peers must be a list of known node IDs")
                with node.lock:
                    node.blocked = set(blocked)
                node._log(f"blocked peers: {sorted(node.blocked) or 'none'}")
                self._reply(200, {"blocked": sorted(node.blocked)})
                return
            if parsed_url.path.startswith("/kv/"):
                key = urllib.parse.unquote(parsed_url.path[len("/kv/") :])
                if not key:
                    raise ValueError("key must not be empty")
                if method == "GET" and query.get("local", [""])[0] in ("1", "true"):
                    with node.lock:
                        self._reply(200, {"key": key, "value": node.kv.get(key), "from": node.node_id})
                    return
                with node.lock:
                    role, leader = node.role, node.leader
                if role != "leader":
                    if leader is None:
                        self._reply(503, {"error": "chưa có leader"})
                    elif leader not in node.all_peers:
                        self._reply(503, {"error": "leader không nằm trong cấu hình cụm"})
                    else:
                        self._forward(method, leader, body)
                    return
                if method == "GET":
                    with node.lock:
                        self._reply(200, {"key": key, "value": node.kv.get(key), "from": node.node_id})
                elif method == "PUT":
                    self._reply(*node.client_write({"op": "put", "key": key, "value": body.decode("utf-8")}))
                elif method == "DELETE":
                    self._reply(*node.client_write({"op": "del", "key": key}))
                else:
                    self._reply(405, {"error": "method not allowed"})
                return
            self._reply(404, {"error": "not found"})
        except Blocked:
            self._reply(503, {"error": "blocked"})
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError, TypeError, KeyError) as error:
            self._reply(400, {"error": str(error)})

    def do_GET(self) -> None:
        self._route("GET")

    def do_PUT(self) -> None:
        self._route("PUT")

    def do_POST(self) -> None:
        self._route("POST")

    def do_DELETE(self) -> None:
        self._route("DELETE")


def parse_peers(value: str) -> dict[str, str]:
    peers: dict[str, str] = {}
    for item in value.split(","):
        node_id, separator, url = item.partition("=")
        if not separator or not node_id.strip() or not url.strip():
            raise ValueError("PEERS must be comma-separated node_id=http://host:port values")
        node_id, url = node_id.strip(), url.strip()
        if node_id in peers:
            raise ValueError(f"duplicate node ID in PEERS: {node_id}")
        peers[node_id] = url
    return peers


def main() -> None:
    node_id = os.environ.get("NODE_ID", "").strip()
    if not node_id:
        raise ValueError("NODE_ID environment variable is required")
    peers = parse_peers(os.environ.get("PEERS", ""))
    port = int(os.environ.get("PORT", "8000"))
    data_dir = os.environ.get("DATA_DIR", "/data")
    node = Node(node_id, peers, data_dir)
    threading.Thread(target=node.run, daemon=True).start()
    node._log(f"started on port {port}, cluster size {len(peers)}, majority = {node.majority}")
    RaftHTTPServer(("0.0.0.0", port), node).serve_forever()


if __name__ == "__main__":
    main()
