import json
import tempfile
import threading
import time
import urllib.error
import unittest
import urllib.request
from pathlib import Path

from node import Node, RaftHTTPServer


class RaftClusterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.nodes: list[Node] = []
        self.servers: list[RaftHTTPServer] = []
        self.server_threads: list[threading.Thread] = []
        self.node_threads: list[threading.Thread] = []

    def tearDown(self) -> None:
        for node in self.nodes:
            node.stop()
        for server in self.servers:
            server.shutdown()
        for thread in self.server_threads + self.node_threads:
            thread.join(timeout=2)
        for server in self.servers:
            server.server_close()
        self.temp_dir.cleanup()

    @staticmethod
    def _reserve_port() -> int:
        import socket

        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    def _start_cluster(self) -> dict[str, str]:
        ports = {node_id: self._reserve_port() for node_id in ("n1", "n2", "n3")}
        peers = {node_id: f"http://127.0.0.1:{port}" for node_id, port in ports.items()}
        for node_id, port in ports.items():
            node = Node(
                node_id,
                peers,
                Path(self.temp_dir.name) / node_id,
                heartbeat_seconds=0.08,
                election_timeout=(0.25, 0.45),
                rpc_timeout=0.2,
                write_timeout=2,
            )
            server = RaftHTTPServer(("127.0.0.1", port), node)
            self.nodes.append(node)
            self.servers.append(server)
            server_thread = threading.Thread(target=server.serve_forever, daemon=True)
            node_thread = threading.Thread(target=node.run, daemon=True)
            self.server_threads.append(server_thread)
            self.node_threads.append(node_thread)
            server_thread.start()
            node_thread.start()
        return peers

    def _request(
        self, url: str, method: str = "GET", data: bytes | None = None
    ) -> tuple[int, dict]:
        request = urllib.request.Request(url, data=data, method=method)
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as error:
            try:
                return error.code, json.loads(error.read())
            finally:
                error.close()

    def test_cluster_elects_leader_and_commits_write_from_follower(self) -> None:
        peers = self._start_cluster()
        deadline = time.time() + 5
        leader_id = None
        while time.time() < deadline:
            leaders = [node for node in self.nodes if node.status()["role"] == "leader"]
            if len(leaders) == 1:
                leader_id = leaders[0].node_id
                break
            time.sleep(0.03)
        self.assertIsNotNone(leader_id, "cluster did not elect exactly one leader")

        follower_id = next(node_id for node_id in peers if node_id != leader_id)
        follower = next(node for node in self.nodes if node.node_id == follower_id)
        deadline = time.time() + 3
        while time.time() < deadline and follower.status()["leader"] != leader_id:
            time.sleep(0.03)
        self.assertEqual(follower.status()["leader"], leader_id)
        status, result = self._request(
            f"{peers[follower_id]}/kv/name", "PUT", b"Hoang"
        )
        self.assertEqual(status, 200, result)
        self.assertTrue(result["ok"])

        deadline = time.time() + 3
        while time.time() < deadline and any(
            node.status()["kv"].get("name") != "Hoang" for node in self.nodes
        ):
            time.sleep(0.03)
        for node in self.nodes:
            self.assertEqual(node.status()["kv"].get("name"), "Hoang")

    def test_conflicting_suffix_is_truncated_and_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            peers = {
                "n1": "http://127.0.0.1:8001",
                "n2": "http://127.0.0.1:8002",
                "n3": "http://127.0.0.1:8003",
            }
            node = Node("n2", peers, data_dir)
            node.log = [
                {"term": 1, "cmd": {"op": "put", "key": "old", "value": "value"}},
                {"term": 2, "cmd": {"op": "put", "key": "stale", "value": "value"}},
            ]
            node._persist()

            response = node.on_append_entries(
                {
                    "from": "n1",
                    "term": 3,
                    "prev_index": 1,
                    "prev_term": 1,
                    "entries": [{"term": 3, "cmd": {"op": "put", "key": "new", "value": "ok"}}],
                    "commit": 0,
                }
            )

            self.assertTrue(response["success"])
            self.assertEqual([entry["term"] for entry in node.log], [1, 3])
            restored = Node("n2", peers, data_dir)
            self.assertEqual(restored.log, node.log)

    def test_vote_is_not_granted_to_stale_log_or_twice_in_one_term(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            peers = {
                "n1": "http://127.0.0.1:8001",
                "n2": "http://127.0.0.1:8002",
                "n3": "http://127.0.0.1:8003",
            }
            node = Node("n2", peers, data_dir)
            node.log.append({"term": 2, "cmd": None})

            stale = node.on_request_vote(
                {"from": "n1", "term": 3, "last_log_index": 0, "last_log_term": 0}
            )
            fresh = node.on_request_vote(
                {"from": "n1", "term": 3, "last_log_index": 1, "last_log_term": 2}
            )
            another = node.on_request_vote(
                {"from": "n3", "term": 3, "last_log_index": 1, "last_log_term": 2}
            )

            self.assertFalse(stale["granted"])
            self.assertTrue(fresh["granted"])
            self.assertFalse(another["granted"])


if __name__ == "__main__":
    unittest.main()
