# Hướng dẫn làm đề tài: KV Store dùng Raft chạy trên 3–5 container Docker

Đề tài minh họa mục **8.1 (Introduction to fault tolerance)** và **8.2 (Process resilience)** của chương 8, môn Hệ thống phân tán.

Project có thể chạy nằm trong thư mục [`raft-kv/`](./raft-kv/), dùng thư viện chuẩn của Python 3.12 (không cần `pip install`). Hướng dẫn chạy nhanh và kiểm thử có trong [`raft-kv/README.md`](./raft-kv/README.md). Cụm Docker 3 node và 5 node đã được build, chạy thử bầu leader, ghi/nhân bản dữ liệu và kiểm tra network partition; bộ test tự động cũng kiểm tra election, commit và khôi phục log.

---

## 1. Mục tiêu và ý tưởng

Xây một kho key-value phân tán gồm **n node** (n = 3 hoặc 5), mỗi node là một container. Các node tự bầu **leader**, dữ liệu được nhân bản bằng giao thức **Raft**. Khi tắt một vài node, hệ thống vẫn đọc/ghi được miễn là còn **đa số** node sống.

Đây chính là **process group có replicated-write** (mục 8.2.1–8.2.2) kết hợp **consensus với crash failure** (mục 8.2.3).

### Ánh xạ sang nội dung tài liệu

| Khái niệm trong sách | Thể hiện trong project |
|---|---|
| Process group, hierarchical group (8.2.1) | Cụm n node, 1 leader (coordinator) + các follower (worker) |
| Primary-backup, leader election (8.2.2) | Raft bầu leader bằng `RequestVote`; leader chết thì bầu lại |
| k-fault tolerant, cần 2k+1 node khi crash (8.2.2) | 3 node chịu k=1, 5 node chịu k=2 |
| Consensus, các node thực thi cùng lệnh theo cùng thứ tự (8.2.3) | Replicated log, lệnh chỉ commit khi đa số xác nhận |
| Raft, term, ⟨o, t, k⟩, đa số ack, commit index (8.2.3) | `term`, `log[{term, cmd}]`, `commit`, `AppendEntries` |
| Failure detection bằng timeout, false positive (8.2.8) | Follower hết *election timeout* thì nghi leader chết |
| Network partition, CAP (8.2.7) | Lệnh `/admin/block` cô lập node; phía thiểu số không ghi được (chọn C hơn A) |
| Redundancy, availability (8.1.3, 8.1.1) | Dữ liệu nhân bản n lần, còn đa số node sống thì hệ thống vẫn sẵn sàng |
| Crash failure, omission failure (8.1.2) | `docker stop` = crash; `/admin/block` = omission (mất tin nhắn) |

---

## 2. Kiến trúc

```
              Client (curl)
                  |
      +-----------+-----------+------...
      |           |           |
  [ n1 ]      [ n2 ]      [ n3 ]    (n4, n5 nếu chạy 5 node)
  follower    LEADER      follower
      ^           |  AppendEntries (heartbeat + log)
      +-----------+-----------> các follower
      <---------- RequestVote khi bầu cử ---------->
```

- Mỗi node chạy một HTTP server (cổng 8000 trong container), vừa nhận request của client vừa nhận RPC của Raft.
- Client gửi request tới **node bất kỳ**; nếu node đó là follower thì nó **chuyển tiếp** cho leader.
- Mỗi node lưu `term`, `voted_for`, `log` ra file JSON để khởi động lại không mất.

### Raft tóm tắt (đủ để thuyết minh)

1. **Bầu leader**: follower không nghe được heartbeat trong khoảng 1,5–3 giây (ngẫu nhiên) thì tăng `term`, tự ứng cử và xin phiếu. Được **đa số** phiếu thì thành leader. Node chỉ bỏ phiếu cho ứng viên có log **không cũ hơn** log của mình.
2. **Nhân bản log**: client ghi → leader thêm entry `{term, cmd}` vào log → gửi `AppendEntries` cho các follower → khi **đa số** (kể cả leader) đã lưu entry thì leader **commit**, áp dụng vào KV và trả lời client.
3. **Heartbeat**: leader gửi `AppendEntries` rỗng mỗi 0,4 giây để giữ quyền.
4. **Sửa log lệch**: nếu log follower khác leader, leader lùi `next_index` cho tới khi khớp rồi ghi đè phần sai của follower.

---

## 3. Cấu trúc thư mục

```
raft-kv/
├── node.py
├── Dockerfile
├── docker-compose.yml
└── .env
```

Tạo thư mục: `mkdir raft-kv && cd raft-kv`

---

## 4. Mã nguồn

### 4.1 `node.py`

```python
"""Mini Raft KV store - chỉ dùng thư viện chuẩn của Python."""
import json, os, random, threading, time, urllib.request, urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

NODE_ID = os.environ["NODE_ID"]
PORT = int(os.environ.get("PORT", "8000"))
DATA_DIR = os.environ.get("DATA_DIR", "/data")
ALL = dict(p.split("=", 1) for p in os.environ["PEERS"].split(","))
PEERS = {k: v for k, v in ALL.items() if k != NODE_ID}      # các node khác
MAJORITY = len(ALL) // 2 + 1
HEARTBEAT = 0.4                       # giây
ELECTION_RANGE = (1.5, 3.0)           # timeout bầu cử ngẫu nhiên


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} [{NODE_ID}] {msg}", flush=True)


def rpc(peer, path, payload, timeout=0.8):
    req = urllib.request.Request(PEERS[peer] + path, json.dumps(payload).encode(),
                                 {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


class Blocked(Exception):
    pass


class Node:
    def __init__(self):
        self.lock = threading.RLock()
        self.cv = threading.Condition(self.lock)
        self.term, self.voted_for, self.log = 0, None, []     # trạng thái bền vững
        self.role, self.leader = "follower", None
        self.commit, self.applied, self.kv = 0, 0, {}
        self.next_index, self.match_index = {}, {}
        self.blocked = set()
        os.makedirs(DATA_DIR, exist_ok=True)
        self.path = os.path.join(DATA_DIR, f"{NODE_ID}.json")
        if os.path.exists(self.path):
            s = json.load(open(self.path))
            self.term, self.voted_for, self.log = s["term"], s["voted_for"], s["log"]
            log(f"khôi phục: term={self.term}, log={len(self.log)} entries")
        self.deadline = self.new_deadline()
        self.next_hb = 0

    # ---------- tiện ích ----------
    def new_deadline(self):
        return time.time() + random.uniform(*ELECTION_RANGE)

    def persist(self):
        tmp = self.path + ".tmp"
        json.dump({"term": self.term, "voted_for": self.voted_for, "log": self.log}, open(tmp, "w"))
        os.replace(tmp, self.path)

    def last_term(self):
        return self.log[-1]["term"] if self.log else 0

    def become_follower(self, term, leader=None):
        if term > self.term:
            self.term, self.voted_for = term, None
            self.persist()
        if self.role != "follower":
            log(f"-> follower (term {self.term})")
        self.role = "follower"
        if leader:
            self.leader = leader

    # ---------- vòng lặp chính ----------
    def run(self):
        while True:
            time.sleep(0.05)
            with self.lock:
                now = time.time()
                if self.role == "leader":
                    if now >= self.next_hb:
                        self.next_hb = now + HEARTBEAT
                        for p in PEERS:
                            threading.Thread(target=self.replicate, args=(p,), daemon=True).start()
                elif now >= self.deadline:
                    self.start_election()

    # ---------- bầu cử ----------
    def start_election(self):
        self.term += 1
        self.role, self.voted_for, self.leader = "candidate", NODE_ID, None
        self.persist()
        self.deadline = self.new_deadline()
        term, votes = self.term, [1]
        log(f"hết timeout -> ứng cử viên, term {term}")
        req = {"from": NODE_ID, "term": term, "last_log_index": len(self.log),
               "last_log_term": self.last_term()}
        for p in PEERS:
            if p not in self.blocked:
                threading.Thread(target=self.ask_vote, args=(p, req, votes), daemon=True).start()

    def ask_vote(self, peer, req, votes):
        resp = rpc(peer, "/raft/request_vote", req)
        if not resp:
            return
        with self.lock:
            if resp["term"] > self.term:
                self.become_follower(resp["term"])
            elif self.role == "candidate" and self.term == req["term"] and resp["granted"]:
                votes[0] += 1
                if votes[0] >= MAJORITY:
                    self.become_leader()

    def become_leader(self):
        self.role, self.leader = "leader", NODE_ID
        log(f"*** trở thành LEADER, term {self.term} ***")
        for p in PEERS:
            self.next_index[p], self.match_index[p] = len(self.log) + 1, 0
        self.log.append({"term": self.term, "cmd": None})      # no-op để commit entry cũ
        self.persist()
        self.next_hb = 0

    def on_request_vote(self, req):
        with self.lock:
            if req["from"] in self.blocked:
                raise Blocked()
            if req["term"] > self.term:
                self.become_follower(req["term"])
            up_to_date = (req["last_log_term"], req["last_log_index"]) >= (self.last_term(), len(self.log))
            grant = req["term"] == self.term and self.voted_for in (None, req["from"]) and up_to_date
            if grant:
                self.voted_for = req["from"]
                self.persist()
                self.deadline = self.new_deadline()
                log(f"bỏ phiếu cho {req['from']} (term {self.term})")
            return {"term": self.term, "granted": grant}

    # ---------- nhân bản log ----------
    def replicate(self, peer):
        with self.lock:
            if self.role != "leader" or peer in self.blocked:
                return
            ni = self.next_index[peer]
            prev = ni - 1
            req = {"from": NODE_ID, "term": self.term, "prev_index": prev,
                   "prev_term": self.log[prev - 1]["term"] if prev > 0 else 0,
                   "entries": self.log[ni - 1: ni - 1 + 50], "commit": self.commit}
        resp = rpc(peer, "/raft/append_entries", req)
        if not resp:
            return
        with self.lock:
            if resp["term"] > self.term:
                self.become_follower(resp["term"])
                return
            if self.role != "leader" or self.term != req["term"]:
                return
            if resp["success"]:
                self.match_index[peer] = max(self.match_index[peer], prev + len(req["entries"]))
                self.next_index[peer] = self.match_index[peer] + 1
                self.advance_commit()
            else:
                self.next_index[peer] = max(1, ni - 1)       # lùi lại và thử lại

    def advance_commit(self):
        for n in range(len(self.log), self.commit, -1):
            if self.log[n - 1]["term"] != self.term:
                break
            if 1 + sum(1 for p in PEERS if self.match_index[p] >= n) >= MAJORITY:
                self.commit = n
                self.apply()
                break

    def apply(self):
        while self.applied < self.commit:
            self.applied += 1
            cmd = self.log[self.applied - 1]["cmd"]
            if cmd and cmd["op"] == "put":
                self.kv[cmd["key"]] = cmd["value"]
            elif cmd and cmd["op"] == "del":
                self.kv.pop(cmd["key"], None)
        self.cv.notify_all()

    def on_append_entries(self, req):
        with self.lock:
            if req["from"] in self.blocked:
                raise Blocked()
            if req["term"] < self.term:
                return {"term": self.term, "success": False}
            self.become_follower(req["term"], req["from"])
            self.deadline = self.new_deadline()
            pi = req["prev_index"]
            if pi > len(self.log) or (pi > 0 and self.log[pi - 1]["term"] != req["prev_term"]):
                return {"term": self.term, "success": False}
            idx, changed = pi, False
            for e in req["entries"]:
                idx += 1
                if idx <= len(self.log) and self.log[idx - 1]["term"] != e["term"]:
                    del self.log[idx - 1:]                  # xung đột -> xóa phần log sai
                if idx > len(self.log):
                    self.log.append(e)
                    changed = True
            if changed:
                self.persist()
            if req["commit"] > self.commit:
                self.commit = min(req["commit"], idx)
                self.apply()
            return {"term": self.term, "success": True}

    # ---------- ghi từ client ----------
    def client_write(self, cmd):
        with self.cv:
            if self.role != "leader":
                return 421, {"error": "not leader", "leader": self.leader}
            self.log.append({"term": self.term, "cmd": cmd})
            self.persist()
            idx, term = len(self.log), self.term
            end = time.time() + 5
            while self.applied < idx:
                if self.role != "leader" or self.term != term:
                    return 503, {"error": "mất quyền leader, hãy thử lại"}
                if time.time() >= end:
                    return 504, {"error": "không đủ đa số xác nhận (timeout)"}
                self.cv.wait(0.2)
            return 200, {"ok": True, "index": idx, "term": term}

    def status(self):
        with self.lock:
            return {"id": NODE_ID, "role": self.role, "term": self.term, "leader": self.leader,
                    "log_len": len(self.log), "commit": self.commit, "kv": self.kv,
                    "blocked": sorted(self.blocked)}


node = Node()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def reply(self, code, obj):
        b = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def forward(self, method, leader, data):
        req = urllib.request.Request(ALL[leader] + self.path, data or None, method=method)
        try:
            with urllib.request.urlopen(req, timeout=7) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())
        except Exception:
            return 503, {"error": f"không liên lạc được leader {leader}"}

    def route(self, method):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        n = int(self.headers.get("Content-Length", 0))
        data = self.rfile.read(n) if n else b""
        try:
            if u.path == "/status":
                return self.reply(200, node.status())
            if u.path == "/raft/request_vote" and method == "POST":
                return self.reply(200, node.on_request_vote(json.loads(data)))
            if u.path == "/raft/append_entries" and method == "POST":
                return self.reply(200, node.on_append_entries(json.loads(data)))
            if u.path == "/admin/block" and method == "POST":
                with node.lock:
                    node.blocked = set(json.loads(data)["peers"])
                log(f"CHẶN liên lạc với: {sorted(node.blocked) or 'không ai'}")
                return self.reply(200, {"blocked": sorted(node.blocked)})
            if u.path.startswith("/kv/"):
                key = u.path[4:]
                if method == "GET" and q.get("local"):          # đọc thẳng từ node này
                    with node.lock:
                        return self.reply(200, {"key": key, "value": node.kv.get(key), "from": NODE_ID})
                if node.role != "leader":                       # follower chuyển tiếp cho leader
                    if not node.leader:
                        return self.reply(503, {"error": "chưa có leader"})
                    return self.reply(*self.forward(method, node.leader, data))
                if method == "GET":
                    with node.lock:
                        return self.reply(200, {"key": key, "value": node.kv.get(key), "from": NODE_ID})
                if method == "PUT":
                    return self.reply(*node.client_write({"op": "put", "key": key, "value": data.decode()}))
                if method == "DELETE":
                    return self.reply(*node.client_write({"op": "del", "key": key}))
            self.reply(404, {"error": "not found"})
        except Blocked:
            self.reply(503, {"error": "blocked"})
        except Exception as e:
            self.reply(500, {"error": str(e)})

    do_GET = lambda self: self.route("GET")
    do_PUT = lambda self: self.route("PUT")
    do_POST = lambda self: self.route("POST")
    do_DELETE = lambda self: self.route("DELETE")


if __name__ == "__main__":
    threading.Thread(target=node.run, daemon=True).start()
    log(f"khởi động, cổng {PORT}, cluster {len(ALL)} node, đa số = {MAJORITY}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
```

### 4.2 `Dockerfile`

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY node.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "node.py"]
```

### 4.3 `.env` (cụm 3 node)

```
PEERS=n1=http://n1:8000,n2=http://n2:8000,n3=http://n3:8000
```

### 4.4 `docker-compose.yml`

```yaml
services:
  n1:
    build: .
    container_name: n1
    environment: { NODE_ID: n1, PEERS: "${PEERS:?thiếu file .env}", PORT: "8000", DATA_DIR: /data }
    ports: ["8001:8000"]
  n2:
    build: .
    container_name: n2
    environment: { NODE_ID: n2, PEERS: "${PEERS:?thiếu file .env}", PORT: "8000", DATA_DIR: /data }
    ports: ["8002:8000"]
  n3:
    build: .
    container_name: n3
    environment: { NODE_ID: n3, PEERS: "${PEERS:?thiếu file .env}", PORT: "8000", DATA_DIR: /data }
    ports: ["8003:8000"]
  n4:
    build: .
    container_name: n4
    profiles: ["five"]
    environment: { NODE_ID: n4, PEERS: "${PEERS:?thiếu file .env}", PORT: "8000", DATA_DIR: /data }
    ports: ["8004:8000"]
  n5:
    build: .
    container_name: n5
    profiles: ["five"]
    environment: { NODE_ID: n5, PEERS: "${PEERS:?thiếu file .env}", PORT: "8000", DATA_DIR: /data }
    ports: ["8005:8000"]
```

---

## 5. Chạy hệ thống

### 5.1 Cụm 3 node

```bash
docker compose up -d --build
docker compose logs -f          # xem log bầu cử (Ctrl+C để thoát)
```

Sau vài giây sẽ thấy dòng kiểu `[n2] *** trở thành LEADER, term 1 ***`.

### 5.2 Cụm 5 node

Sửa `.env`:

```
PEERS=n1=http://n1:8000,n2=http://n2:8000,n3=http://n3:8000,n4=http://n4:8000,n5=http://n5:8000
```

Rồi chạy:

```bash
docker compose down
docker compose --profile five up -d --build
```

### 5.3 Lệnh kiểm tra nhanh

Xem trạng thái các node (đổi danh sách cổng thành `8001 8002 8003 8004 8005` nếu chạy 5 node):

```bash
for p in 8001 8002 8003; do curl -s -m2 localhost:$p/status || echo "cổng $p: DOWN"; echo; done
```

Mỗi node trả về JSON gồm `role` (leader/follower/candidate), `term`, `leader`, `log_len`, `commit`, `kv` và `blocked`.

### 5.4 API

| Lệnh | Ý nghĩa |
|---|---|
| `curl -X PUT -d 'Hoang' localhost:8001/kv/name` | Ghi key `name` (gửi node nào cũng được) |
| `curl localhost:8002/kv/name` | Đọc qua leader |
| `curl "localhost:8003/kv/name?local=1"` | Đọc thẳng từ **node đó** (để chứng minh dữ liệu đã nhân bản) |
| `curl -X DELETE localhost:8001/kv/name` | Xóa key |
| `curl localhost:8001/status` | Trạng thái node |
| `curl -X POST -d '{"peers":["n1","n2","n3"]}' localhost:8001/admin/block` | Node 8001 ngừng liên lạc với danh sách node (mô phỏng partition) |
| `curl -X POST -d '{"peers":[]}' localhost:8001/admin/block` | Gỡ chặn |

---

## 6. Kịch bản demo (làm theo thứ tự)

Dùng cụm 3 node. Các số cổng: n1=8001, n2=8002, n3=8003.

### Kịch bản 1: Bầu leader (mục 8.2.1, 8.2.3)

```bash
docker compose logs | grep -E "LEADER|bỏ phiếu"
```

**Giải thích**: lúc khởi động mọi node là follower, node nào hết timeout trước sẽ ứng cử và nhận phiếu của các node còn lại. Timeout ngẫu nhiên giúp tránh việc nhiều node ứng cử cùng lúc.

### Kịch bản 2: Ghi và nhân bản dữ liệu (mục 8.2.3)

```bash
curl -X PUT -d 'Hoang' localhost:8001/kv/name      # gửi vào node bất kỳ
curl "localhost:8001/kv/name?local=1"
curl "localhost:8002/kv/name?local=1"
curl "localhost:8003/kv/name?local=1"
```

**Kết quả mong đợi**: cả 3 node trả về `"value": "Hoang"`. Dữ liệu đã được nhân bản tới mọi node. Nếu gửi vào follower thì nó chuyển tiếp cho leader.

### Kịch bản 3: Một follower chết, hệ thống vẫn chạy (k-fault tolerance)

```bash
docker stop n3
curl -X PUT -d 'HN' localhost:8001/kv/city
```

**Kết quả**: vẫn ghi được (`"ok": true`) vì còn 2/3 node, tức vẫn đủ đa số. Cụm 3 node chịu được **k=1** lỗi crash (2k+1 = 3).

Bật lại và xem node bắt kịp dữ liệu:

```bash
docker start n3
sleep 3
curl "localhost:8003/kv/city?local=1"
```

### Kịch bản 4: Leader chết, bầu leader mới (mục 8.2.2 primary-backup)

```bash
# tìm leader qua /status rồi dừng đúng container đó, ví dụ n2
docker stop n2
sleep 4
for p in 8001 8003; do curl -s -m2 localhost:$p/status; echo; done
```

**Kết quả**: một node khác thành leader với `term` tăng thêm 1. Dữ liệu cũ vẫn còn, ghi mới vẫn hoạt động. Đây là cơ chế **failure detection bằng timeout** (mục 8.2.8): follower thấy leader im lặng quá lâu thì cho rằng nó đã crash.

### Kịch bản 5: Mất đa số, hệ thống ngừng ghi (giới hạn của 2k+1)

Khi đã tắt n2, tắt thêm n3 (cụm chỉ còn 1/3 node):

```bash
docker stop n3
curl -m8 -X PUT -d 'x' localhost:8001/kv/fail
```

**Kết quả**: trả về lỗi (timeout hoặc `chưa có leader`). Vì không còn đa số nên hệ thống **từ chối ghi** thay vì ghi bừa. Đây là lựa chọn **an toàn (consistency) hơn sẵn sàng (availability)** ở mục CAP. Bật lại hai node rồi hệ thống tự hồi phục:

```bash
docker start n2 n3
```

### Kịch bản 6: Network partition và split-brain (mục 8.2.7 CAP)

Cụm 3 node, giả sử leader hiện tại là `nX`. Cô lập leader đó (ví dụ n1, cổng 8001):

```bash
curl -X POST -d '{"peers":["n1","n2","n3"]}' localhost:8001/admin/block
sleep 5
for p in 8001 8002 8003; do curl -s -m2 localhost:$p/status; echo; done
```

**Kết quả**: phía đa số (n2, n3) bầu leader mới với `term` cao hơn, còn n1 vẫn tưởng mình là leader (`term` cũ). Thử ghi vào hai phía:

```bash
curl -m8 -X PUT -d 'cũ' localhost:8001/kv/lost     # leader cũ bị cô lập -> lỗi timeout
curl -m8 -X PUT -d 'mới' localhost:8002/kv/y       # phía đa số -> ok
```

Gỡ chặn:

```bash
curl -X POST -d '{"peers":[]}' localhost:8001/admin/block
sleep 4
curl "localhost:8001/kv/lost?local=1"    # value: null, entry không commit bị xóa
curl "localhost:8001/kv/y?local=1"       # value: "mới"
```

**Giải thích**: leader cũ nhận `AppendEntries` có `term` cao hơn nên tự hạ xuống follower, entry chưa commit của nó bị **ghi đè** bởi log của leader mới. Không có dữ liệu sai nào được xác nhận cho client, nên không xảy ra split-brain.

> Có thể dùng thêm `docker network disconnect` để cắt mạng thật, nhưng khi đó cổng publish của container cũng bị mất nên không `curl` vào node đó được. Dùng `/admin/block` tiện hơn khi trình diễn.

### Kịch bản 7: Khôi phục sau crash (log bền vững)

```bash
docker restart n1
docker logs n1 | head
```

**Kết quả**: log có dòng `khôi phục: term=..., log=... entries`. Node đọc lại `term`, `voted_for` và `log` từ file, rồi được leader cập nhật phần còn thiếu.

---

## 7. Gợi ý cách trình bày với giảng viên

1. Nêu vấn đề: một server đơn lẻ chết là mất dịch vụ (khái niệm availability, mục 8.1.1).
2. Giải pháp: dùng **physical redundancy** (nhân bản n node, mục 8.1.3) trong một **process group** (mục 8.2.1).
3. Vấn đề mới: các bản sao phải **đồng thuận** về thứ tự lệnh (mục 8.2.3) nên dùng Raft.
4. Demo theo thứ tự kịch bản 2, 3, 4, 5, 6.
5. Kết luận: cụm 2k+1 node chịu k lỗi crash, chịu partition bằng cách hy sinh availability của phía thiểu số (CAP).

Câu hỏi thường gặp:

- **Vì sao phải đa số?** Hai đa số bất kỳ luôn giao nhau ít nhất 1 node, nên không thể có hai leader cùng term cùng được xác nhận.
- **Vì sao cụm 4 node không tốt hơn cụm 3?** Đa số của 4 là 3, vẫn chỉ chịu được 1 lỗi như cụm 3 nhưng tốn thêm một node.
- **Raft có chịu được node nói dối (Byzantine) không?** Không. Raft chỉ chịu crash failure; muốn chịu arbitrary failure cần 3k+1 node và giao thức như PBFT (mục 8.2.5).

---

## 8. Giới hạn của bản demo (nên nói rõ khi báo cáo)

- Chưa có **snapshot/log compaction**, log lớn dần.
- Chưa hỗ trợ **thay đổi thành viên** (thêm/bớt node khi đang chạy); danh sách node cố định qua biến `PEERS`.
- Đọc từ leader chưa dùng *read-index/lease*, nên leader bị cô lập vẫn có thể trả dữ liệu cũ trong thời gian ngắn trước khi nó biết mình đã mất quyền.
- Chưa có **pre-vote**, nên node bị cô lập tăng `term` liên tục và khi nối lại có thể làm leader hiện tại bị hạ xuống một lần (bạn sẽ thấy `term` nhảy nhiều ở kịch bản 6, hoàn toàn bình thường).
- `fsync` chưa được gọi khi lưu file, đủ cho demo nhưng chưa đủ bền cho môi trường thật.
- Lệnh chưa có ID chống trùng, nếu client gửi lại sau timeout thì lệnh có thể bị thực thi hai lần.

## 9. Hướng mở rộng

- Thêm trang web dashboard hiển thị vai trò, term, log của từng node theo thời gian thực (gọi `/status` mỗi giây).
- Thêm tham số `?consistent=1` để so sánh đọc local (nhanh, có thể cũ) với đọc qua leader (chính xác).
- Thêm lệnh CAS (compare-and-set) để thấy lợi ích của thứ tự lệnh toàn cục.
- Thay phần failure detection bằng bộ phát hiện thích nghi (tăng timeout sau mỗi lần nhầm, Note 8.7).
- Viết bộ test tự động: kill ngẫu nhiên các node, vừa ghi vừa kiểm tra không mất dữ liệu đã commit.

## 10. Xử lý sự cố thường gặp

| Hiện tượng | Nguyên nhân / cách xử lý |
|---|---|
| `curl` báo `Connection refused` | Container chưa lên hoặc đã bị dừng; kiểm tra `docker compose ps` |
| Mãi không có leader | Số node sống không đủ đa số, hoặc `.env` ghi sai danh sách `PEERS` |
| Lỗi `thiếu file .env` | Chưa tạo file `.env` cùng thư mục với `docker-compose.yml` |
| Chạy 5 node nhưng chỉ thấy 3 container | Quên `--profile five` khi chạy `docker compose up` |
| Sửa `PEERS` mà không có tác dụng | Chạy `docker compose down` rồi `up` lại; muốn xóa sạch dữ liệu cũ thêm `docker compose down -v` hoặc xóa container |
| Đổi từ 3 sang 5 node bị lỗi lạ | Log cũ lưu trong container; `docker compose down` rồi `up` lại để tạo container mới |
