# Raft KV Store

Project này triển khai kho key-value phân tán bằng Raft cho cụm Docker 5 node
mặc định, với chuẩn quorum $2f+1=3$ cho hệ thống sai sót crash. Mỗi node dùng
Python standard library, lưu term/vote/log trên volume riêng và chỉ xác nhận ghi
sau khi đa số node đã lưu entry.

## Chạy cụm 5 node

Yêu cầu Docker Engine và Docker Compose:

```powershell
docker compose up -d --build
docker compose logs -f
```

Chờ một node trở thành leader, sau đó gọi API bằng `curl.exe`:

Mở dashboard demo tại [http://localhost:8001](http://localhost:8001) để theo dõi
leader, term, commit index, trạng thái key-value và mô phỏng cô lập/khôi phục node.
Các cổng demo chỉ bind trên máy local.

```powershell
curl.exe -X PUT --data-binary "Hoang" http://localhost:8001/kv/name
curl.exe http://localhost:8002/kv/name
curl.exe "http://localhost:8003/kv/name?local=1"
curl.exe http://localhost:8001/status
```

Các cổng host là `8001`–`8005`. Gửi PUT/GET/DELETE tới bất kỳ node nào; follower
chuyển tiếp request tới leader. Thêm `?local=1` cho GET để đọc trực tiếp bản sao
của node đó, có thể thấy dữ liệu chưa được cập nhật tức thời.

## Demo Paxos và PBFT

Chọn tab giao thức trên dashboard:

- **Raft** là cụm Docker thật, có thể ghi key-value và mô phỏng network partition.
- **Paxos** là sandbox cục bộ với ba acceptor và hai proposer. Bắt đầu P1 và P2
  trước khi giao message để xem ballot `(m, i)` lớn hơn preempt proposer còn lại;
  dùng `Deliver next` hoặc `Drop next` để điều khiển từng message. Proposer phải
  chọn lại value có accepted ballot cao nhất nếu Promise báo đã có value trước đó.
- **PBFT** cho phép chọn 3 hoặc 4 replicas, tiêm primary/backup nói dối, giao từng
  PRE-PREPARE, PREPARE, COMMIT và chủ động đổi view. Với 4 replicas, PBFT chịu
  được `f = 1` Byzantine replica (`n >= 3f + 1`); với 3 replicas chỉ có `f = 0`,
  nên thử một replica nói dối nằm ngoài fault bound và dashboard sẽ cảnh báo không
  thể đảm bảo kết quả. Khi primary đưa sai request, replicas trung thực từ chối;
  trigger view change để primary mới thử lại request gốc.

Paxos và PBFT trong dashboard là mô phỏng từng bước chạy trên trình duyệt, không
phải các node distributed chạy trong Docker và không thay đổi dữ liệu Raft.

## Kịch bản demo 5-node cho bảo vệ đề tài

1. Mở mục Raft LIVE CLUSTER: hệ thống có 5 node `n1`-`n5`, trong đó quorum là 3 node.
   - Đặt mục tiêu: chứng minh rằng một leader được bầu và hệ thống vẫn hoạt động khi 1 hoặc 2 node mất kết nối, miễn là vẫn còn đa số khỏe mạnh.
2. Cho chạy đồng thời `docker compose up -d --build` và quan sát leader mới được bầu trên dashboard.
   - Hệ thống sẽ chọn một node làm leader, các node còn lại chuyển sang `follower` và về cơ bản chờ replicate.
3. Gửi một write từ một follower, ví dụ `PUT /kv/service`.
   - Hệ thống phải kiểm tra tính hợp lệ: leader nhận request, đẩy log tới các follower, và chỉ khi có tối thiểu 3/5 node xác nhận thì commit được ghi.
4. Tiến hành partition: chọn "Isolate n1" hoặc "Isolate n2".
   - Nếu 3 node còn lại vẫn kết nối, cluster không bị treo; ý nghĩa là Raft vẫn phục vụ được client với quorum 3/5.
   - Nếu cô lập tới mức còn lại dưới đa số, hệ thống sẽ không commit thêm write mới cho tới khi liên lạc được lại.
5. Chuyển sang tab Paxos MESSAGE SANDBOX.
   - Chạy `P1` với ballot `(1,1)` và `P2` với ballot `(1,2)`. Vì `(1,2) > (1,1)` theo thứ tự lexicographic, proposer có ballot cao hơn sẽ thắng khi nhận được promise từ đa số acceptor.
   - Đây là minh họa rõ ràng cho khái niệm preemption, nơi proposer thấp hơn bị chặn khỏi việc learn value.
6. Chuyển sang tab PBFT BYZANTINE SANDBOX.
   - Chọn 5 replicas và kích hoạt `Primary lies` hoặc `Backup lies`.
   - Một primary nói dối sẽ gửi PRE-PREPARE sai lệch; các replica trung thực sẽ phát hiện mismatch và từ chối, sau đó bắt đầu view change.
   - Khi view change diễn ra, primary mới thay thế và client request được replay để đảm bảo tính nhất quán của quyết định.
7. Kết luận trong bảo vệ:
   - Raft giải quyết crash fault tolerance bằng plurality/majority commit và leader election.
   - Paxos giải quyết sự đồng thuận trong môi trường crashed-but-honest nodes.
   - PBFT mở rộng để chịu sự cố Byzantine bằng cách yêu cầu `n >= 3f + 1` và kiểm tra client request qua certificate từ nhiều replica.

## Mô tả từng state của Paxos và PBFT

### Paxos: cycle của một instance

- `idle`: proposer chưa bắt đầu instance mới; system đang chờ một request tới từ client hoặc một proposal mới.
- `preparing`: proposer gửi `PREPARE` tới từng acceptor với ballot `(m, i)`. Mỗi acceptor so sánh ballot nhận được với ballot đã hứa trước đó. Nếu mới lớn hơn hoặc bằng, acceptor trả lời `PROMISE` và cam kết không chấp nhận ballot thấp hơn nữa.
- `accepting`: khi proposer nhận được đa số `PROMISE`, nó chọn value có accepted ballot cao nhất từ các promise, nếu có, rồi gửi `ACCEPT` tới tất cả acceptor. Đây là bước quyết định giá trị cần được xác nhận.
- `learned`: khi số lượng acceptor phản hồi `ACCEPTED` đạt quorum, value được coi là learned và không còn bị thay đổi bởi bất kỳ proposer nào khác trong cùng round.
- `preempted`: nếu acceptor trả `NACK` với một ballot lớn hơn, proposer phải ngừng phiên chuẩn bị hiện tại và quay lại với ballot mới để tránh xung đột.

### PBFT: trạng thái của một request trong 5 replica

- `pre-prepare`: primary nhận request từ client và gửi PRE-PREPARE kèm sequence number và value tới tất cả replica. Đây là bước khởi đầu của một quyết định.
- `prepare`: mỗi replica không tin primary lập tức chấp nhận; nó kiểm tra request, nếu hợp lệ thì gửi PREPARE và thu thập các vote từ các replica khác. Tại đây, replica trung thực chỉ chuyển tiếp request nếu giá trị khớp với client request.
- `prepared`: khi một replica tích lũy đủ `PREPARE` từ quorum, nó có thể coi request là đã được chuẩn bị. Đây là điểm mà replica biết rằng đa số node đang đồng thuận về cùng một value cho sequence đó.
- `commit`: sau khi trạng thái prepared đạt được, replica gửi `COMMIT` và chờ certificate đủ mạnh. Nếu nhận được quorum commit, replica mới commit value chính thức.
- `view-change`: khi primary sai hoặc không phản hồi đúng, các replica phát hiện tình trạng bất thường và bắt đầu chuyển sang view mới. Mục tiêu là thay primary hỏng bằng primary mới, không phá vỡ tính nhất quán của toàn hệ thống.
- `new-view`: sau khi đủ số `VIEW-CHANGE` đạt quorum, primary mới được chấp nhận và replay lại request ban đầu để tái đồng thuận với hệ thống.
- `rejected`: nếu request hoặc value không khớp digest/điều kiện xác thực, replica trung thực sẽ từ chối. Điều này là nguyên lý cốt lõi của PBFT để bảo vệ chống Byzantine fault.

Nếu bạn muốn tùy biến peer list, chỉnh `.env` thành:

```text
PEERS=n1=http://n1:8000,n2=http://n2:8000,n3=http://n3:8000,n4=http://n4:8000,n5=http://n5:8000
```

Sau đó chạy:

```powershell
docker compose down
docker compose up -d --build
```

Dữ liệu node được giữ trong Docker named volume; `docker compose down -v` sẽ xóa
các volume này.

## API

| Request                                         | Mô tả                                        |
| ----------------------------------------------- | -------------------------------------------- |
| `PUT /kv/{key}` với body là giá trị dạng text   | Ghi có đồng thuận đa số                      |
| `GET /kv/{key}`                                 | Đọc qua leader                               |
| `GET /kv/{key}?local=1`                         | Đọc trực tiếp từ node hiện tại               |
| `DELETE /kv/{key}`                              | Xóa key qua Raft                             |
| `GET /status`                                   | Vai trò, term, commit index và dữ liệu local |
| `POST /admin/block` với `{"peers":["n1","n2"]}` | Chặn RPC gửi đến/từ các node chỉ định        |
| `POST /admin/block` với `{"peers":[]}`          | Bỏ chặn                                      |

Ví dụ cô lập node n1 rồi gỡ chặn:

```powershell
curl.exe -X POST -H "Content-Type: application/json" --data-binary '{"peers":["n1","n2","n3"]}' http://localhost:8001/admin/block
curl.exe -X POST -H "Content-Type: application/json" --data-binary '{"peers":[]}' http://localhost:8001/admin/block
```

## Kiểm thử

Không cần Docker hoặc package ngoài:

```powershell
python -m unittest discover -s tests -v
```

Đây là bản demo học tập, không phải Raft production-ready. Phạm vi còn thiếu
gồm membership changes, snapshot/log compaction, read-index/lease, pre-vote và
deduplication cho client request. API `/admin/block` chỉ phục vụ mô phỏng lỗi,
không có xác thực và không nên expose ra mạng công cộng.

Xem thêm [hướng dẫn đề tài](../huong-dan-raft-kv-docker.md) để tham khảo các
kịch bản demo về crash failure, leader election và network partition.
