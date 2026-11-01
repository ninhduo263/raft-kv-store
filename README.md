# Raft KV Store

Project này triển khai kho key-value phân tán bằng Raft cho cụm Docker 3 hoặc
5 node. Mỗi node dùng Python standard library, lưu term/vote/log trên volume
riêng và chỉ xác nhận ghi sau khi đa số node đã lưu entry.

## Chạy cụm 3 node

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

Các cổng host là `8001`–`8003`. Gửi PUT/GET/DELETE tới bất kỳ node nào; follower
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

## Chạy cụm 5 node

Sửa `.env` thành:

```text
PEERS=n1=http://n1:8000,n2=http://n2:8000,n3=http://n3:8000,n4=http://n4:8000,n5=http://n5:8000
```

Sau đó chạy:

```powershell
docker compose down
docker compose --profile five up -d --build
```

Khôi phục `.env` về 3 node để chạy lại cụm mặc định. Dữ liệu node được giữ trong
Docker named volume; `docker compose down -v` sẽ xóa các volume này.

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
