# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `<điền>` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `<điền>` |
| GitHub | `<điền: tên tài khoản>` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

A có `ts_utc` dạng ISO UTC, `speed_mps` tính bằng m/s, `heading_deg`, `lat`/`lon`, `gas`/`brake` dạng tỷ lệ. B có `timestamp_ms` dạng epoch mili giây, `spd_kmh` tính bằng km/h, `hdg`, `latitude`/`longitude`, `throttle_pct`/`brake_pct` dạng phần trăm; đồng hồ B còn nhanh hơn video 1.500 ms. Bronze giữ hai schema gốc ở hai bảng để không làm mất thông tin hoặc ép sai các ô trống trước khi kiểm tra. Bước 1 nhận 3.021 dòng A và 3.234 dòng B; nạp lại A báo `already present` và không thêm dòng.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging A ép `ts_utc` sang thời gian UTC và giữ `speed_mps`; staging B đổi `timestamp_ms` sang thời gian UTC sau khi trừ offset 1.500 ms từ `bronze.video_files`, đổi `spd_kmh / 3.6` sang m/s và các cột bàn đạp phần trăm sang tỷ lệ. Silver sạch có 2.968 dòng A và 3.181 dòng B; thời điểm đầu B là `2025-10-08 14:30:00.009+00:00`, lệch 9 ms so với mốc video. Quarantine giữ 106 dòng cùng lý do để có thể kiểm tra nguồn và tái xử lý, thay vì xóa mất bằng chứng.

| Drive | Trùng | Thiếu tốc độ | Tốc độ ngoài khoảng |
|---|---:|---:|---:|
| A | 20 | 30 | 3 |
| B | 20 | 30 | 3 |

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` chỉ cảnh báo đoạn cảm biến mất tín hiệu đã biết: bước 2 có `PASS=6 WARN=1 ERROR=0`, nên pipeline vẫn chạy và khung mất đồng bộ sẽ được xử lý ở bước sau. `in_range` chặn tốc độ vô lý trước khi dùng để huấn luyện. Trong `break_gate.json`, khi tạm nới luật nhận tốc độ lên 1.000 m/s, `in_range_silver_telemetry_speed_mps__70__0` phát hiện 6 dòng, `PASS=5 WARN=1 ERROR=1`; cổng lỗi dừng dữ liệu xấu. Khôi phục luật 70 m/s cho `PASS=6 WARN=1 ERROR=0`.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Với `trip-00003`, bảng Silver có 1.246 dòng trước và sau hai lần chạy lại; fingerprint đều là `6c7480dfde7e`. Trip này có 60 frame, nhưng mô phỏng `append` hai lần tạo 120 dòng, tức nhân đôi. Delta `overwrite` có predicate `trip_id` thay thế đúng partition và commit nguyên tử: nếu trích xuất hoặc ghi lỗi giữa chừng, phiên bản bảng cũ vẫn đọc được; xoá rồi ghi lại có khoảng trống làm mất dữ liệu của trip.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Frame đen gần như không có tín hiệu hình ảnh nên bị loại; frame tối do đường hầm hoặc bóng râm vẫn có cảnh hữu ích, được gắn cờ để xem xét nhưng giữ lại. Trong 1.246 frame, A có 6 đen và 4 tối, B có 4 đen và 61 tối; cách xử lý này hợp lý cho dữ liệu lái xe ban đêm. Tôi chọn ngưỡng dHash 2 đang dùng: ngưỡng 0 dễ bỏ sót ảnh rất giống nhau, còn ngưỡng 6 có thể loại quá nhiều cảnh thật (A 363, B 244). Xe chạy chậm cho nhiều frame liên tiếp giống nhau hơn, nên tần suất trùng và ngưỡng thích hợp phụ thuộc tốc độ, tốc độ lấy mẫu và chuyển động camera.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
