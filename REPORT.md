# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Khánh Đô |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602687` |
| GitHub | `Diohus` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-09608` |

## Kết quả cuối

- `dataset_version`: `62a803b13357`
- Số khung hình train / val / testtest: `711 / 174 / 134`.
- `report/verify.txt`: `27/27 checks passed` ở lượt chạy cuối từ lake trống.
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, `airflow_failed_graph.png`, `fail_task_log.png`, `airflow_retry_graph.png`.

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

Trong 1.019 frame đạt điều kiện của 26 trip, chia ngẫu nhiên theo frame làm cả 26 trip nằm ở nhiều split và cả 26 có frame ở train lẫn test. Chia theo trip trong Gold làm 0 trip bị chia cắt. Các frame liền nhau cùng cảnh, vật thể và điều kiện ánh sáng rất giống nhau; khi cảnh của tập train xuất hiện lại trong test, mô hình có thể ghi nhớ thay vì tổng quát hóa, khiến điểm test cao giả tạo.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Pipeline băm danh sách `frame_id|split` đã sắp xếp, nên cùng tập frame và cách chia cho cùng mã `62a803b13357` dù export lại ở thời điểm khác. Hai lần export liên tiếp của tôi đều cho phiên bản này, với 1.019 frame: train 711, val 174, test 134. MSSV được nối vào `trip_id` khi sắp thứ tự băm MD5 để gán split: thử `2A202602688` cho mã `0bed563a4be9` và split 707/167/145; khôi phục `2A202602687` thì mã trở lại `62a803b13357`. Ngày giờ hoặc số đếm không chứng minh hai dataset có cùng nội dung.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`manifest.json` cuối của `62a803b13357` ghi Bronze `raw_telemetry_a=0`, `raw_telemetry_b=0`, `trips_raw=0`, `video_files=1`; Silver `frames=25`, `telemetry=0`, `frames_flagged=0`, `frame_telemetry=0`. Hai video nguồn có SHA256 A `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa` và B `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Để tái tạo sau 3 tháng cần đúng file nguồn theo SHA256, phiên bản bảng/luật xử lý, MSSV `2A202602687` dùng cho split và danh sách `frame_id|split` của manifest; mã băm manifest Parquet là `582eba33134264397e620f9dad7b74bfaca4171f32e3655f5137c1cad56ece69`.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Sau reset, đúng `silver_frames[25]` của `trip-09608` chuyển đỏ; log trong `fail_task_log.png` ghi `simulated crash while extracting trip-09608`. Bảy task phụ thuộc ở trạng thái `upstream_failed`. Tôi tắt tùy chọn Downstream và clear đúng task đỏ (Affected Tasks: 1); nó chạy lại thành công. Airflow 2.10 không tự đặt lại bảy trạng thái `upstream_failed`, nên tôi clear tiếp chỉ bảy task này (Only Failed, Affected Tasks: 7) để DAG xanh như `airflow_retry_graph.png`. Không task frame tốt nào chạy lại; kiểm tra sau retry vẫn `27/27`, Silver có 1.246 `frame_id` duy nhất, Gold có 1.019 frame và `dataset_version` giữ `62a803b13357`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(1) Trích frame bằng FFmpeg trong một container Airflow với `max_active_tis_per_dagrun=1` sẽ thành hàng đợi khoảng 6.500 trip/ngày nếu vẫn 13 trip/xe; chuyển sang worker phân tán với hàng đợi và autoscaling. (2) DuckDB ghi một file cục bộ để dbt quét toàn bộ frame/telemetry sẽ nghẽn CPU, RAM và I/O; dùng Delta trên object storage với engine phân tán, xử lý tăng dần theo partition/event time. (3) Postgres phục vụ mart/gallery ảnh và Metabase trên một máy sẽ quá tải truy vấn đồng thời; dùng warehouse cột, mart tổng hợp trước và giới hạn gallery theo trang. Cần đo lưu lượng ingest và độ trễ từng chặng trước khi chọn kích thước cụm.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Tôi thêm câu hỏi “Trip nào bị loại nhiều frame nhất?” vào dashboard, dùng SQL:

```sql
select drive_id, trip_id, frames, frames_kept,
       round(100.0*(frames-frames_kept)/nullif(frames,0),1) as drop_pct
from marts.mart_trip_summary
order by drop_pct desc limit 5;
```

`drive_a/trip-04622` đứng đầu: 60 frame, giữ 26, loại 34 (`56,7%`). Đây là trip cần xem lại ảnh và cờ chất lượng trước khi quyết định điều chỉnh ngưỡng; tỷ lệ cao tự nó chưa chứng minh camera hỏng.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Bất ngờ nhất là 61 frame tối ở drive B vẫn được giữ, trong khi chỉ 4 frame đen bị loại: độ sáng thấp không đồng nghĩa ảnh vô dụng. Tôi còn muốn hiểu rõ cách chọn ngưỡng dHash theo tốc độ xe thay vì cố định 2 bit. Một lỗi chưa có test rõ là GPS nhảy vị trí phi thực tế dù từng giá trị tọa độ vẫn hợp lệ; cần kiểm tra quãng đường và tốc độ suy ra giữa hai mốc thời gian. Khi chỉ clear task đỏ, Airflow 2.10 giữ bảy task `upstream_failed`; phải clear riêng chúng để DAG tiếp tục.
