# End-to-End Event-to-Warehouse Pipeline: Analisis Transaksi E-Commerce

Mini project Praktikum Data Processing Workflow. Pipeline ini menggabungkan dua jalur masuk data (batch bergaya REST API dan streaming Kafka), menegakkan data contract dengan Pydantic, mentransformasi data dengan Polars, memuatnya ke DuckDB, lalu membangun model analitik dengan dbt.

Data yang dipakai adalah data sintetis (`data/orders.csv`, 36 pesanan O1001–O1036). Enam pesanan terakhir (O1031–O1036) sengaja cacat untuk menguji data contract.

## Arsitektur

```
data/orders.csv (simulasi REST API, pagination)
        |
        v
batch_ingest.py  -- retry/backoff + watermark -->  data/raw/orders.parquet
        |
        v
validate.py (Pydantic: contract.py) --> data/valid/valid_orders.parquet
        |                           \-> data/rejected/rejected_orders.jsonl
        |
producer.py --> Kafka topic "orders" --> consumer.py (contract.py yang sama)
                                          |--> data/stream/valid_events.jsonl
                                          \--> data/stream/rejected_events.jsonl
        |
        v
transform.py (Polars: gabung batch + stream, hapus duplikat order_id,
              hitung gross_amount dan net_amount)
        |
        v
load_duckdb.py --> warehouse/warehouse.duckdb (tabel raw_orders)
        |
        v
dbt: stg_orders --> fct_orders, dim_customer, dim_product_category --> mart_daily_sales
        |
        v
dbt test + dbt docs  |  run_queries.py (5 pertanyaan analitik)
```

Kedua jalur masuk memakai satu kontrak data (`scripts/contract.py`), sehingga aturan validasi konsisten.

## Struktur Folder

```
.
├── data/
│   ├── orders.csv                 # sumber data sintetis (36 baris)
│   ├── raw/orders.parquet         # hasil ingestion (string apa adanya)
│   ├── valid/                     # valid_orders.parquet, orders_transformed.parquet
│   ├── rejected/                  # rejected_orders.jsonl (batch)
│   ├── stream/                    # valid_events.jsonl, rejected_events.jsonl
│   └── bench/                     # dataset benchmark (dibuat skrip, di-.gitignore)
├── scripts/
│   ├── batch_ingest.py            # pagination, retry + exponential backoff, watermark
│   ├── contract.py                # model Pydantic Order
│   ├── validate.py                # validasi batch, mode karantina / --fail-fast
│   ├── producer.py                # Kafka producer
│   ├── consumer.py                # Kafka consumer + validasi + sink idempoten
│   ├── transform.py               # transformasi Polars
│   ├── verify_calc.py             # verifikasi perhitungan manual
│   ├── benchmark.py               # Pandas vs Polars (eager/lazy) + eksperimen skew
│   ├── load_duckdb.py             # muat ke DuckDB
│   └── run_queries.py             # query analitik
├── dbt_project/                   # model staging dan marts, tes, profiles.yml
├── warehouse/warehouse.duckdb     # data warehouse lokal
├── docker-compose.yml             # Kafka single-node (apache/kafka:3.9.0, KRaft)
└── requirements.txt
```

## Prasyarat

- Python 3.12 (diuji dengan 3.12.10 di Windows 11)
- Docker Desktop (untuk Kafka)
- Dependensi Python: pandas, pyarrow, pydantic, polars, kafka-python, duckdb, dbt-duckdb, requests (lihat `requirements.txt`)

## Instalasi

Jalankan semua perintah dari **root proyek** (path di skrip bersifat relatif).

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1          # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

Buat folder yang tidak ikut repositori:

```powershell
New-Item -ItemType Directory -Force data/bench, warehouse   # Linux/macOS: mkdir -p data/bench warehouse
```

## Menjalankan Pipeline

### 1. Batch ingestion

```powershell
python scripts/batch_ingest.py --source data/orders.csv --out data/raw
```

Harapan run pertama: `page=0 rows=25`, `page=1 rows=11`, `SUMMARY read=36 after_watermark=36 appended=36 total=36`.
Jalankan sekali lagi untuk menguji idempotensi: `after_watermark=0 appended=0 total=36`.

Uji retry dengan kegagalan simulasi (log `[retry] ... tunggu=0.5s`, lalu `1.0s`):

```powershell
python scripts/batch_ingest.py --fail-rate 0.4
```

Untuk mengulang dari nol, hapus `data/raw/orders.parquet` dan `data/watermark.json`.

### 2. Validasi data contract

```powershell
python scripts/validate.py              # mode karantina per record
python scripts/validate.py --fail-fast  # berhenti bila ada pelanggaran
```

Harapan: `total=36 valid=30 rejected=6`. Alasan penolakan tersimpan di `data/rejected/rejected_orders.jsonl`.

### 3. Streaming Kafka

```powershell
docker compose up -d
docker exec dpw-kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
python scripts/producer.py                  # kirim 36 event (key = order_id)
python scripts/consumer.py                  # validasi + tulis ke data/stream/
```

Harapan consumer: `{'valid': 30, 'rejected': 6, 'duplicate': 0}`. Cek offset:

```powershell
docker exec dpw-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group dpw-orders
```

Uji duplikasi dan JSON rusak (setelah `data/stream/` berisi hasil run utama):

```powershell
python scripts/producer.py --limit 1 --duplicate O1001 --bad-json
python scripts/consumer.py                  # harapan: duplicate=2, rejected=1
```

Untuk mengulang dari nol, hapus folder `data/stream/` dan topic atau gunakan `--group` baru.

### 4. Transformasi dan verifikasi

```powershell
python scripts/transform.py        # data/valid/orders_transformed.parquet
python scripts/verify_calc.py      # O1001 net=237.5, O1003 net=35.073 -> COCOK
```

`transform.py` menggabungkan data valid batch dan stream, lalu menghapus duplikat `order_id`.

### 5. Benchmark dan eksperimen skew

```powershell
python scripts/benchmark.py --copies 20000
```

Membuat dataset 600.000 baris (normal dan skew: 80% baris dipaksa ke `C001`, seed 42), lalu mencetak median waktu Pandas, Polars eager, dan Polars lazy (warm-up 1x, median 5x).

### 6. Warehouse DuckDB

```powershell
python scripts/load_duckdb.py      # tutup koneksi sebelum menjalankan dbt
```

### 7. dbt

```powershell
cd dbt_project
dbt debug --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .    # buka http://127.0.0.1:8080
cd ..
```

### 8. Query analitik

```powershell
python scripts/run_queries.py
```

## Data Contract

Didefinisikan di `scripts/contract.py` (Pydantic v2, `extra="forbid"`).

| Atribut | Aturan |
|---|---|
| order_id, customer_id | wajib, tidak kosong |
| product_category, payment_method | wajib, tidak kosong |
| quantity | integer > 0 |
| unit_price | angka >= 0 |
| discount_pct | 0 sampai 1 |
| order_ts | harus dapat diparse sebagai datetime |
| status | paid, shipped, cancelled, atau refunded |

## Model dbt

| Model | Jenis | Keterangan |
|---|---|---|
| `stg_orders` | view | merapikan nama dan tipe kolom, menurunkan `order_date` |
| `fct_orders` | tabel | satu baris per `order_id` |
| `dim_customer` | tabel | `customer_id`, `first_order_ts`, `total_orders` |
| `dim_product_category` | tabel | daftar kategori produk |
| `mart_daily_sales` | tabel | penjualan bersih per tanggal, kategori, metode bayar; hanya status `paid` dan `shipped` yang diakui sebagai penjualan |

Tes (9): `unique` dan `not_null` pada kunci, serta `relationships` dari `fct_orders` ke kedua dimensi.

## Hasil Utama

| Tahap | Hasil |
|---|---|
| Batch | 36 dibaca, 30 valid, 6 ditolak (O1031–O1036), run kedua menambah 0 record |
| Streaming | 36 pesan terbaca (offset partisi 15 + 5 + 16, lag 0): 30 valid, 6 rejected |
| Transformasi | 30 record; `gross_amount` dan `net_amount` cocok dengan perhitungan manual |
| dbt | `dbt run` 5 model berhasil, `dbt test` 9 tes lulus |
| Penjualan bersih (paid + shipped) | 3.412,023; puncak harian 2026-09-06 sebesar 1.022,600 |
| Kategori tertinggi | Electronics 1.605,115 |
| Metode bayar terbanyak | card (14 pesanan) |

Benchmark (600.000 baris, Windows 11, 4 core logis, Python 3.12.10):

| Skenario | Grup terbesar | Pandas (s) | Polars eager (s) | Polars lazy (s) |
|---|---|---|---|---|
| normal | 80.000 (13%) | 0,201 | 0,194 | 0,154 |
| skew | 496.411 (83%) | 0,287 | 0,249 | 0,133 |

Selisih absolut kecil dan hanya diukur pada satu mesin, sehingga tidak disimpulkan sebagai efek skew semata. Tidak ada klaim kehabisan memori.

## Jaminan dan Keterbatasan

- **Pengiriman Kafka:** at-least-once (offset di-commit manual setelah sink menulis). Duplikasi bisnis dicegah di sink dengan melewati `order_id` yang sudah tersimpan. Pipeline ini **tidak** mengklaim exactly-once end-to-end.
- **Idempotensi batch:** watermark `order_ts` disimpan setelah Parquet sukses, ditambah `drop_duplicates("order_id")`. Idempotensi warehouse memakai `PRIMARY KEY` dan `INSERT OR REPLACE`.
- Sumber "API" hanyalah CSV lokal yang dipaginasi; retry baru menangani error simulasi, belum HTTP 429/5xx atau `Retry-After`.
- Watermark berbasis timestamp dapat melewatkan data terlambat, dan baris dengan `order_ts` tidak valid tidak lolos filter watermark pada run berikutnya.
- Sink rejected di streaming belum dideduplikasi (pesan cacat yang diputar ulang tercatat dua kali).
- `fct_orders` masih full-refresh (belum model incremental); dimensi masih sederhana.
- Benchmark hanya satu mesin, satu jenis operasi, dan data hasil penggandaan.

## Pemecahan Masalah

| Masalah | Solusi |
|---|---|
| Instalasi `dbt-duckdb` gagal di Windows | Batas panjang path; pindahkan proyek ke folder dengan path pendek (hindari OneDrive berpath panjang) |
| `transform.py` error pada konversi `order_ts` | Polars versi baru butuh `time_zone` eksplisit (sudah diatur di skrip) |
| dbt docs tidak terbuka di `localhost` | Gunakan `http://127.0.0.1:8080` |
| dbt gagal membuka database | Pastikan `load_duckdb.py` sudah selesai (koneksi ditutup) |
| `benchmark.py` error `non-existent directory` | Buat folder `data/bench` terlebih dahulu |