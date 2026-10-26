# Supporting files — Mini Project Data Processing Workflow

## Isi paket
- `data/orders.csv`: data transaksi sintetis, termasuk beberapa baris invalid untuk menguji data contract.
- `scripts/batch_ingest.py`: contoh awal batch pagination, Parquet, checkpoint watermark, dan deduplikasi.
- `requirements.txt`: dependensi Python awal.
- `docker-compose.yml`: konfigurasi single-node Kafka untuk praktikum.
- `MINI_PROJECT.md`: brief tugas mahasiswa.
- `SOLUTION_STEP_BY_STEP.md`: panduan solusi langkah demi langkah.

## Mulai cepat
1. Buat environment Python dan install `requirements.txt`.
2. Jalankan batch ingestion:
   `python scripts/batch_ingest.py --source data/orders.csv --out data/raw`
3. Jalankan Kafka:
   `docker compose up -d`
4. Lengkapi producer, consumer, validasi Pydantic, benchmark, pemuatan DuckDB, dan model dbt sesuai brief.

## Catatan penting
Skrip batch merupakan kerangka edukatif, bukan pipeline produksi: sumbernya CSV lokal yang dipaginasi, bukan API eksternal. Perluas retry untuk error HTTP, implementasikan validasi kontrak, dan uji incremental loading secara eksplisit. Kafka Compose menyiapkan broker; producer dan consumer tetap harus dibuat/dilengkapi oleh mahasiswa.
