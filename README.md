# Proyek ETL dengan Apache Airflow dan Metabase

Proyek ini adalah implementasi proses ETL (Extract, Transform, Load) menggunakan Apache Airflow untuk mentransfer dan memproses data dari database sumber ke data warehouse. Data yang telah diproses kemudian dianalisis menggunakan Metabase.

## Persyaratan

- Docker
- Docker Compose
- PostgreSQL

## Struktur Proyek

- `dags/`: Direktori ini berisi DAG Airflow yang melakukan proses ETL.
- `scripts/`: Direktori ini berisi skrip untuk mengatur Metabase.
- `requirements.txt`: File ini berisi dependensi Python yang diperlukan.
- `Dockerfile`: File ini mendefinisikan image Docker untuk Airflow.
- `docker-compose.yml`: File ini mendefinisikan layanan Docker untuk Airflow dan Metabase.
- `README.md`: Dokumentasi proyek ini.

## Instalasi

1. **Clone repositori ini:**

    ```bash
    git clone https://github.com/username/repository.git
    cd repository
    ```

2. **Buat file `.env` berdasarkan `.env.example`:**

    ```bash
    cp .env.example .env
    ```

    Sesuaikan variabel-variabel lingkungan dalam `.env` dengan kebutuhan Anda.

3. **Bangun dan jalankan layanan Docker:**

    ```bash
    docker-compose up --build
    ```

4. **Akses layanan:**

    - Airflow Web UI: [http://localhost:8080](http://localhost:8080)
    - Metabase: [http://localhost:3000](http://localhost:3000)
