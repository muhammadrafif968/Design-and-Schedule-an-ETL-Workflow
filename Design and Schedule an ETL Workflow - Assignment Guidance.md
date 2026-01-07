| ‼️ Catatan: Diharapkan seluruh pengerjaan Assignment tidak sepenuhnya mengandalkan penggunaan AI‼️  
*“Proses belajar ibarat menanam pohon. Jika hanya mengandalkan AI tanpa memahami esensinya, yang berkembang bukan kompetensimu, melainkan ketergantungan yang melemahkan.” – Learning Design Dibimbing* |
| :---: |

# **Assignment Guidance: Design and Schedule an ETL Workflow**

**Data Engineer Bootcamp**

---

## **Periode Pembelajaran**

- Docker Bash  
- Introduction to Workflow Orchestration with Airflow  
- Designing and Scheduling ETL Workflows

---

## **Objectives**

Setelah menyelesaikan assignment ini, Anda diharapkan dapat:

### 1. Memahami Konsep ETL Pipeline
- Memahami proses Extract, Transform, dan Load dalam konteks data engineering  
- Mampu menerjemahkan kebutuhan bisnis ke dalam workflow ETL  
- Memahami pentingnya automasi data pipeline di environment production

### 2. Menguasai Apache Airflow
- Membuat dan mengonfigurasi DAG (Directed Acyclic Graph)  
- Memahami task dependency dan workflow orchestration  
- Menggunakan XCom untuk komunikasi antar task  
- Mengimplementasikan retry mechanism dan error handling

### 3. Integrasi Multi-Database
- Menggunakan PostgresHook untuk PostgreSQL  
- Menggunakan MySqlHook untuk MySQL  
- Memahami perbedaan OLTP vs OLAP  
- Melakukan operasi CRUD dan UPSERT

### 4. Data Transformation
- Implementasi business logic  
- Data cleaning dan standardisasi  
- Perhitungan derived metrics (contoh: margin)  
- Validasi data dan data quality handling

### 5. Best Practices Data Engineering
- Clean & maintainable code  
- Proper logging dan monitoring  
- Error handling dan recovery  
- Security (tanpa hardcode credentials)

---

## **Deskripsi Assignment**

### Studi Kasus Bisnis
- **Perusahaan**: RetailCorp (E-commerce)
- **Masalah**: Data operasional di PostgreSQL perlu dipindahkan ke MySQL untuk reporting
- **Tugas**: Membangun DAG Airflow untuk ETL antar database

### Output yang Diharapkan
- File DAG Python yang production-ready  
- Pipeline otomatis berjalan setiap 6 jam  
- Transformasi sesuai business rules  
- Dokumentasi jelas (docstring & comment)  
- Error handling yang robust

---

## **Detail Assignment**

### 1. Konfigurasi DAG (20 Poin)

Lokasi file:
```
/airflow/dags/postgres_to_mysql_etl.py
```

**Persyaratan**:
- DAG name: `postgres_to_mysql_etl`  
- Schedule: setiap 6 jam  
- Owner: `data-engineering-team`  
- Retries: 2 (delay 5 menit)  
- Catchup: False  
- Tags: `['etl', 'postgresql', 'mysql', 'data-pipeline']`

**Contoh**:
```python
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data-engineering-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='postgres_to_mysql_etl',
    default_args=default_args,
    schedule_interval=timedelta(hours=6),
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'postgresql', 'mysql', 'data-pipeline'],
)
```

---

### 2. Extract Data dari PostgreSQL (30 Poin)

#### 2.1 Extract Customers (10 poin)
```python
def extract_customers_from_postgres(**context):
    """Ekstrak data customer dari PostgreSQL"""
    pass
```

#### 2.2 Extract Products (10 poin)
```python
def extract_products_from_postgres(**context):
    """Ekstrak data product dari PostgreSQL"""
    pass
```

#### 2.3 Extract Orders (10 poin)
```python
def extract_orders_from_postgres(**context):
    """Ekstrak data order dari PostgreSQL"""
    pass
```

---

### 3. Transform & Load ke MySQL (30 Poin)

#### 3.1 Transform & Load Customers (10 poin)
```python
def transform_and_load_customers(**context):
    """Transform data customer dan load ke MySQL"""
    pass
```

#### 3.2 Transform & Load Products (10 poin)
```python
def transform_and_load_products(**context):
    """Transform data product dan load ke MySQL"""
    pass
```

#### 3.3 Transform & Load Orders (10 poin)
```python
def transform_and_load_orders(**context):
    """Transform data order dan load ke MySQL"""
    pass
```

---

### 4. Dependency Task (10 Poin)
```python
extract_customers >> transform_and_load_customers
extract_products >> transform_and_load_products
extract_orders >> transform_and_load_orders
```

---

### 5. Kualitas Kode (10 Poin)
- Clean code (PEP8)  
- Tidak hardcode credential  
- Try-except & logging  
- Docstring di setiap fungsi  
- Tutup koneksi DB dengan benar

---

## **Tools**
- Visual Studio Code
- Docker
- Apache Airflow

---

## **Pengumpulan Assignment**
- **Deadline**: H+7 kelas (23.30 WIB)  
- **Format**: ZIP (Individual)  
- **Platform**: LMS

---

## **Indikator Penilaian**

| Aspek | Bobot |
|------|------|
| Konfigurasi DAG | 20 |
| Extract Data | 30 |
| Transform & Load | 30 |
| Dependency | 10 |
| Kualitas Kode | 10 |
| **Total** | **100** |

