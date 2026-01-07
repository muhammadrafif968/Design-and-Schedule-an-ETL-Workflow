

| ‼️Catatan: Diharapkan seluruh pengerjaan Assignment tidak sepenuhnya mengandalkan penggunaan AI‼️  *“Proses belajar ibarat menanam pohon. Jika hanya mengandalkan AI tanpa memahami esensinya, yang berkembang bukan kompetensimu, melainkan ketergantungan yang melemahkan.” \- Learning Design Dibimbing* |
| :---: |

#  **Assignment Guidance: Design and Schedule an ETL Workflow**

**Data Engineer Bootcamp**

# **Periode Pembelajaran**

Docker Bash  
Introduction to Workflow Orchestration with Airflow  
Designing and Scheduling ETL Workflows

# **Objectives**

Setelah menyelesaikan assignment ini, Anda diharapkan dapat:

1. **Memahami Konsep ETL Pipeline**  
- Mengerti proses Extract, Transform, dan Load dalam konteks data engineering  
- Mampu mengidentifikasi kebutuhan bisnis dan menerjemahkannya ke dalam workflow ETL  
- Memahami pentingnya data pipeline automation dalam lingkungan production  
2. **Menguasai Apache Airflow**  
- Mampu membuat dan mengkonfigurasi DAG (Directed Acyclic Graph)  
- Memahami konsep task dependencies dan workflow orchestration  
- Menggunakan XCom untuk komunikasi antar tasks  
- Mengimplementasikan retry mechanism dan error handling  
3. **Integrasi Multi-Database**  
- Menguasai penggunaan PostgresHook untuk koneksi PostgreSQL  
- Menguasai penggunaan MySqlHook untuk koneksi MySQL  
- Memahami perbedaan antara database operasional (OLTP) dan data warehouse (OLAP)  
- Mampu melakukan operasi CRUD dan UPSERT di berbagai database  
4. **Data Transformation**  
- Mengimplementasikan business logic dalam transformasi data  
- Melakukan data cleaning dan standardisasi  
- Menghitung derived metrics (seperti profit margin)  
- Melakukan validasi data dan handling data quality issues  
5. **Best Practices Data Engineering**  
-  Menulis kode yang bersih, maintainable, dan well-documented  
- Implementasi proper logging untuk monitoring dan debugging  
- Error handling dan recovery mechanisms  
- Security practices (tidak hardcode credentials)

# **Deskripsi Assignment**

**Studi Kasus Bisnis**  
**Perusahaan:** RetailCorp (E-commerce)

**Masalah:** Tim bisnis membutuhkan data dari database operasional PostgreSQL ke warehouse MySQL untuk keperluan pelaporan.

**Tugas Anda:** Membangun DAG Airflow yang memindahkan dan mentransformasi data antar database ini.

**Output yang Diharapkan**  
Pada akhir assignment, Anda akan menghasilkan:

- File DAG Python yang fully functional dan production-ready  
- ETL Pipeline yang berjalan otomatis setiap 6 jam  
- Data Transformation yang memenuhi business requirements  
- Documentation yang jelas melalui docstrings dan comments  
- Error Handling yang robust untuk mengatasi kegagalan

**Skenario Real-World**  
Assignment ini mensimulasikan skenario nyata di industri di mana:

- Data Source: Database operasional PostgreSQL yang terus menerima transaksi baru  
- Data Target: Data warehouse MySQL untuk analytics dan reporting  
- Requirement: Data harus di-sync setiap 6 jam untuk laporan real-time  
- Challenge: Menangani data yang terus berubah (incremental load)  
- Solution: Automated ETL pipeline dengan Airflow

**Skills yang Akan Dipraktikkan**  
Technical Skills:

- Python programming untuk data engineering  
- SQL queries (SELECT, JOIN, INSERT, UPDATE)  
- Apache Airflow (DAG, Operators, Hooks, XCom)  
- Database connectivity dan transactions  
- Data transformation logic

Soft Skills:

- Problem-solving dalam konteks data engineering  
- Attention to detail dalam transformasi data  
- Documentation dan code readability  
- Debugging dan troubleshooting

# **Detail Assignment**

Lakukan langkah-langkah berikut menyelesaikan assignment ini

1. ### **Konfigurasi DAG (20 Poin)**

Buat file DAG: /airflow/dags/postgres\_to\_mysql\_etl.py

Persyaratan:

* Nama DAG: postgres\_to\_mysql\_etl  
*  Jadwal: Setiap 6 jam  
* Owner: data-engineering-team  
* Retries: 2 kali dengan jeda 5 menit  
* Catchup: Dinonaktifkan

Tags: \['etl', 'postgresql', 'mysql', 'data-pipeline'\]

Contoh:

| from datetime import timedelta from airflow import DAG from airflow.utils.dates import days\_ago default\_args \= { 	'owner': 'data-engineering-team', 	'retries': 2, 	'retry\_delay': timedelta(minutes=5), } dag \= DAG( 	'postgres\_to\_mysql\_etl', 	default\_args=default\_args, 	schedule\_interval=timedelta(hours=6), 	start\_date=days\_ago(1), 	catchup=False, 	tags=\['etl', 'postgresql', 'mysql', 'data-pipeline'\], ) |
| :---- |

2. ### **Ekstrak Data dari PostgreSQL (30 Poin)**

Buat 3 fungsi ekstraksi menggunakan PythonOperator:

#### **2.1 Ekstrak Customers (10 poin)**

| def extract\_customers\_from\_postgres(\*\*context): 	"""Ekstrak data customer dari PostgreSQL""" 	*\# TODO: Implementasikan ini* 	pass |
| :---- |

**Persyaratan**:

* Gunakan PostgresHook untuk koneksi  
*  SELECT semua field customer dari raw\_data.customers  
*  Filter: WHERE updated\_at \>= CURRENT\_DATE \- INTERVAL '1 day'  
* Konversi ke list of dictionaries  
* Push ke XCom dengan key 'customers\_data'

#### **2.2 Ekstrak Products (10 poin)**

| def extract\_products\_from\_postgres(\*\*context): 	"""Ekstrak data product dari PostgreSQL""" 	*\# TODO: Implementasikan ini* 	pass |
| :---- |

**Persyaratan**:

* Gunakan PostgresHook untuk koneksi  
* SELECT field product dari raw\_data.products  
*  JOIN dengan raw\_data.suppliers untuk mendapatkan supplier\_name  
*  Filter: WHERE updated\_at \>= CURRENT\_DATE \- INTERVAL '1 day'  
* Konversi ke list of dictionaries  
* Push ke XCom dengan key 'products\_data'

#### **2.3 Ekstrak Orders (10 poin)**

| def extract\_orders\_from\_postgres(\*\*context): 	"""Ekstrak data order dari PostgreSQL""" 	*\# TODO: Implementasikan ini* 	pass |
| :---- |

 **Persyaratan**:

* Gunakan PostgresHook untuk koneksi  
* SELECT semua field order dari raw\_data.orders  
*  Filter: WHERE updated\_at \>= CURRENT\_DATE \- INTERVAL '1 day'  
* Konversi ke list of dictionaries  
* Push ke XCom dengan key 'orders\_data'

3. ### **Transform dan Load ke MySQL (30 Poin)**

**Buat 3 fungsi transformasi \+ loading:**

#### **3.1 Transform & Load Customers (10 poin)**

**Transformasi**:

1. **Nomor telepon:** Hapus karakter non-digit, format sebagai (XXX) XXX-XXXX  
2. **Kode negara bagian:** Konversi ke UPPERCASE

**Loading**:

-  Gunakan MySqlHook  
- UPSERT ke dim\_customers (INSERT ... ON DUPLICATE KEY UPDATE)

| def transform\_and\_load\_customers(\*\*context): 	"""Transform data customer dan load ke MySQL""" 	*\# Pull dari XCom* 	customers \= context\['task\_instance'\].xcom\_pull(     	task\_ids='extract\_customers',     	key='customers\_data' 	) 	 	*\# Transform* 	for customer in customers:     	*\# Bersihkan phone: (XXX) XXX-XXXX*     	*\# Uppercase state*     	pass 	 	*\# Load ke MySQL dengan UPSERT* 	pass |
| :---- |

#### **3.2 Transform & Load Products (10 poin)**

**Transformasi**:

1. Margin keuntungan: Hitung ((price \- cost) / price) \* 100  
2. Kategori: Konversi ke Title Case

**Loading**:

- Gunakan MySqlHook  
- UPSERT ke dim\_products

| def transform\_and\_load\_products(\*\*context): 	"""Transform data product dan load ke MySQL""" 	*\# TODO: Implementasikan ini* 	pass |
| :---- |

#### 

#### 

#### **3.3 Transform & Load Orders (10  poin)**

**Transformasi**:

1. Status order: Konversi ke lowercase  
2. Total amount: Validasi positif (jika negatif, set ke 0 dan log warning)

**Loading**:

-  Gunakan MySqlHook  
- UPSERT ke fact\_orders

| def transform\_and\_load\_orders(\*\*context): 	"""Transform data order dan load ke MySQL""" 	*\# TODO: Implementasikan ini* 	pass |
| :---- |

4. ### **Dependensi Task (10 Poin)**

Definisikan alur kerja dengan menghubungkan tasks:

| *\# Definisikan objek task terlebih dahulu (menggunakan PythonOperator)* extract\_customers \= PythonOperator( 	task\_id='extract\_customers',     python\_callable=extract\_customers\_from\_postgres, 	dag=dag, ) *\# ... definisikan task lainnya ...* *\# Set dependencies* *\# TODO: Implementasikan ini* |
| :---- |

5. ### **Kualitas Kode (10 Poin)**

- Kode yang bersih (sesuai PEP 8\)  
- Tidak ada kredensial yang di-hardcode  
- Error handling dengan try-except  
- Docstrings untuk semua fungsi  
- Logging yang tepat di seluruh kode  
- Tutup koneksi database dengan benar

# **Tools**

Visual Studio Code, Docker, Airflow

# **Pengumpulan Assignment**

**Deadline :**   
Maksimal H+7 Kelas (Pukul 23.30 WIB)  
**Details :**  
Dikumpulkan dalam bentuk ZIP, secara INDIVIDU, di LMS

# **Indikator Penilaian**

| No | Aspek Penilaian | Parameter | Bobot Maksimal |
| :---: | :---: | :---: | :---: |
| 1 | Konfigurasi DAG | DAG setup, default\_args, schedule, tags | 20 |
| 2 | Extract Data | 3 fungsi ekstraksi (Customers, Products, Orders) | 30 |
| 3 | Transform & Load | 3 fungsi transformasi dan loading | 30 |
| 4 | Dependency | Task relationships dan workflow | 10 |
| 5 | Kualitas Kode | Clean code, error handling, logging | 10 |
| Total |  |  | 100 |

**Ketentuan Pencapaian Nilai:**  
Nilai minimum Lulus Penyaluran Kerja: 75  
Nilai minimum Lulus Bootcamp: 65

**Ketentuan Penilaian:**  
Mengumpulkan Assignment tepat waktu: Sesuai dengan nilai yang diberikan mentor  
Mengumpulkan Assignment 12 jam setelah deadline: \- 3 dari nilai yang diberikan mentor  
Mengumpulkan Assignment 1 x 24 Jam setelah deadline: \- 6 dari nilai yang diberikan mentor  
Mengumpulkan Assignment 2 x 24 Jam setelah deadline: \- 12 dari nilai yang diberikan mentor	Mengumpulkan Assignment 3 x 24 Jam setelah deadline: \- 18 dari nilai yang diberikan mentor  
Mengumpulkan Assignment 4 x 24 Jam setelah deadline: \- 24 dari nilai yang diberikan mentor	Mengumpulkan Assignment 5 x 24 Jam setelah deadline: \- 30 dari nilai yang diberikan mentor  
Mengumpulkan Assignment 6 x 24 Jam setelah deadline: \- 36 dari nilai yang diberikan mentor	Mengumpulkan Assignment 7 x 24 Jam setelah deadline: \- 42 dari nilai yang diberikan mentor