# Postgres to MySQL ETL Pipeline (Airflow)

## 📌 Overview
Project ini merupakan implementasi **ETL (Extract, Transform, Load)** menggunakan **Apache Airflow**.

Pipeline ini melakukan:
- Extract data dari **PostgreSQL**
- Transform data sesuai business rules
- Load data ke **MySQL** dengan mekanisme **UPSERT**

Project ini dibuat sebagai bagian dari assignment **“Design and Schedule an ETL Workflow”**.

---

## 🏗️ Architecture

**Source**: PostgreSQL  
**Orchestrator**: Apache Airflow  
**Target**: MySQL (Data Warehouse)

Flow:
```
PostgreSQL
   ├── customers  ──▶ dim_customers
   ├── products   ──▶ dim_products
   └── orders     ──▶ fact_orders
```

Setiap entity diproses secara **independen dan paralel**:
- Extract → Transform → Load

---

## 🗂️ DAG Structure

DAG Name: `postgres_to_mysql_etl`  
Schedule: **Setiap 6 jam**  
Catchup: `False`

Task flow:
```
extract_customers  ──▶ transform_and_load_customers
extract_products   ──▶ transform_and_load_products
extract_orders     ──▶ transform_and_load_orders
```

---

## 🔄 ETL Details

### 1️⃣ Extract (PostgreSQL)
Data diekstrak dari schema `raw_data` dengan filter incremental:
- `updated_at >= CURRENT_DATE - INTERVAL '1 day'`

Tables:
- `raw_data.customers`
- `raw_data.products` (join `suppliers`)
- `raw_data.orders`

Data hasil extract disimpan sementara menggunakan **XCom**.

---

### 2️⃣ Transform & Load

#### 🧑 Customers → `dim_customers`
Transformasi:
- Phone number → format `(XXX) XXX-XXXX`
- State code → uppercase

Load:
- MySQL UPSERT (`INSERT ... ON DUPLICATE KEY UPDATE`)

---

#### 📦 Products → `dim_products`
Transformasi:
- Margin (%) = `((price - cost) / price) * 100`
- Category → Title Case

Load:
- MySQL UPSERT

---

#### 🧾 Orders → `fact_orders`
Transformasi:
- Order status → lowercase
- `total_amount` negatif → diset ke 0 + warning log

Load:
- MySQL UPSERT

---

## ⚙️ Airflow Connections
Pastikan connection berikut sudah tersedia di Airflow:

| Connection ID | Type        |
|--------------|-------------|
| postgres_default | PostgreSQL |
| mysql_default    | MySQL      |

---

## 🛠️ Tech Stack
- Apache Airflow
- PostgreSQL
- MySQL
- Python
- Docker (opsional)

---

## 📌 Notes
- Pipeline dirancang modular dan scalable
- Setiap entity diproses secara independen
- Logging dan error handling sudah disiapkan untuk production-like scenario

---

## 👤 Author
**Muhammad Rafif**  
Data / Analytics Engineer Enthusiast

