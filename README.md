# Insighta Labs: Intelligence Query Engine (Stage 2)

This repository contains the advanced backend system for Insighta Labs, designed to provide demographic intelligence through structured filtering and natural language processing.

---

## 🚀 Live API URL
**Base URL:** `https://profile-intelligence-service-production-2118.up.railway.app/` 

---

## 🛠 Features

### 1. Advanced Query Engine
The `GET /api/profiles` endpoint supports complex combinations of the following filters:

* **Demographics:** `gender`, `age_group`
* **Location:** `country_id` (ISO 2-letter codes)
* **Thresholds:** `min_age`, `max_age`, `min_gender_probability`, `min_country_probability`
* **Sorting:** Results can be sorted by `age`, `created_at`, or `gender_probability` in `asc` or `desc` order.
* **Pagination:** Controlled pagination via `page` and `limit` (max 50 per page).

### 2. Natural Language Query (NLQ)
The `GET /api/profiles/search` endpoint allows users to query the database using plain English. 

**How the parser works:**
Our custom rule-based engine tokenizes the input string and matches keywords against predefined mapping logic:
* **Age Mapping:** "young" is dynamically mapped to the 16–24 age range.
* **Group Mapping:** Keywords like "adult" or "teenager" apply filters to the `age_group` field.
* **Comparative Logic:** Uses Regex to detect patterns like "above 30" to apply numerical filters.
* **Geography:** Scans the query for full country names (e.g., "Nigeria") and maps them to their respective ISO codes.

---

## 📊 Database Schema
The system uses PostgreSQL with the following required structure:

| Field | Type | Notes |
| :--- | :--- | :--- |
| **id** | UUID v7 | Primary Key (Time-ordered) |
| **name** | VARCHAR | Unique, full name |
| **gender** | VARCHAR | male / female |
| **age** | INT | Exact age |
| **age_group** | VARCHAR | child, teenager, adult, senior |
| **country_id** | VARCHAR(2) | ISO code (e.g., NG) |
| **created_at** | TIMESTAMP | UTC ISO 8601 |

---

## ⚙️ Technical Setup

### Data Seeding
The database is seeded with 2026 unique records using an idempotent script (`seed.py`). It ensures no duplicate records are created during container restarts.

### Performance & Error Handling
* **Indexing:** Filtered columns are indexed to ensure O(log n) performance.
* **Safety:** The `format_profile` function includes a Null-check for timestamps to prevent `AttributeError` during serialization.
* **Standard Errors:** All errors follow the `{ "status": "error", "message": "..." }` format.

---

## 📦 Requirements
* Flask & Flask-SQLAlchemy
* Psycopg2-binary (PostgreSQL)
* UUID6 (UUID v7)
* Flask-CORS