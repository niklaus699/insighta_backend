import json
import os
import random
import sqlite3
import statistics
import string
import tempfile
import time
from pathlib import Path


DATASET_SIZE = 100_000
INGEST_SIZE = 50_000
PAGE_LIMIT = 25
QUERY_RUNS = 40
CHUNK_SIZE = 2_000


def random_name(index):
    suffix = "".join(random.choices(string.ascii_lowercase, k=8))
    return f"user_{index}_{suffix}"


def seed_database(connection):
    connection.execute(
        """
        CREATE TABLE profile (
            id TEXT PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            gender TEXT,
            gender_probability REAL,
            sample_size INTEGER,
            age INTEGER,
            age_group TEXT,
            country_id TEXT,
            country_name TEXT,
            country_probability REAL,
            created_at TEXT
        )
        """
    )

    rows = []
    for index in range(DATASET_SIZE):
        gender = "female" if index % 2 == 0 else "male"
        age = 18 + (index % 50)
        age_group = "adult" if age >= 20 else "teenager"
        country_id = "NG" if index % 3 == 0 else "KE"
        rows.append(
            (
                f"id-{index}",
                random_name(index),
                gender,
                0.95,
                1000 + index,
                age,
                age_group,
                country_id,
                "nigeria" if country_id == "NG" else "kenya",
                0.92,
                f"2026-01-{(index % 28) + 1:02d}T00:00:00Z",
            )
        )

    connection.executemany(
        """
        INSERT INTO profile (
            id, name, gender, gender_probability, sample_size, age, age_group,
            country_id, country_name, country_probability, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    connection.commit()


def measure_query(connection):
    durations = []
    sql = """
        SELECT id, name, gender, age, age_group, country_id, created_at
        FROM profile
        WHERE country_id = ? AND gender = ? AND age BETWEEN ? AND ?
        ORDER BY created_at ASC
        LIMIT ? OFFSET ?
    """
    params = ("NG", "female", 20, 45, PAGE_LIMIT, 0)

    for _ in range(QUERY_RUNS):
        started = time.perf_counter()
        connection.execute(sql, params).fetchall()
        connection.execute(
            """
            SELECT COUNT(*)
            FROM profile
            WHERE country_id = ? AND gender = ? AND age BETWEEN ? AND ?
            """,
            params[:4],
        ).fetchone()
        durations.append((time.perf_counter() - started) * 1000)

    return round(statistics.mean(durations), 2)


def measure_cache_hit():
    payload = {"filters": {"country_id": "NG", "gender": "female", "min_age": 20, "max_age": 45}}
    cache = {"profiles:search": json.dumps(payload)}
    durations = []
    for _ in range(QUERY_RUNS):
        started = time.perf_counter()
        json.loads(cache["profiles:search"])
        durations.append((time.perf_counter() - started) * 1000)
    return round(statistics.mean(durations), 4)


def measure_row_by_row_insert(connection):
    started = time.perf_counter()
    for index in range(INGEST_SIZE):
        connection.execute(
            """
            INSERT INTO profile (
                id, name, gender, gender_probability, sample_size, age, age_group,
                country_id, country_name, country_probability, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"ingest-row-{index}",
                f"bulk_name_{index}",
                "female",
                0.9,
                10,
                30,
                "adult",
                "NG",
                "nigeria",
                0.8,
                "2026-05-01T00:00:00Z",
            ),
        )
    connection.commit()
    return round((time.perf_counter() - started) * 1000, 2)


def measure_chunked_insert(connection):
    started = time.perf_counter()
    rows = []
    for index in range(INGEST_SIZE):
        rows.append(
            (
                f"ingest-batch-{index}",
                f"chunk_name_{index}",
                "female",
                0.9,
                10,
                30,
                "adult",
                "NG",
                "nigeria",
                0.8,
                "2026-05-01T00:00:00Z",
            )
        )
        if len(rows) >= CHUNK_SIZE:
            connection.executemany(
                """
                INSERT INTO profile (
                    id, name, gender, gender_probability, sample_size, age, age_group,
                    country_id, country_name, country_probability, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            connection.commit()
            rows = []

    if rows:
        connection.executemany(
            """
            INSERT INTO profile (
                id, name, gender, gender_probability, sample_size, age, age_group,
                country_id, country_name, country_probability, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        connection.commit()

    return round((time.perf_counter() - started) * 1000, 2)


def main():
    db_path = Path(tempfile.gettempdir()) / "insighta_benchmark.sqlite3"
    if db_path.exists():
        db_path.unlink()

    connection = sqlite3.connect(db_path)
    seed_database(connection)

    before_query_ms = measure_query(connection)
    connection.execute(
        "CREATE INDEX ix_profiles_country_gender_age ON profile (country_id, gender, age)"
    )
    connection.execute("CREATE INDEX ix_profiles_created_at_id ON profile (created_at, id)")
    connection.commit()
    after_query_ms = measure_query(connection)

    row_by_row_ms = measure_row_by_row_insert(connection)
    connection.execute("DELETE FROM profile WHERE id LIKE 'ingest-row-%'")
    connection.commit()
    chunked_ms = measure_chunked_insert(connection)

    print(
        json.dumps(
            {
                "dataset_size": DATASET_SIZE,
                "query_before_ms": before_query_ms,
                "query_after_index_ms": after_query_ms,
                "query_after_cache_hit_ms": measure_cache_hit(),
                "row_by_row_insert_ms": row_by_row_ms,
                "chunked_insert_ms": chunked_ms,
            },
            indent=2,
        )
    )

    connection.close()
    if db_path.exists():
        os.unlink(db_path)


if __name__ == "__main__":
    main()
