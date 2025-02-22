import pytest
import psycopg2
import subprocess
import sys
from config import DB_CONFIG
from database import insert_companies

def get_db_connection():
    """Establish database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def test_database_connection():
    """Test if the database connection is successful."""
    conn = get_db_connection()
    assert conn is not None, "❌ Database connection failed!"
    conn.close()
    print("✅ Database connection successful.")

def test_schema_exists():
    """Test if schema 'Ginkgo' and table 'csr_reports' exist."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'Ginkgo';")
    schema_exists = cursor.fetchone()

    cursor.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'Ginkgo' AND table_name = 'csr_reports';
    """)
    table_exists = cursor.fetchone()

    cursor.close()
    conn.close()

    assert schema_exists, "❌ Schema 'Ginkgo' does not exist."
    assert table_exists, "❌ Table 'csr_reports' does not exist."
    print("✅ Schema and table exist.")

def test_data_insertion():
    """Test if data is successfully inserted into Ginkgo.csr_reports."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Run data insertion
    insert_companies()

    cursor.execute("SELECT COUNT(*) FROM Ginkgo.csr_reports;")
    count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    assert count > 0, "❌ No data inserted into Ginkgo.csr_reports."
    print(f"✅ Data insertion successful. {count} records found.")

def test_primary_key_constraint():
    """Test if duplicate insertions are prevented by ON CONFLICT DO NOTHING."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Count before re-insertion
    cursor.execute("SELECT COUNT(*) FROM Ginkgo.csr_reports;")
    before_count = cursor.fetchone()[0]

    # Run insert again (should not create duplicates)
    insert_companies()

    # Count after re-insertion
    cursor.execute("SELECT COUNT(*) FROM Ginkgo.csr_reports;")
    after_count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    assert before_count == after_count, "❌ Duplicate records were inserted!"
    print("✅ Primary key constraint working correctly. No duplicates inserted.")

def test_code_quality():
    """Run linting, formatting, and security scans."""
    python_exec = sys.executable  # Get the correct Python path

    print("🔍 Running flake8 linting...")
    subprocess.run([python_exec, "-m", "flake8", "--max-line-length=100"], check=True)

    print("🔍 Checking code formatting with black...")
    subprocess.run([python_exec, "-m", "black", "--check", "."], check=True)

    print("🔍 Sorting imports with isort...")
    subprocess.run([python_exec, "-m", "isort", "--check-only", "."], check=True)

    print("🔍 Running security scans with Bandit...")
    subprocess.run([python_exec, "-m", "bandit", "-r", "."], check=True)

if __name__ == "__main__":
    test_database_connection()
    test_schema_exists()
    test_data_insertion()
    test_primary_key_constraint()
    test_code_quality()
