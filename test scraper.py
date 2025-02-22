import pytest
import random
import psycopg2
from config import DB_CONFIG
from scraper import google_search_pdf, get_db_connection, get_companies_to_scrape, process_company_data

def test_database_connection():
    """Test if the database connection can be established."""
    conn = get_db_connection()
    assert conn is not None, "❌ Database connection failed! Check DB_CONFIG."
    conn.close()
    print("✅ Database connection successful.")

def test_load_companies():
    """Test if companies with NULL report URLs can be retrieved."""
    companies = get_companies_to_scrape()
    assert isinstance(companies, list), "❌ Query result should be a list."
    assert all(len(c) == 3 for c in companies), "❌ Data format error, expected (symbol, company_name, report_year)."
    
    if not companies:
        print("⚠️ No companies found for scraping. Check if the database is fully updated.")
    else:
        print(f"✅ Successfully loaded {len(companies)} companies for scraping.")

def test_google_search_pdf():
    """Test Google Search API for retrieving CSR report PDFs."""
    companies = get_companies_to_scrape()
    if not companies:
        print("⚠️ No companies available for scraping. Skipping Google Search test.")
        return

    company, year = random.choice(companies)[1], random.randint(2014, 2024)  # Randomly select a company and a year
    url = google_search_pdf(company, year)
    
    assert url is not None, f"❌ Google Search returned no URL for {company} ({year})."
    assert url.endswith(".pdf"), f"❌ Returned URL is not a PDF file: {url}."
    
    print(f"✅ Found PDF report for {company} ({year}): {url}")

def test_database_update():
    """Test if `process_company_data()` correctly updates the database."""
    companies = get_companies_to_scrape()
    if not companies:
        print("⚠️ No companies found for scraping. Skipping database update test.")
        return

    company_data = random.choice(companies)  # Randomly select a company
    symbol, company_name, _ = company_data
    year = random.randint(2014, 2024)  # Randomly select a report year

    print(f"🛠️ Testing database update for {company_name} ({year})...")

    process_company_data((symbol, company_name, year))  # Run scraping

    # Re-query the database to check if the report URL is updated
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT report_url FROM Ginkgo.csr_reports 
        WHERE symbol = %s AND report_year = %s;
    """, (symbol, year))
    updated_url = cursor.fetchone()

    cursor.close()
    conn.close()

    assert updated_url is not None and updated_url[0] is not None, f"❌ {company_name} ({year}) report URL was not updated."
    print(f"✅ Successfully updated PDF report URL for {company_name} ({year}): {updated_url[0]}.")

if __name__ == "__main__":
    test_database_connection()
    test_load_companies()
    test_google_search_pdf()
    test_database_update()
