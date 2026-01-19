#!/usr/bin/env python3
"""
ETL Loader - Loads JSONL files and ZIP archives into SQLite database
Enables cross-referencing and complex queries across all data sources
"""

import json
import sqlite3
import sys
import zipfile
import csv
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import re

DATA_RAW_DIR = Path("data/raw")
DATA_DB_DIR = Path("data")
DB_PATH = DATA_DB_DIR / "fraud_data.db"

# Normalize names for cross-referencing
def normalize_name(name: str) -> Optional[str]:
    """Normalize a name for matching (lowercase, remove extra spaces, handle common variations)."""
    if not name or not isinstance(name, str):
        return None
    # Remove extra whitespace, lowercase, remove common prefixes/suffixes
    normalized = re.sub(r'\s+', ' ', name.strip().lower())
    # Remove common prefixes
    normalized = re.sub(r'^(dr\.?|professor|prof\.?|mr\.?|mrs\.?|ms\.?)\s+', '', normalized)
    return normalized if normalized else None


def create_schema(conn: sqlite3.Connection):
    """Create database schema for all data sources."""
    cursor = conn.cursor()
    
    # PubMed articles
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pubmed_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pmid TEXT UNIQUE,
            title TEXT,
            authors TEXT,
            journal TEXT,
            abstract TEXT,
            doi TEXT,
            scraped_at TEXT,
            source TEXT,
            filename TEXT,
            text_content TEXT
        )
    """)
    
    # Retractions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS retractions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doi TEXT UNIQUE,
            title TEXT,
            journal TEXT,
            authors TEXT,
            scraped_at TEXT,
            source TEXT,
            filename TEXT,
            text_content TEXT
        )
    """)
    
    # FDA FAERS (Adverse Events)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fda_faers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id TEXT UNIQUE,
            drug TEXT,
            reaction TEXT,
            patient_age TEXT,
            patient_sex TEXT,
            scraped_at TEXT,
            source TEXT,
            filename TEXT,
            text_content TEXT
        )
    """)
    
    # NIH Grants
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nih_grants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_num TEXT UNIQUE,
            pi_name TEXT,
            pi_name_normalized TEXT,
            org_name TEXT,
            total_cost REAL,
            fiscal_year INTEGER,
            scraped_at TEXT,
            source TEXT,
            filename TEXT,
            text_content TEXT
        )
    """)
    
    # Clinical Trials
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clinical_trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nct_id TEXT UNIQUE,
            title TEXT,
            status TEXT,
            principal_investigator TEXT,
            pi_name_normalized TEXT,
            scraped_at TEXT,
            source TEXT,
            filename TEXT,
            text_content TEXT
        )
    """)
    
    # CMS OpenPayments (from ZIP)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cms_openpayments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id TEXT UNIQUE,
            physician_first_name TEXT,
            physician_last_name TEXT,
            physician_name_normalized TEXT,
            recipient_name TEXT,
            recipient_name_normalized TEXT,
            payment_amount REAL,
            payment_date TEXT,
            nature_of_payment TEXT,
            product_category TEXT,
            scraped_at TEXT
        )
    """)
    
    # FDA MAUDE (from ZIP)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fda_maude (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_number TEXT UNIQUE,
            manufacturer_name TEXT,
            device_name TEXT,
            event_type TEXT,
            event_date TEXT,
            patient_problem TEXT,
            scraped_at TEXT
        )
    """)
    
    # PubPeer articles
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pubpeer_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pub_id TEXT UNIQUE,
            title TEXT,
            url TEXT,
            comment_count INTEGER,
            comments TEXT,
            scraped_at TEXT,
            source TEXT,
            filename TEXT,
            text_content TEXT
        )
    """)
    
    # Create indexes for cross-referencing
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_nih_grants_pi_normalized ON nih_grants(pi_name_normalized)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cms_physician_normalized ON cms_openpayments(physician_name_normalized)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cms_recipient_normalized ON cms_openpayments(recipient_name_normalized)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clinical_trials_pi_normalized ON clinical_trials(pi_name_normalized)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_retractions_doi ON retractions(doi)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pubmed_pmid ON pubmed_articles(pmid)")
    
    conn.commit()


def load_jsonl_file(conn: sqlite3.Connection, filepath: Path):
    """Load a JSONL file into the appropriate table based on source name."""
    cursor = conn.cursor()
    filename = filepath.name
    source = filepath.stem.replace("data_", "")
    
    print(f"Loading {filename}...")
    records_loaded = 0
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                record = json.loads(line)
                
                # Route to appropriate table based on source
                if source == "PubMed_US":
                    cursor.execute("""
                        INSERT OR REPLACE INTO pubmed_articles 
                        (pmid, title, abstract, doi, scraped_at, source, filename, text_content)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get("pmid"),
                        record.get("title"),
                        record.get("abstract"),
                        record.get("doi"),
                        record.get("scraped_at"),
                        record.get("source"),
                        record.get("filename"),
                        record.get("text", "")
                    ))
                
                elif source == "RetractionWatch":
                    # Extract authors from text if available
                    authors = None
                    text = record.get("text", "")
                    if "Author" in text or "author" in text:
                        # Try to extract authors from text
                        pass  # Could add regex extraction here
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO retractions 
                        (doi, title, journal, authors, scraped_at, source, filename, text_content)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get("doi"),
                        record.get("title"),
                        record.get("journal"),
                        authors,
                        record.get("scraped_at"),
                        record.get("source"),
                        record.get("filename"),
                        text
                    ))
                
                elif source == "FDA_FAERS":
                    cursor.execute("""
                        INSERT OR REPLACE INTO fda_faers 
                        (report_id, drug, reaction, scraped_at, source, filename, text_content)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get("report_id"),
                        record.get("drug"),
                        record.get("reaction"),
                        record.get("scraped_at"),
                        record.get("source"),
                        record.get("filename"),
                        record.get("text", "")
                    ))
                
                elif source == "NIH_Grants":
                    pi_name = record.get("pi_name", "")
                    pi_normalized = normalize_name(pi_name)
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO nih_grants 
                        (project_num, pi_name, pi_name_normalized, org_name, total_cost, scraped_at, source, filename, text_content)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get("project_num"),
                        pi_name,
                        pi_normalized,
                        record.get("org_name"),
                        record.get("total_cost"),
                        record.get("scraped_at"),
                        record.get("source"),
                        record.get("filename"),
                        record.get("text", "")
                    ))
                
                elif source == "ClinicalTrials":
                    pi_name = record.get("principal_investigator") or record.get("pi_name", "")
                    pi_normalized = normalize_name(pi_name)
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO clinical_trials 
                        (nct_id, title, status, principal_investigator, pi_name_normalized, scraped_at, source, filename, text_content)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get("nct_id"),
                        record.get("title"),
                        record.get("status"),
                        pi_name,
                        pi_normalized,
                        record.get("scraped_at"),
                        record.get("source"),
                        record.get("filename"),
                        record.get("text", "")
                    ))
                
                records_loaded += 1
                if records_loaded % 100 == 0:
                    print(f"  Loaded {records_loaded} records...", end='\r', flush=True)
                    conn.commit()
            
            except json.JSONDecodeError as e:
                print(f"  Warning: Skipping invalid JSON on line {line_num}: {e}", file=sys.stderr)
                continue
            except Exception as e:
                print(f"  Error processing line {line_num}: {e}", file=sys.stderr)
                continue
    
    conn.commit()
    print(f"\n  ✅ Loaded {records_loaded} records from {filename}")


def load_pubpeer_file(conn: sqlite3.Connection, filepath: Path):
    """Load PubPeer JSONL file."""
    cursor = conn.cursor()
    print(f"Loading {filepath.name}...")
    records_loaded = 0
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            try:
                record = json.loads(line)
                cursor.execute("""
                    INSERT OR REPLACE INTO pubpeer_articles 
                    (pub_id, title, url, comment_count, comments, scraped_at, source, filename, text_content)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.get("pub_id"),
                    record.get("title"),
                    record.get("url"),
                    record.get("comment_count", 0),
                    json.dumps(record.get("comments", [])),
                    record.get("scraped_at"),
                    "PubPeer",
                    record.get("filename"),
                    record.get("text", "")
                ))
                records_loaded += 1
            except Exception as e:
                print(f"  Error: {e}", file=sys.stderr)
                continue
    
    conn.commit()
    print(f"  ✅ Loaded {records_loaded} records from {filepath.name}")


def load_cms_zip(conn: sqlite3.Connection, zip_path: Path):
    """Load CMS OpenPayments ZIP file."""
    print(f"Loading {zip_path.name}...")
    cursor = conn.cursor()
    records_loaded = 0
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Find the main CSV file (usually the largest one)
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if not csv_files:
                print(f"  ⚠️  No CSV files found in {zip_path.name}")
                return
            
            # Use the largest CSV file (usually the main data file)
            main_csv = max(csv_files, key=lambda f: zip_ref.getinfo(f).file_size)
            print(f"  Extracting {main_csv}...")
            
            with zip_ref.open(main_csv) as csv_file:
                # Read CSV (handle large files)
                reader = csv.DictReader(csv_file.read().decode('utf-8', errors='ignore').splitlines())
                
                for row in reader:
                    # Normalize physician name
                    first_name = row.get('Physician_First_Name', '')
                    last_name = row.get('Physician_Last_Name', '')
                    physician_name = f"{first_name} {last_name}".strip()
                    physician_normalized = normalize_name(physician_name)
                    
                    # Normalize recipient name
                    recipient_name = row.get('Recipient_Name', '')
                    recipient_normalized = normalize_name(recipient_name)
                    
                    # Parse payment amount
                    payment_amount = None
                    try:
                        payment_str = row.get('Total_Amount_of_Payment_USDollars', '0')
                        payment_amount = float(payment_str.replace(',', '')) if payment_str else 0
                    except (ValueError, AttributeError):
                        pass
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO cms_openpayments 
                        (record_id, physician_first_name, physician_last_name, physician_name_normalized,
                         recipient_name, recipient_name_normalized, payment_amount, payment_date,
                         nature_of_payment, product_category, scraped_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row.get('Record_ID'),
                        first_name,
                        last_name,
                        physician_normalized,
                        recipient_name,
                        recipient_normalized,
                        payment_amount,
                        row.get('Date_of_Payment'),
                        row.get('Nature_of_Payment_or_Transfer_of_Value'),
                        row.get('Product_Category_or_Therapeutic_Area_1'),
                        datetime.now().isoformat()
                    ))
                    
                    records_loaded += 1
                    if records_loaded % 1000 == 0:
                        print(f"  Loaded {records_loaded} records...", end='\r', flush=True)
                        conn.commit()
    
    except Exception as e:
        print(f"  ❌ Error loading {zip_path.name}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return
    
    conn.commit()
    print(f"\n  ✅ Loaded {records_loaded} records from {zip_path.name}")


def main():
    from datetime import datetime
    
    DATA_DB_DIR.mkdir(parents=True, exist_ok=True)
    
    print("ETL Loader - Loading data into SQLite database")
    print(f"Database: {DB_PATH}")
    print()
    
    # Create/connect to database
    conn = sqlite3.connect(DB_PATH)
    create_schema(conn)
    
    # Load JSONL files
    jsonl_files = sorted(DATA_RAW_DIR.glob("*.jsonl"))
    for jsonl_file in jsonl_files:
        if "pubpeer" in jsonl_file.name.lower():
            load_pubpeer_file(conn, jsonl_file)
        elif jsonl_file.name.startswith("data_"):
            load_jsonl_file(conn, jsonl_file)
    
    # Load ZIP files
    zip_files = sorted(DATA_RAW_DIR.glob("*.zip"))
    for zip_file in zip_files:
        if "CMS" in zip_file.name or "OpenPayments" in zip_file.name:
            load_cms_zip(conn, zip_file)
        elif "MAUDE" in zip_file.name:
            # Similar function for MAUDE (implement if needed)
            print(f"  ⚠️  MAUDE ZIP loading not yet implemented: {zip_file.name}")
    
    # Print summary
    cursor = conn.cursor()
    print("\n" + "="*60)
    print("DATABASE SUMMARY")
    print("="*60)
    
    tables = [
        "pubmed_articles", "retractions", "fda_faers", "nih_grants",
        "clinical_trials", "cms_openpayments", "fda_maude", "pubpeer_articles"
    ]
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"  {table:25s}: {count:>10,} records")
    
    print("="*60)
    print(f"\n✅ ETL Complete! Database saved to: {DB_PATH}")
    print("\nNext: Run cross-reference queries to find red flags")
    
    conn.close()


if __name__ == "__main__":
    main()
