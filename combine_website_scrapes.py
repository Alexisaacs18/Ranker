#!/usr/bin/env python3
"""
Combine Website Scrapes and PubMed Trending - Combines all website_scrape_*.jsonl and 
pubmed_trending_*.jsonl files into combined_medical_fraud_data.jsonl
"""

import json
import sys
from pathlib import Path

DATA_RAW_DIR = Path("data/raw")
DATA_PROCESSED_DIR = Path("data/processed")
OUTPUT_FILE = DATA_PROCESSED_DIR / "combined_medical_fraud_data.jsonl"

def main():
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    # Find all website scrape files
    website_scrape_files = sorted(DATA_RAW_DIR.glob("website_scrape_*.jsonl"))
    
    # Find all PubMed trending files
    pubmed_trending_files = sorted(DATA_RAW_DIR.glob("pubmed_trending_*.jsonl"))
    
    # Combine both types
    all_files = website_scrape_files + pubmed_trending_files
    
    if not all_files:
        print("No scrape files found in data/raw/")
        print("Expected files matching: website_scrape_*.jsonl or pubmed_trending_*.jsonl")
        sys.exit(1)
    
    print(f"Found {len(all_files)} scrape file(s):")
    if website_scrape_files:
        print(f"  Website scrapes ({len(website_scrape_files)}):")
        for f in website_scrape_files:
            print(f"    - {f.name}")
    if pubmed_trending_files:
        print(f"  PubMed trending ({len(pubmed_trending_files)}):")
        for f in pubmed_trending_files:
            print(f"    - {f.name}")
    
    combined_records = []
    
    # Read all records from all scrape files
    for scrape_file in all_files:
        print(f"\nReading {scrape_file.name}...")
        file_records = 0
        try:
            with open(scrape_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        combined_records.append(record)
                        file_records += 1
                    except json.JSONDecodeError as e:
                        print(f"  Warning: Skipping invalid JSON on line {line_num}: {e}", file=sys.stderr)
                        continue
            print(f"  Added {file_records} records from {scrape_file.name}")
        except Exception as e:
            print(f"  Error reading {scrape_file.name}: {e}", file=sys.stderr)
            continue
    
    if not combined_records:
        print("\n❌ No records found in scrape files")
        sys.exit(1)
    
    # Write combined file
    print(f"\nWriting {len(combined_records)} combined records to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for i, record in enumerate(combined_records, 1):
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
            if i % 100 == 0:
                print(f"  Written {i}/{len(combined_records)}...", end='\r', flush=True)
    
    print(f"\n✅ Combined {len(combined_records)} records from {len(all_files)} file(s)")
    print(f"   Output: {OUTPUT_FILE}")
    if OUTPUT_FILE.exists():
        size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
        print(f"   File size: {size_mb:.2f} MB")
    print(f"\nNext step: Run converter to process this file")


if __name__ == "__main__":
    main()
