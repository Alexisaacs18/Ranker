#!/usr/bin/env python3
"""
Medical Fraud Data Scraper - Comprehensive Version
Pulls data from FDA, CMS, DOJ, SEC, and other sources

INSTALLATION:
pip install requests beautifulsoup4 pandas lxml --break-system-packages

For Selenium-based scrapers:
pip install selenium --break-system-packages
brew install --cask chromedriver  # or download from chromedriver.chromium.org
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import time
from pathlib import Path
import re
from datetime import datetime
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, quote
import pandas as pd

# ============================================
# CONFIGURATION
# ============================================

DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
}

# ============================================
# 1. CMS LEIE Database (WORKING)
# ============================================

def scrape_cms_leie():
    """
    Download CMS List of Excluded Individuals/Entities
    Direct CSV download - most reliable source
    """
    print("\n" + "="*60)
    print("1. CMS LEIE Database")
    print("="*60)
    
    url = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
    output_file = DATA_DIR / "cms_leie_raw.csv"
    
    try:
        print(f"Downloading from {url}...")
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        with open(output_file, 'wb') as f:
            f.write(response.content)
        
        print(f"✅ Downloaded {len(response.content)} bytes")
        
        # Convert to JSON
        records = parse_leie_csv(output_file)
        json_file = DATA_DIR / "cms_leie_fraud.json"
        
        with open(json_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"✅ Extracted {len(records)} fraud-related records to {json_file}")
        return records
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


def parse_leie_csv(csv_file):
    """Parse LEIE CSV and extract fraud-related records"""
    records = []
    fraud_keywords = ['fraud', 'false', 'kickback', 'billing', 'claim']
    
    try:
        with open(csv_file, 'r', encoding='latin-1') as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                if idx >= 1000:  # Limit for demo - remove for full scrape
                    break
                    
                excltype = row.get('EXCLTYPE', '').lower()
                
                # Filter for fraud-related exclusions
                if any(kw in excltype for kw in fraud_keywords):
                    record = {
                        "id": f"leie_{idx}",
                        "first_name": row.get('FIRSTNAME', ''),
                        "last_name": row.get('LASTNAME', ''),
                        "business_name": row.get('BUSNAME', ''),
                        "exclusion_type": row.get('EXCLTYPE', ''),
                        "exclusion_date": row.get('EXCLDATE', ''),
                        "state": row.get('STATE', ''),
                        "specialty": row.get('SPECIALTY', ''),
                        "npi": row.get('NPI', ''),
                        "source": "CMS LEIE"
                    }
                    records.append(record)
    except Exception as e:
        print(f"Error parsing CSV: {e}")
    
    return records


# ============================================
# 2. DOJ Press Releases (WORKING)
# ============================================

def scrape_doj_healthcare_fraud():
    """
    Scrape DOJ press releases about healthcare fraud
    """
    print("\n" + "="*60)
    print("2. DOJ Healthcare Fraud Press Releases")
    print("="*60)
    
    base_url = "https://www.justice.gov"
    # Healthcare fraud component
    search_url = f"{base_url}/news?f%5B0%5D=field_pr_component%3A60"
    
    records = []
    
    try:
        for page in range(3):  # First 3 pages
            print(f"Scraping page {page + 1}...")
            
            response = requests.get(f"{search_url}&page={page}", headers=HEADERS, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find press release links
            articles = soup.find_all('h2', class_='node__title')
            
            for article in articles[:5]:  # Limit per page
                try:
                    link_tag = article.find('a')
                    if not link_tag:
                        continue
                        
                    title = link_tag.get_text(strip=True)
                    link = urljoin(base_url, link_tag['href'])
                    
                    # Filter for healthcare fraud
                    fraud_terms = ['healthcare', 'fraud', 'false claims', 'kickback', 'medicare', 'medicaid']
                    if not any(term in title.lower() for term in fraud_terms):
                        continue
                    
                    # Get article content
                    time.sleep(1)  # Be polite
                    article_resp = requests.get(link, headers=HEADERS, timeout=30)
                    article_soup = BeautifulSoup(article_resp.content, 'html.parser')
                    
                    date_elem = article_soup.find('time')
                    date = date_elem['datetime'] if date_elem else 'Unknown'
                    
                    content_elem = article_soup.find('div', class_='field--name-body')
                    content = content_elem.get_text(strip=True) if content_elem else ''
                    
                    # Extract settlement amount
                    settlement_patterns = [
                        r'\$[\d,]+(?:\.\d+)?\s*million',
                        r'\$[\d,]+(?:\.\d+)?\s*billion',
                        r'\$[\d,]+'
                    ]
                    settlement = 'Unknown'
                    for pattern in settlement_patterns:
                        match = re.search(pattern, content, re.IGNORECASE)
                        if match:
                            settlement = match.group(0)
                            break
                    
                    record = {
                        "id": f"doj_{len(records)}",
                        "title": title,
                        "date": date,
                        "settlement_amount": settlement,
                        "content": content[:2000],  # First 2000 chars
                        "url": link,
                        "source": "DOJ"
                    }
                    records.append(record)
                    print(f"  ✅ {title[:60]}...")
                    
                except Exception as e:
                    print(f"  ⚠️  Error on article: {e}")
                    continue
            
            time.sleep(2)  # Between pages
    
    except Exception as e:
        print(f"❌ Error: {e}")
    
    output_file = DATA_DIR / "doj_fraud_cases.json"
    with open(output_file, 'w') as f:
        json.dump(records, f, indent=2)
    
    print(f"✅ Saved {len(records)} records to {output_file}")
    return records


# ============================================
# 3. CMS Open Payments (WORKING)
# ============================================

def scrape_cms_open_payments():
    """
    CMS Open Payments Data - Pharma payments to doctors
    Using their API for recent data
    """
    print("\n" + "="*60)
    print("3. CMS Open Payments Data")
    print("="*60)
    
    # CMS Open Payments API
    api_url = "https://openpaymentsdata.cms.gov/api/1/datastore/query/73eb2b6f-1aaa-426d-aead-fa04c47fffb5/0"
    
    records = []
    
    try:
        # Query for high-value payments
        params = {
            'limit': 500,
            'offset': 0,
            'sort': 'Total_Amount_of_Payment_USDollars DESC'
        }
        
        print(f"Querying API...")
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        
        print(f"✅ Retrieved {len(results)} payment records")
        
        for idx, result in enumerate(results[:200]):  # Top 200 payments
            record = {
                "id": f"openpay_{idx}",
                "physician_name": f"{result.get('Physician_First_Name', '')} {result.get('Physician_Last_Name', '')}",
                "physician_specialty": result.get('Physician_Specialty', ''),
                "amount": result.get('Total_Amount_of_Payment_USDollars', 0),
                "payment_nature": result.get('Nature_of_Payment_or_Transfer_of_Value', ''),
                "submitting_entity": result.get('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', ''),
                "payment_date": result.get('Date_of_Payment', ''),
                "product_name": result.get('Name_of_Associated_Covered_Drug_or_Biological1', ''),
                "source": "CMS Open Payments"
            }
            records.append(record)
        
        output_file = DATA_DIR / "cms_open_payments.json"
        with open(output_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"✅ Saved {len(records)} records to {output_file}")
        return records
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


# ============================================
# 4. FDA Drugs@FDA (NEW)
# ============================================

def scrape_fda_drugs():
    """
    FDA Drugs@FDA database
    """
    print("\n" + "="*60)
    print("4. FDA Drugs@FDA")
    print("="*60)
    
    # FDA provides downloadable files
    url = "https://www.fda.gov/media/89850/download"
    
    try:
        print("Downloading FDA drug products file...")
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        
        output_file = DATA_DIR / "fda_drugs.zip"
        with open(output_file, 'wb') as f:
            f.write(response.content)
        
        print(f"✅ Downloaded {len(response.content)} bytes to {output_file}")
        print("   Note: Unzip and parse Products.txt for full data")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


# ============================================
# 5. FDA Warning Letters (WORKING)
# ============================================

def scrape_fda_warning_letters():
    """
    Scrape FDA warning letters via RSS feed
    """
    print("\n" + "="*60)
    print("5. FDA Warning Letters")
    print("="*60)
    
    rss_url = "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/warning-letters/rss.xml"
    
    records = []
    
    try:
        print("Fetching RSS feed...")
        response = requests.get(rss_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        
        for idx, item in enumerate(root.findall('.//item')[:50]):  # First 50
            title = item.find('title').text if item.find('title') is not None else ''
            link = item.find('link').text if item.find('link') is not None else ''
            description = item.find('description').text if item.find('description') is not None else ''
            pub_date = item.find('pubDate').text if item.find('pubDate') is not None else ''
            
            # Filter for drug/device related
            relevant_terms = ['drug', 'device', 'pharmaceutical', 'medical', 'clinical']
            if any(term in title.lower() for term in relevant_terms):
                record = {
                    "id": f"fda_warn_{idx}",
                    "title": title,
                    "date": pub_date,
                    "description": description,
                    "url": link,
                    "source": "FDA Warning Letters"
                }
                records.append(record)
        
        output_file = DATA_DIR / "fda_warning_letters.json"
        with open(output_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"✅ Saved {len(records)} records to {output_file}")
        return records
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


# ============================================
# 6. FDA FAERS (Adverse Events) (NEW)
# ============================================

def scrape_fda_faers():
    """
    FDA Adverse Event Reporting System
    Download quarterly data files
    """
    print("\n" + "="*60)
    print("6. FDA FAERS Adverse Events")
    print("="*60)
    
    # FAERS provides quarterly ZIP files
    base_url = "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"
    
    print(f"FAERS data available at: {base_url}")
    print("Note: Large files (GB) - use API for queries:")
    print("https://api.fda.gov/drug/event.json?search=patient.drug.openfda.brand_name:lipitor&limit=10")
    
    # Example API query
    try:
        api_url = "https://api.fda.gov/drug/event.json"
        params = {
            'limit': 100
        }
        
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        
        output_file = DATA_DIR / "fda_faers_sample.json"
        with open(output_file, 'w') as f:
            json.dump(results[:50], f, indent=2)
        
        print(f"✅ Saved {len(results[:50])} sample adverse event records")
        return results
        
    except Exception as e:
        print(f"❌ API Error: {e}")
        return []


# ============================================
# 7. FDA Devices@FDA (NEW)
# ============================================

def scrape_fda_devices():
    """
    FDA medical devices database via API
    """
    print("\n" + "="*60)
    print("7. FDA Devices@FDA")
    print("="*60)
    
    # OpenFDA Device API
    api_url = "https://api.fda.gov/device/510k.json"
    
    records = []
    
    try:
        params = {
            'limit': 100
        }
        
        print("Querying Device 510(k) API...")
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        
        for idx, device in enumerate(results):
            record = {
                "id": f"device_{idx}",
                "device_name": device.get('device_name', ''),
                "applicant": device.get('applicant', ''),
                "decision_date": device.get('decision_date', ''),
                "decision_description": device.get('decision_description', ''),
                "product_code": device.get('product_code', ''),
                "k_number": device.get('k_number', ''),
                "source": "FDA Devices"
            }
            records.append(record)
        
        output_file = DATA_DIR / "fda_devices.json"
        with open(output_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"✅ Saved {len(records)} device records")
        return records
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


# ============================================
# 8. FDA Device Recalls (NEW)
# ============================================

def scrape_fda_device_recalls():
    """
    FDA Device recall database
    """
    print("\n" + "="*60)
    print("8. FDA Device Recalls")
    print("="*60)
    
    api_url = "https://api.fda.gov/device/recall.json"
    
    records = []
    
    try:
        params = {
            'limit': 100
        }
        
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        
        for idx, recall in enumerate(results):
            record = {
                "id": f"recall_{idx}",
                "product_description": recall.get('product_description', ''),
                "reason_for_recall": recall.get('reason_for_recall', ''),
                "recall_status": recall.get('recall_status', ''),
                "recalling_firm": recall.get('recalling_firm', ''),
                "recall_date": recall.get('recall_initiation_date', ''),
                "classification": recall.get('classification', ''),
                "source": "FDA Device Recalls"
            }
            records.append(record)
        
        output_file = DATA_DIR / "fda_device_recalls.json"
        with open(output_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"✅ Saved {len(records)} recall records")
        return records
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


# ============================================
# 9. ClinicalTrials.gov (NEW)
# ============================================

def scrape_clinicaltrials():
    """
    ClinicalTrials.gov API
    """
    print("\n" + "="*60)
    print("9. ClinicalTrials.gov")
    print("="*60)
    
    api_url = "https://clinicaltrials.gov/api/query/study_fields"
    
    records = []
    
    try:
        params = {
            'expr': 'fraud OR misconduct',
            'fields': 'NCTId,BriefTitle,Condition,StartDate,CompletionDate,LeadSponsorName',
            'min_rnk': 1,
            'max_rnk': 100,
            'fmt': 'json'
        }
        
        print("Querying ClinicalTrials.gov...")
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        studies = data.get('StudyFieldsResponse', {}).get('StudyFields', [])
        
        for study in studies:
            record = {
                "id": study.get('NCTId', [''])[0],
                "title": study.get('BriefTitle', [''])[0],
                "condition": study.get('Condition', [''])[0],
                "sponsor": study.get('LeadSponsorName', [''])[0],
                "start_date": study.get('StartDate', [''])[0],
                "completion_date": study.get('CompletionDate', [''])[0],
                "source": "ClinicalTrials.gov"
            }
            records.append(record)
        
        output_file = DATA_DIR / "clinicaltrials.json"
        with open(output_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"✅ Saved {len(records)} clinical trial records")
        return records
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


# ============================================
# 10. SEC EDGAR Pharma Companies (NEW)
# ============================================

def scrape_sec_edgar():
    """
    SEC EDGAR filings for pharmaceutical companies
    """
    print("\n" + "="*60)
    print("10. SEC EDGAR Filings")
    print("="*60)
    
    # Major pharma companies
    companies = [
        ('0000078003', 'Pfizer'),
        ('0001067983', 'Johnson & Johnson'),
        ('0000310158', 'Merck'),
        ('0001551152', 'AbbVie'),
    ]
    
    records = []
    base_url = "https://data.sec.gov"
    
    try:
        for cik, name in companies[:2]:  # First 2 for demo
            print(f"Fetching filings for {name}...")
            
            # SEC requires user agent
            sec_headers = {
                'User-Agent': 'Medical Research contact@example.com'
            }
            
            url = f"{base_url}/submissions/CIK{cik}.json"
            response = requests.get(url, headers=sec_headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            filings = data.get('filings', {}).get('recent', {})
            
            # Get recent 10-K and 8-K forms
            forms = filings.get('form', [])
            dates = filings.get('filingDate', [])
            accession = filings.get('accessionNumber', [])
            
            for i in range(min(10, len(forms))):
                if forms[i] in ['10-K', '8-K', '10-Q']:
                    record = {
                        "id": f"sec_{cik}_{i}",
                        "company": name,
                        "cik": cik,
                        "form_type": forms[i],
                        "filing_date": dates[i],
                        "accession_number": accession[i],
                        "url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={forms[i]}",
                        "source": "SEC EDGAR"
                    }
                    records.append(record)
            
            time.sleep(1)  # SEC rate limiting
        
        output_file = DATA_DIR / "sec_edgar_pharma.json"
        with open(output_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"✅ Saved {len(records)} SEC filings")
        return records
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


# ============================================
# MASTER FUNCTION
# ============================================

def scrape_all():
    """
    Run all scrapers
    """
    print("\n" + "="*70)
    print(" MEDICAL FRAUD DATA SCRAPER v2.0")
    print(" Comprehensive Multi-Source Data Collection")
    print("="*70)
    
    all_records = {}
    
    # Run each scraper
    scrapers = [
        ("CMS LEIE", scrape_cms_leie),
        ("DOJ Cases", scrape_doj_healthcare_fraud),
        ("CMS Open Payments", scrape_cms_open_payments),
        ("FDA Drugs", scrape_fda_drugs),
        ("FDA Warning Letters", scrape_fda_warning_letters),
        ("FDA FAERS", scrape_fda_faers),
        ("FDA Devices", scrape_fda_devices),
        ("FDA Device Recalls", scrape_fda_device_recalls),
        ("ClinicalTrials", scrape_clinicaltrials),
        ("SEC EDGAR", scrape_sec_edgar),
    ]
    
    for name, scraper_func in scrapers:
        try:
            result = scraper_func()
            all_records[name] = result if result else []
        except Exception as e:
            print(f"\n❌ {name} failed: {e}")
            all_records[name] = []
        
        time.sleep(2)  # Pause between scrapers
    
    # Summary
    print("\n" + "="*70)
    print(" SCRAPING COMPLETE - SUMMARY")
    print("="*70)
    
    total_records = 0
    for name, records in all_records.items():
        count = len(records) if isinstance(records, list) else 0
        total_records += count
        status = "✅" if count > 0 else "⚠️"
        print(f"{status} {name:.<40} {count:>5} records")
    
    print(f"\nTotal Records: {total_records}")
    print(f"Output Directory: {DATA_DIR.absolute()}")
    print("\n" + "="*70)
    
    return all_records


# ============================================
# DATA COMBINER FOR RANKER
# ============================================

def combine_for_ranker():
    """
    Combine all JSON files into JSONL format for GPT ranker
    """
    print("\n" + "="*60)
    print("Combining data for ranker...")
    print("="*60)
    
    combined_records = []
    
    # Load all JSON files
    json_files = list(DATA_DIR.glob("*.json"))
    
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                
            if isinstance(data, list):
                for record in data:
                    # Create searchable text
                    text = json.dumps(record, indent=2)
                    
                    combined_records.append({
                        "id": record.get('id', f"unknown_{len(combined_records)}"),
                        "text": text,
                        "metadata": record
                    })
        except Exception as e:
            print(f"⚠️  Error loading {json_file}: {e}")
    
    # Save as JSONL
    output_file = DATA_DIR.parent / "processed" / "combined_medical_fraud_data.jsonl"
    output_file.parent.mkdir(exist_ok=True)
    
    with open(output_file, 'w') as f:
        for record in combined_records:
            f.write(json.dumps(record) + '\n')
    
    print(f"✅ Combined {len(combined_records)} records")
    print(f"   Output: {output_file}")
    
    return output_file


# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    # Scrape all sources
    results = scrape_all()
    
    # Combine for ranker
    combined_file = combine_for_ranker()
    
    print("\n🎉 ALL DONE!")
    print(f"\n📁 Raw data: {DATA_DIR}")
    print(f"📄 Combined data: {combined_file}")
    print("\nNext steps:")
    print("1. Review data/raw/*.json files")
    print("2. Use combined JSONL file with your GPT ranker")
    print("3. Adjust filters/limits in code for full-scale scraping")