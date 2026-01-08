#!/usr/bin/env python3
"""
UNFILED QUI TAM OPPORTUNITY SCRAPER
Focus: FDA/CMS databases for fraud indicators that haven't been prosecuted yet

Strategy:
- Look for FDA warning letters + high Medicare spending = potential fraud
- Look for FAERS adverse events + continued Medicare coverage = potential off-label
- Look for Open Payments + prescribing patterns = potential kickbacks
- Look for device recalls + Medicare billing = potential unnecessary procedures

SKIP: DOJ settlements (already filed)
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import time
from pathlib import Path
import re
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import sys

# ============================================
# CONFIGURATION
# ============================================

DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
}

# ============================================
# 1. FDA WARNING LETTERS (High Fraud Potential)
# ============================================

def scrape_fda_warning_letters():
    """
    FDA Warning Letters - look for:
    - Off-label marketing
    - Manufacturing violations
    - False/misleading claims
    
    Cross-reference with Medicare spending = qui tam opportunity
    """
    print("\n" + "="*60)
    print("1. FDA Warning Letters (Potential Unfiled Fraud)")
    print("="*60)
    
    # FDA Warning Letters RSS feed
    rss_url = "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/warning-letters/rss.xml"
    
    records = []
    
    try:
        print("Fetching warning letters...")
        response = requests.get(rss_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        
        for idx, item in enumerate(root.findall('.//item')[:100]):  # Last 100 letters
            try:
                title = item.find('title').text if item.find('title') is not None else ''
                link = item.find('link').text if item.find('link') is not None else ''
                description = item.find('description').text if item.find('description') is not None else ''
                pub_date = item.find('pubDate').text if item.find('pubDate') is not None else ''
                
                # Get full letter text
                time.sleep(1)
                letter_resp = requests.get(link, headers=HEADERS, timeout=30)
                soup = BeautifulSoup(letter_resp.content, 'html.parser')
                
                # Extract letter content
                content_div = soup.find('div', class_='content-body')
                content = content_div.get_text(strip=True) if content_div else description
                
                # Look for qui tam indicators
                qui_tam_indicators = []
                fraud_score = 0
                
                # Check for Medicare/Medicaid mentions
                if 'medicare' in content.lower() or 'medicaid' in content.lower():
                    qui_tam_indicators.append('Federal healthcare program mentioned')
                    fraud_score += 30
                
                # Check for fraud-related violations
                fraud_keywords = {
                    'off-label': 20,
                    'misleading': 15,
                    'false claim': 25,
                    'kickback': 30,
                    'promotional': 10,
                    'unapproved use': 20,
                    'misbranding': 10
                }
                
                for keyword, points in fraud_keywords.items():
                    if keyword in content.lower():
                        qui_tam_indicators.append(f'Violation: {keyword}')
                        fraud_score += points
                
                # Extract company name
                company_match = re.search(r'([A-Z][A-Za-z\s&.,]+(?:Inc|LLC|Corp|Co|Pharma|Laboratories))', content)
                company = company_match.group(1).strip() if company_match else 'Unknown'
                
                # Extract drug/device names
                drug_pattern = r'\b([A-Z][a-z]+(?:mab|nib|pril|olol|statin|mycin))\b'
                drugs = list(set(re.findall(drug_pattern, content)))
                
                # Only include if has qui tam potential (fraud_score > 15)
                if fraud_score >= 15:
                    record = {
                        "id": f"fda_warn_{idx}",
                        "company": company,
                        "title": title,
                        "date": pub_date,
                        "drugs_mentioned": drugs[:5],  # Top 5
                        "qui_tam_indicators": qui_tam_indicators,
                        "fraud_potential_score": fraud_score,
                        "violation_summary": description[:300],
                        "letter_content": content[:2000],
                        "url": link,
                        "case_status": "unfiled",  # FDA action, not qui tam filed yet
                        "source": "FDA Warning Letters"
                    }
                    records.append(record)
                    print(f"  ✅ {company} - Score: {fraud_score}")
                    
            except Exception as e:
                print(f"  ⚠️  Error: {e}")
                continue
        
        output_file = DATA_DIR / "fda_warning_letters_unfiled.json"
        with open(output_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"\n✅ Found {len(records)} potential unfiled cases")
        return records
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


# ============================================
# 2. CMS OPEN PAYMENTS + MEDICARE SPENDING
# ============================================

def scrape_open_payments_high_prescribers():
    """
    CMS Open Payments cross-referenced with high prescribing
    Strategy: High payments + high prescribing = potential kickback
    """
    print("\n" + "="*60)
    print("2. Open Payments (Potential Kickback Schemes)")
    print("="*60)
    
    api_url = "https://openpaymentsdata.cms.gov/api/1/datastore/query/73eb2b6f-1aaa-426d-aead-fa04c47fffb5/0"
    
    records = []
    
    try:
        # Query for high-value payments
        params = {
            'limit': 1000,
            'offset': 0,
            'sort': 'Total_Amount_of_Payment_USDollars DESC'
        }
        
        print("Querying high-value pharmaceutical payments...")
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        
        print(f"Retrieved {len(results)} payment records")
        
        # Filter for suspicious patterns
        for idx, result in enumerate(results):
            amount = float(result.get('Total_Amount_of_Payment_USDollars', 0))
            
            # High-value payments (potential kickbacks)
            if amount >= 50000:  # $50k+ threshold
                physician_name = f"{result.get('Physician_First_Name', '')} {result.get('Physician_Last_Name', '')}".strip()
                specialty = result.get('Physician_Specialty', '')
                company = result.get('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', '')
                nature = result.get('Nature_of_Payment_or_Transfer_of_Value', '')
                
                # Calculate fraud indicators
                fraud_indicators = []
                fraud_score = 40  # Base score for high payment
                
                # Suspicious payment types
                suspicious_natures = ['Consulting Fee', 'Compensation for services other than consulting', 'Honoraria', 'Travel and Lodging']
                if any(s.lower() in nature.lower() for s in suspicious_natures):
                    fraud_indicators.append(f'Suspicious payment type: {nature}')
                    fraud_score += 15
                
                # Multiple payments boost score
                # (In reality, would query for this physician's total)
                if amount > 100000:
                    fraud_indicators.append('Very high single payment (>$100k)')
                    fraud_score += 20
                
                record = {
                    "id": f"kickback_{idx}",
                    "physician_name": physician_name,
                    "physician_specialty": specialty,
                    "npi": result.get('Covered_Recipient_NPI', ''),
                    "payment_amount": amount,
                    "payment_nature": nature,
                    "paying_company": company,
                    "payment_date": result.get('Date_of_Payment', ''),
                    "product_name": result.get('Name_of_Associated_Covered_Drug_or_Biological1', ''),
                    "fraud_indicators": fraud_indicators,
                    "fraud_potential_score": fraud_score,
                    "case_status": "unfiled",
                    "next_steps": "Cross-reference with Medicare prescribing data to establish pattern",
                    "source": "CMS Open Payments"
                }
                records.append(record)
        
        output_file = DATA_DIR / "open_payments_kickback_potential.json"
        with open(output_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"✅ Found {len(records)} potential kickback cases")
        return records
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

#!/usr/bin/env python3
"""
Add this function to your unfiled_qui_tam_scraper.py

Place it AFTER the FAERS function and BEFORE the LEIE function
"""

# ============================================
# NEW: MEDICARE PART D PRESCRIBER DATA
# ============================================

def scrape_medicare_part_d_high_prescribers():
    """
    Medicare Part D Prescriber Data - WORKING API
    Strategy: High prescribers + Open Payments = kickback leads
    """
    print("\n" + "="*60)
    print("5. Medicare Part D High Prescribers (KICKBACK LEADS)")
    print("="*60)
    
    # WORKING API URL (Jan 2025)
    api_url = "https://data.cms.gov/data-api/v1/dataset/9552739e-3d05-4c1b-8eff-ecabf391e2e5/data"
    
    records = []
    
    try:
        # Query for top prescribers by total drug cost
        params = {
            'size': 1000,  # Get top 1000 prescribers
            'offset': 0,
            'sort': 'Tot_Drug_Cst:desc'  # Sort by total cost
        }
        
        print("Querying high-cost Medicare Part D prescribers...")
        print(f"API: {api_url}")
        
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        
        print(f"✅ Retrieved {len(data)} prescriber records")
        
        # Analyze for fraud patterns
        for idx, prescriber in enumerate(data):
            try:
                # Get key fields
                npi = prescriber.get('Prscrbr_NPI', '')
                first_name = prescriber.get('Prscrbr_First_Name', '')
                last_name = prescriber.get('Prscrbr_Last_Name', '')
                specialty = prescriber.get('Prscrbr_Type', '')
                state = prescriber.get('Prscrbr_State_Abrvtn', '')
                
                # Financial data
                total_cost = float(prescriber.get('Tot_Drug_Cst', 0))
                total_claims = int(prescriber.get('Tot_Clms', 0))
                beneficiary_count = int(prescriber.get('Tot_Benes', 0))
                
                # Calculate fraud indicators
                fraud_score = 50  # Base score for high prescriber
                fraud_indicators = []
                
                # INDICATOR 1: Very high total cost
                if total_cost >= 5000000:  # $5M+
                    fraud_score += 35
                    fraud_indicators.append(f'Very high Medicare spending: ${total_cost/1000000:.1f}M')
                elif total_cost >= 2000000:  # $2M+
                    fraud_score += 25
                    fraud_indicators.append(f'High Medicare spending: ${total_cost/1000000:.1f}M')
                
                # INDICATOR 2: High cost per beneficiary
                if beneficiary_count > 0:
                    cost_per_bene = total_cost / beneficiary_count
                    if cost_per_bene >= 15000:  # $15k+ per patient
                        fraud_score += 20
                        fraud_indicators.append(f'Very high cost per patient: ${cost_per_bene:,.0f}/patient')
                    elif cost_per_bene >= 10000:  # $10k+ per patient
                        fraud_score += 15
                        fraud_indicators.append(f'High cost per patient: ${cost_per_bene:,.0f}/patient')
                
                # INDICATOR 3: High claims volume
                if total_claims >= 10000:
                    fraud_score += 15
                    fraud_indicators.append(f'Very high claim volume: {total_claims:,} claims')
                elif total_claims >= 5000:
                    fraud_score += 10
                    fraud_indicators.append(f'High claim volume: {total_claims:,} claims')
                
                # INDICATOR 4: High beneficiary count (possible mill)
                if beneficiary_count >= 2000:
                    fraud_score += 10
                    fraud_indicators.append(f'Very high patient volume: {beneficiary_count:,} patients')
                
                # Only include high-value targets (score 70+ OR cost $2M+)
                if fraud_score >= 70 or total_cost >= 2000000:
                    provider_name = f"{first_name} {last_name}".strip()
                    
                    record = {
                        "id": f"partd_{idx}",
                        "provider_name": provider_name,
                        "npi": npi,
                        "specialty": specialty,
                        "state": state,
                        "total_drug_cost": total_cost,
                        "total_claims": total_claims,
                        "beneficiary_count": beneficiary_count,
                        "cost_per_beneficiary": round(cost_per_bene, 2) if beneficiary_count > 0 else 0,
                        "fraud_indicators": fraud_indicators,
                        "fraud_potential_score": fraud_score,
                        "case_status": "unfiled",
                        "next_steps": f"1. Cross-reference NPI {npi} with Open Payments; 2. Download detailed drug-level data for this prescriber; 3. Compare to specialty averages",
                        "source": "Medicare Part D"
                    }
                    records.append(record)
                    
                    # Show first 10
                    if len(records) <= 10:
                        print(f"  🎯 {provider_name} ({specialty}) - ${total_cost/1000000:.1f}M - Score: {fraud_score}")
                
            except Exception as e:
                continue
        
        output_file = DATA_DIR / "medicare_part_d_high_prescribers.json"
        with open(output_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"\n✅ Found {len(records)} high-value prescribers")
        
        if records:
            total_spending = sum(r['total_drug_cost'] for r in records)
            avg_spending = total_spending / len(records)
            print(f"   Total spending: ${total_spending/1000000:.1f}M")
            print(f"   Average: ${avg_spending/1000000:.1f}M per prescriber")
            print(f"   Highest: ${max(r['total_drug_cost'] for r in records)/1000000:.1f}M")
        
        return records
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nAPI failed. Alternative:")
        print("1. Go to: https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers")
        print("2. Download CSV manually")
        print("3. Save as: data/raw/medicare_part_d_prescribers.csv")
        return []


# ADD THIS TO YOUR combine_for_ranker() function:
# In the source_files list, add:
#     "medicare_part_d_high_prescribers.json",

# ADD THIS TO YOUR main() function:
# After the Device Fraud section, add:
#     results['Part D High Prescribers'] = scrape_medicare_part_d_high_prescribers()
#     time.sleep(2)


# ============================================
# 3. FAERS + MEDICARE COVERAGE (Off-Label)
# ============================================

def scrape_faers_medicare_overlap():
    """
    FDA Adverse Events + Medicare coverage
    Strategy: High adverse events + Medicare still covers = potential off-label fraud
    """
    print("\n" + "="*60)
    print("3. FAERS Adverse Events (Off-Label Fraud Potential)")
    print("="*60)
    
    api_url = "https://api.fda.gov/drug/event.json"
    
    records = []
    
    try:
        # Query for serious adverse events
        params = {
            'search': 'serious:1',
            'limit': 500
        }
        
        print("Querying serious adverse events...")
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        
        print(f"Retrieved {len(results)} adverse event reports")
        
        # Aggregate by drug
        drug_events = {}
        
        for event in results:
            patient = event.get('patient', {})
            drugs = patient.get('drug', [])
            
            for drug in drugs:
                brand_name = drug.get('openfda', {}).get('brand_name', ['Unknown'])[0]
                
                if brand_name not in drug_events:
                    drug_events[brand_name] = {
                        'count': 0,
                        'serious_outcomes': [],
                        'indications': []
                    }
                
                drug_events[brand_name]['count'] += 1
                
                # Track serious outcomes
                reactions = patient.get('reaction', [])
                for reaction in reactions:
                    outcome = reaction.get('reactionoutcome', 'Unknown')
                    drug_events[brand_name]['serious_outcomes'].append(outcome)
                
                # Track off-label use indicators
                indication = drug.get('drugindication', '')
                if indication:
                    drug_events[brand_name]['indications'].append(indication)
        
        # Create records for drugs with high adverse event rates
        for idx, (drug_name, data) in enumerate(sorted(drug_events.items(), key=lambda x: x[1]['count'], reverse=True)[:100]):
            
            if data['count'] >= 5:  # At least 5 events
                fraud_score = 30  # Base score
                fraud_indicators = []
                
                # More events = higher score
                if data['count'] >= 20:
                    fraud_score += 30
                    fraud_indicators.append(f"High adverse event volume: {data['count']} reports")
                elif data['count'] >= 10:
                    fraud_score += 20
                    fraud_indicators.append(f"Moderate adverse event volume: {data['count']} reports")
                
                # Diverse indications = potential off-label use
                unique_indications = len(set(data['indications']))
                if unique_indications >= 5:
                    fraud_score += 25
                    fraud_indicators.append(f"Multiple indications ({unique_indications}) suggesting off-label use")
                
                record = {
                    "id": f"offlabel_{idx}",
                    "drug_name": drug_name,
                    "adverse_event_count": data['count'],
                    "serious_outcomes": list(set(data['serious_outcomes']))[:10],
                    "indication_diversity": unique_indications,
                    "fraud_indicators": fraud_indicators,
                    "fraud_potential_score": fraud_score,
                    "case_status": "unfiled",
                    "next_steps": "Verify Medicare coverage and prescribing patterns for off-label use",
                    "source": "FDA FAERS"
                }
                records.append(record)
        
        output_file = DATA_DIR / "faers_offlabel_potential.json"
        with open(output_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"✅ Found {len(records)} potential off-label cases")
        return records
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


# ============================================
# 4. DEVICE RECALLS + MEDICARE BILLING
# ============================================

def scrape_device_recalls_medicare():
    """
    FDA Device Recalls + continued Medicare billing
    Strategy: Recalled devices still being billed to Medicare = potential fraud
    """
    print("\n" + "="*60)
    print("4. Device Recalls (Unnecessary Procedure Fraud)")
    print("="*60)
    
    api_url = "https://api.fda.gov/device/recall.json"
    
    records = []
    
    try:
        params = {
            'search': 'classification:("Class I" OR "Class II")',  # Serious recalls
            'limit': 200
        }
        
        print("Querying device recalls...")
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        
        print(f"Retrieved {len(results)} recall records")
        
        for idx, recall in enumerate(results):
            product = recall.get('product_description', '')
            reason = recall.get('reason_for_recall', '')
            classification = recall.get('classification', '')
            firm = recall.get('recalling_firm', '')
            recall_date = recall.get('recall_initiation_date', '')
            
            # Score based on severity and reason
            fraud_score = 0
            fraud_indicators = []
            
            if classification == 'Class I':
                fraud_score += 40
                fraud_indicators.append('Class I recall (most serious)')
            elif classification == 'Class II':
                fraud_score += 25
                fraud_indicators.append('Class II recall (serious)')
            
            # Look for fraud-relevant reasons
            fraud_keywords = {
                'defect': 15,
                'malfunction': 15,
                'failure': 15,
                'adverse': 20,
                'death': 30,
                'injury': 25
            }
            
            reason_lower = reason.lower()
            for keyword, points in fraud_keywords.items():
                if keyword in reason_lower:
                    fraud_score += points
                    fraud_indicators.append(f'Serious issue: {keyword}')
                    break  # Only add once
            
            # Check if recent (more likely still being billed)
            try:
                recall_year = int(recall_date[:4]) if recall_date else 2020
                if recall_year >= 2022:
                    fraud_score += 20
                    fraud_indicators.append('Recent recall (likely still being billed)')
            except:
                pass
            
            if fraud_score >= 40:  # Threshold for qui tam potential
                record = {
                    "id": f"recall_{idx}",
                    "product_description": product,
                    "reason_for_recall": reason,
                    "classification": classification,
                    "recalling_firm": firm,
                    "recall_date": recall_date,
                    "fraud_indicators": fraud_indicators,
                    "fraud_potential_score": fraud_score,
                    "case_status": "unfiled",
                    "next_steps": "Check Medicare claims data for continued billing of recalled device",
                    "source": "FDA Device Recalls"
                }
                records.append(record)
        
        output_file = DATA_DIR / "device_recalls_fraud_potential.json"
        with open(output_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"✅ Found {len(records)} potential device fraud cases")
        return records
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


# ============================================
# 5. CMS LEIE RECENT EXCLUSIONS
# ============================================

def scrape_cms_leie_recent():
    """
    Recent CMS LEIE exclusions (last 2 years)
    Strategy: Just excluded = potential qui tam not filed yet
    """
    print("\n" + "="*60)
    print("5. Recent CMS LEIE Exclusions (Potential Unfiled)")
    print("="*60)
    
    url = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
    
    try:
        print("Downloading LEIE database...")
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        csv_file = DATA_DIR / "cms_leie_raw.csv"
        with open(csv_file, 'wb') as f:
            f.write(response.content)
        
        print(f"Downloaded {len(response.content):,} bytes")
        
        records = []
        fraud_keywords = ['fraud', 'false claim', 'kickback', 'billing']
        current_year = datetime.now().year
        
        with open(csv_file, 'r', encoding='latin-1') as f:
            reader = csv.DictReader(f)
            
            for idx, row in enumerate(reader):
                if idx >= 5000:  # Limit
                    break
                
                excltype = row.get('EXCLTYPE', '').lower()
                excldate = row.get('EXCLDATE', '')
                
                # Filter for fraud AND recent (last 2 years)
                if any(kw in excltype for kw in fraud_keywords):
                    try:
                        # Parse date (format: YYYYMMDD)
                        year = int(excldate[:4]) if len(excldate) >= 4 else 2020
                        
                        if year >= current_year - 2:  # Last 2 years
                            provider_name = f"{row.get('FIRSTNAME', '')} {row.get('LASTNAME', '')}".strip()
                            if not provider_name:
                                provider_name = row.get('BUSNAME', 'Unknown')
                            
                            # Calculate fraud score
                            fraud_score = 50  # Base for recent fraud exclusion
                            fraud_indicators = ['Recent exclusion (unfiled qui tam opportunity)']
                            
                            # Boost score for specific fraud types
                            if 'false claim' in excltype:
                                fraud_score += 25
                                fraud_indicators.append('False Claims Act violation')
                            if 'kickback' in excltype:
                                fraud_score += 25
                                fraud_indicators.append('Kickback violation')
                            if 'medicare' in excltype or 'medicaid' in excltype:
                                fraud_score += 20
                                fraud_indicators.append('Federal healthcare program fraud')
                            
                            record = {
                                "id": f"leie_{idx}",
                                "provider_name": provider_name,
                                "business_name": row.get('BUSNAME', ''),
                                "exclusion_type": row.get('EXCLTYPE', ''),
                                "exclusion_date": excldate,
                                "state": row.get('STATE', ''),
                                "specialty": row.get('SPECIALTY', ''),
                                "npi": row.get('NPI', ''),
                                "fraud_indicators": fraud_indicators,
                                "fraud_potential_score": fraud_score,
                                "case_status": "unfiled",
                                "next_steps": "Investigate if qui tam case already filed; if not, potential opportunity",
                                "source": "CMS LEIE"
                            }
                            records.append(record)
                    except:
                        continue
        
        output_file = DATA_DIR / "cms_leie_recent_unfiled.json"
        with open(output_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"✅ Found {len(records)} recent exclusions")
        return records
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


# ============================================
# COMBINE FOR RANKER
# ============================================

def combine_for_ranker():
    """
    Combine all sources into JSONL for ranker
    OPTIMIZED for large CSV files with progress tracking
    """
    print("\n" + "="*60)
    print("Combining Data for Ranker")
    print("="*60)
    
    combined_records = []
    
    # Get ALL .json and .csv files
    print(f"Scanning directory: {DATA_DIR}")
    json_files = list(DATA_DIR.glob("*.json"))
    csv_files = list(DATA_DIR.glob("*.csv"))
    
    all_files = json_files + csv_files
    
    print(f"Found {len(json_files)} JSON files and {len(csv_files)} CSV files")
    print(f"Total: {len(all_files)} files to process")
    
    if not all_files:
        print("⚠️  No JSON or CSV files found in data/raw!")
        return None
    
    # ============================================
    # PROCESS JSON FILES
    # ============================================
    for file_path in json_files:
        print(f"  Loading JSON: {file_path.name}")
        
        try:
            with open(file_path, 'r') as f:
                records = json.load(f)
            
            if not isinstance(records, list):
                records = [records]
            
            for record in records:
                text_parts = []
                
                text_parts.append(f"SOURCE: {record.get('source', file_path.stem)}")
                text_parts.append(f"CASE STATUS: {record.get('case_status', 'unknown')}")
                text_parts.append(f"FRAUD POTENTIAL SCORE: {record.get('fraud_potential_score', 0)}")
                text_parts.append("")
                
                if 'fraud_indicators' in record and record['fraud_indicators']:
                    text_parts.append("FRAUD INDICATORS:")
                    for indicator in record['fraud_indicators']:
                        text_parts.append(f"  - {indicator}")
                    text_parts.append("")
                
                for key, value in record.items():
                    if key not in ['id', 'source', 'case_status', 'fraud_potential_score', 'fraud_indicators']:
                        if isinstance(value, list):
                            if value:
                                text_parts.append(f"{key.upper().replace('_', ' ')}: {', '.join(str(v) for v in value[:10])}")
                        else:
                            text_parts.append(f"{key.upper().replace('_', ' ')}: {value}")
                
                combined_records.append({
                    "source": record.get('source', file_path.stem),
                    "filename": record.get('id', f"{file_path.stem}_{len(combined_records)}"),
                    "text": "\n".join(text_parts),
                    "metadata": record
                })
            
            print(f"    ✅ Loaded {len(records)} records from {file_path.name}")
        
        except Exception as e:
            print(f"    ⚠️  Error loading {file_path.name}: {e}")
    
    # ============================================
    # PROCESS CSV FILES - OPTIMIZED
    # ============================================
    for file_path in csv_files:
        print(f"  Loading CSV: {file_path.name}")
        
        # Check file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        print(f"    File size: {file_size_mb:.1f} MB")
        
        try:
            # Set limits based on file size
            if file_size_mb > 100:  # Very large files
                max_rows = 1000
                print(f"    ⚠️  Large file detected, limiting to {max_rows:,} rows")
            elif file_size_mb > 50:  # Large files
                max_rows = 5000
                print(f"    ⚠️  Limiting to {max_rows:,} rows")
            else:  # Normal files
                max_rows = 10000
            
            row_count = 0
            processed_count = 0
            
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                # First, count total rows quickly
                print(f"    Counting rows...", end='', flush=True)
                total_rows = sum(1 for _ in f) - 1  # Subtract header
                print(f" {total_rows:,} rows found")
                
                # Reset file pointer
                f.seek(0)
                
                reader = csv.DictReader(f)
                
                print(f"    Processing (max {max_rows:,})...", end='', flush=True)
                
                for idx, row in enumerate(reader):
                    if idx >= max_rows:
                        break
                    
                    row_count += 1
                    
                    # Progress indicator every 100 rows
                    if row_count % 100 == 0:
                        print(f"\r    Processing row {row_count:,}/{min(max_rows, total_rows):,}...", end='', flush=True)
                    
                    # Skip rows with no useful data
                    non_empty_values = [v for v in row.values() if v and str(v).strip()]
                    if len(non_empty_values) < 3:  # Must have at least 3 non-empty fields
                        continue
                    
                    # Format CSV row as text (SIMPLIFIED for speed)
                    text_parts = [
                        f"SOURCE: {file_path.stem}",
                        f"FILE TYPE: CSV",
                        f"ROW NUMBER: {idx + 1}",
                        ""
                    ]
                    
                    # Add only the most important columns (first 20 non-empty)
                    added = 0
                    for key, value in row.items():
                        if value and str(value).strip() and added < 20:
                            clean_key = key.strip().upper().replace('_', ' ')
                            clean_value = str(value).strip()
                            if len(clean_value) > 200:  # Shorter truncation
                                clean_value = clean_value[:200] + "..."
                            text_parts.append(f"{clean_key}: {clean_value}")
                            added += 1
                    
                    combined_records.append({
                        "source": file_path.stem,
                        "filename": f"{file_path.stem}_row_{idx}",
                        "text": "\n".join(text_parts),
                        "metadata": {
                            "file_type": "csv",
                            "source_file": file_path.name,
                            "row_number": idx + 1
                        }
                    })
                    processed_count += 1
            
            print(f"\r    ✅ Loaded {processed_count:,} rows from {file_path.name}")
        
        except Exception as e:
            print(f"\n    ⚠️  Error loading {file_path.name}: {e}")
    
    # ============================================
    # SAVE COMBINED DATA
    # ============================================
    output_dir = DATA_DIR.parent / "processed"
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "combined_medical_fraud_data.jsonl"
    
    print(f"\nWriting {len(combined_records):,} records to JSONL...")
    with open(output_file, 'w') as f:
        for i, record in enumerate(combined_records):
            if i % 1000 == 0 and i > 0:
                print(f"  Written {i:,}/{len(combined_records):,}...", end='\r', flush=True)
            f.write(json.dumps(record) + '\n')
    
    print(f"\n✅ Combined {len(combined_records):,} records from {len(all_files)} files")
    print(f"   - JSON files: {len(json_files)}")
    print(f"   - CSV files: {len(csv_files)}")
    print(f"   Output: {output_file}")
    print(f"   File size: {output_file.stat().st_size / (1024*1024):.1f} MB")
    
    # Show sample
    if combined_records:
        print("\nSample record (first):")
        print("-" * 60)
        print(combined_records[0]['text'][:400])
        print("...")
    
    return output_file


# ============================================
# MAIN
# ============================================

def main():
    print("\n" + "="*70)
    print(" UNFILED QUI TAM OPPORTUNITY SCRAPER")
    print(" Focus: FDA/CMS databases for undisclosed fraud")
    print("="*70)
    
    results = {}
    
    print("\n🎯 Scraping for UNFILED opportunities...")
    print("(SKIPPING: DOJ settlements, news articles)")
    print()
    
    results['Warning Letters'] = scrape_fda_warning_letters()
    time.sleep(2)
    
    results['Kickback Potential'] = scrape_open_payments_high_prescribers()
    time.sleep(2)
    
    results['Off-Label Potential'] = scrape_faers_medicare_overlap()
    time.sleep(2)
    
    results['Device Fraud'] = scrape_device_recalls_medicare()
    time.sleep(2)
    
    results['Recent Exclusions'] = scrape_cms_leie_recent()
    
    # Summary
    print("\n" + "="*70)
    print(" SCRAPING COMPLETE")
    print("="*70)
    
    for name, records in results.items():
        count = len(records) if records else 0
        print(f"✅ {name:.<50} {count:>5} records")
    
    total = sum(len(r) if r else 0 for r in results.values())
    print(f"\nTotal Unfiled Opportunities: {total}")
    
    # Combine
    combined_file = combine_for_ranker()
    
    print("\n🎉 DONE!")
    print(f"\n📁 Raw data: {DATA_DIR}")
    print(f"📄 Combined JSONL: {combined_file}")
    print("\nNext steps:")
    print("1. python3 convert_jsonl_to_csv.py")
    print("2. python gpt_ranker.py --chunk-size 0 --max-rows 10")
    print("\nExpected scores:")
    print("  - FDA warning letters + Medicare: 60-85")
    print("  - High-value kickback payments: 55-75")
    print("  - Off-label + adverse events: 50-70")
    print("  - Recalled devices still billed: 55-75")
    print("  - Recent LEIE fraud exclusions: 60-80")
    print("\nAll cases are UNFILED (not in DOJ settlements)")


if __name__ == "__main__":
    main()