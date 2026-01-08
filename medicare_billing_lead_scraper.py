#!/usr/bin/env python3
"""
ENHANCED QUI TAM LEAD SCRAPER
Cross-references FDA/CMS databases with Medicare billing data

Strategy:
1. Get leads from warning letters, payments, recalls (your scraper)
2. Cross-reference with Medicare Part D prescriber data
3. Flag HIGH-RISK patterns (kickbacks, upcoding, off-label)
4. Output prioritized research leads

This gives you LEADS, not complete cases. You still need to:
- PACER search (check if already filed)
- Get insider documents
- Prove systematic pattern
- Calculate damages
"""

import requests
import json
import csv
from pathlib import Path
from collections import defaultdict
import time

DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
}

# ============================================
# 1. MEDICARE PART D PRESCRIBER DATA
# ============================================

def download_medicare_part_d_data():
    """
    Download Medicare Part D prescriber data
    This has ACTUAL BILLING PATTERNS
    """
    print("\n" + "="*60)
    print("1. Downloading Medicare Part D Prescriber Data")
    print("="*60)
    
    # CMS Data.gov API endpoint
    # This is a HUGE file (several GB) so we'll use the API to query
    
    api_url = "https://data.cms.gov/data-api/v1/dataset/3a662c00-1e9e-4e89-83e0-74e6f312f11e/data"
    
    # For demo, get top 1000 high-cost prescribers
    params = {
        'size': 1000,
        'offset': 0,
        'sort': 'tot_drug_cst:desc'  # Sort by total cost
    }
    
    try:
        print("Querying top Medicare Part D prescribers...")
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        
        output_file = DATA_DIR / "medicare_part_d_prescribers.json"
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✅ Downloaded {len(data)} prescriber records")
        return data
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Note: This is a large dataset. For full data, download CSV from:")
        print("https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers")
        return []


# ============================================
# 2. CROSS-REFERENCE OPEN PAYMENTS + PART D
# ============================================

def find_kickback_patterns():
    """
    Cross-reference Open Payments with Medicare Part D
    High payments + High prescribing = STRONG kickback lead
    """
    print("\n" + "="*60)
    print("2. Finding Kickback Patterns (STRONG LEADS)")
    print("="*60)
    
    leads = []
    
    # Get Open Payments data (high-value payments)
    open_payments_api = "https://openpaymentsdata.cms.gov/api/1/datastore/query/73eb2b6f-1aaa-426d-aead-fa04c47fffb5/0"
    
    try:
        print("Fetching high-value payments...")
        params = {
            'limit': 500,
            'sort': 'Total_Amount_of_Payment_USDollars DESC'
        }
        
        response = requests.get(open_payments_api, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        payment_data = response.json().get('results', [])
        print(f"Got {len(payment_data)} payment records")
        
        # Index by NPI
        payments_by_npi = defaultdict(list)
        for payment in payment_data:
            npi = payment.get('Covered_Recipient_NPI', '')
            if npi:
                payments_by_npi[npi].append(payment)
        
        # Now get Medicare prescriber data for these NPIs
        # (In real implementation, you'd download full Part D data and cross-reference)
        
        print("Analyzing payment patterns...")
        for npi, payments in list(payments_by_npi.items())[:100]:  # Top 100
            total_payments = sum(float(p.get('Total_Amount_of_Payment_USDollars', 0)) for p in payments)
            
            if total_payments >= 50000:  # $50k+ threshold
                physician_name = f"{payments[0].get('Physician_First_Name', '')} {payments[0].get('Physician_Last_Name', '')}".strip()
                
                # Get unique companies paying this doctor
                companies = {}
                for payment in payments:
                    company = payment.get('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', '')
                    if company:
                        if company not in companies:
                            companies[company] = 0
                        companies[company] += float(payment.get('Total_Amount_of_Payment_USDollars', 0))
                
                # High payments from single company = red flag
                for company, amount in companies.items():
                    if amount >= 50000:
                        lead = {
                            "lead_type": "KICKBACK_PATTERN",
                            "physician_name": physician_name,
                            "npi": npi,
                            "paying_company": company,
                            "total_payments": amount,
                            "fraud_score": min(95, 50 + int(amount / 10000)),  # Cap at 95
                            "research_steps": [
                                f"1. Download full Medicare Part D data for NPI {npi}",
                                f"2. Check if high prescriber of {company} drugs",
                                f"3. Compare prescribing before/after payments started",
                                f"4. PACER search: '{physician_name}' + 'qui tam' (check if already filed)",
                                "5. Calculate excess costs to Medicare",
                                "6. Find insider (nurse, billing staff) who can testify"
                            ],
                            "data_sources": {
                                "medicare_part_d": "https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers",
                                "open_payments": f"https://openpaymentsdata.cms.gov/physician/{npi}",
                                "pacer": "https://pacer.uscourts.gov/ (search sealed qui tam cases)"
                            },
                            "case_status": "RESEARCH_LEAD",
                            "source": "Open Payments + Part D Cross-Reference"
                        }
                        leads.append(lead)
                        print(f"  🎯 LEAD: {physician_name} ← ${amount:,.0f} from {company}")
        
        output_file = DATA_DIR / "kickback_research_leads.json"
        with open(output_file, 'w') as f:
            json.dump(leads, f, indent=2)
        
        print(f"\n✅ Found {len(leads)} kickback research leads")
        return leads
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


# ============================================
# 3. FIND UPCODING OUTLIERS
# ============================================

def find_upcoding_outliers():
    """
    Download Medicare Physician/Practitioner data
    Find outliers who bill way above average
    """
    print("\n" + "="*60)
    print("3. Finding Upcoding Outliers (STRONG LEADS)")
    print("="*60)
    
    # Medicare Physician & Other Practitioners API
    api_url = "https://data.cms.gov/data-api/v1/dataset/8c783483-7d0e-4bdb-b69c-c657f4f7134c/data"
    
    leads = []
    
    try:
        print("Querying high-billing providers...")
        
        # Get providers sorted by total payment
        params = {
            'size': 500,
            'offset': 0,
            'sort': 'tot_sbmtd_chrg:desc'  # Total submitted charges
        }
        
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        print(f"Got {len(data)} provider records")
        
        # Analyze billing patterns
        # (In real implementation, calculate z-scores vs. national averages)
        
        for provider in data[:50]:  # Top 50 billers
            npi = provider.get('Rndrng_NPI', '')
            name = f"{provider.get('Rndrng_Prvdr_First_Name', '')} {provider.get('Rndrng_Prvdr_Last_Name', '')}".strip()
            specialty = provider.get('Rndrng_Prvdr_Type', '')
            
            total_charges = float(provider.get('Tot_Sbmtd_Chrg', 0))
            total_services = int(provider.get('Tot_Srvcs', 0))
            
            if total_services > 0:
                avg_charge = total_charges / total_services
                
                # Flag if average charge is suspiciously high
                # (Real implementation would compare to specialty average)
                if avg_charge > 500:  # Threshold
                    lead = {
                        "lead_type": "UPCODING_OUTLIER",
                        "provider_name": name,
                        "npi": npi,
                        "specialty": specialty,
                        "total_charges": total_charges,
                        "total_services": total_services,
                        "avg_charge_per_service": avg_charge,
                        "fraud_score": 70,  # Default for outlier
                        "research_steps": [
                            f"1. Download detailed service-level data for NPI {npi}",
                            "2. Compare CPT codes billed vs. specialty average",
                            "3. Check ratio of 99215 (high-complexity) vs 99213 (routine)",
                            "4. Look for impossible billing (24+ hrs/day)",
                            f"5. PACER search: '{name}' + 'false claims'",
                            "6. Review medical records for actual complexity",
                            "7. Find billing staff who can testify to systematic upcoding"
                        ],
                        "data_sources": {
                            "detailed_billing": f"https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners",
                            "specialty_averages": "https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-service-type-reports"
                        },
                        "case_status": "RESEARCH_LEAD",
                        "source": "Medicare Provider Utilization"
                    }
                    leads.append(lead)
                    print(f"  🎯 LEAD: {name} - Avg ${avg_charge:,.0f}/service ({total_services:,} services)")
        
        output_file = DATA_DIR / "upcoding_research_leads.json"
        with open(output_file, 'w') as f:
            json.dump(leads, f, indent=2)
        
        print(f"\n✅ Found {len(leads)} upcoding research leads")
        return leads
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("For full data, download from:")
        print("https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners")
        return []


# ============================================
# 4. COMBINE WITH YOUR EXISTING SCRAPER
# ============================================

def combine_all_leads():
    """
    Combine leads from all sources
    """
    print("\n" + "="*60)
    print("4. Combining All Research Leads")
    print("="*60)
    
    all_leads = []
    
    # Load from all source files
    source_files = [
        "kickback_research_leads.json",
        "upcoding_research_leads.json",
        # Add your other scraped data here
        "fda_warning_letters_unfiled.json",
        "open_payments_kickback_potential.json",
        "cms_leie_recent_unfiled.json"
    ]
    
    for source_file in source_files:
        file_path = DATA_DIR / source_file
        if not file_path.exists():
            continue
        
        try:
            with open(file_path, 'r') as f:
                leads = json.load(f)
            
            all_leads.extend(leads if isinstance(leads, list) else [leads])
        except Exception as e:
            print(f"⚠️  Error loading {source_file}: {e}")
    
    # Sort by fraud score
    all_leads.sort(key=lambda x: x.get('fraud_score', 0) or x.get('fraud_potential_score', 0), reverse=True)
    
    # Format for ranker
    combined_records = []
    for lead in all_leads:
        # Format as text
        text_parts = []
        
        text_parts.append(f"SOURCE: {lead.get('source', 'Unknown')}")
        text_parts.append(f"LEAD TYPE: {lead.get('lead_type', 'General')}")
        text_parts.append(f"CASE STATUS: {lead.get('case_status', 'RESEARCH_LEAD')}")
        text_parts.append(f"FRAUD SCORE: {lead.get('fraud_score', 0) or lead.get('fraud_potential_score', 0)}")
        text_parts.append("")
        
        # Add research steps if available
        if 'research_steps' in lead and lead['research_steps']:
            text_parts.append("RESEARCH STEPS:")
            for step in lead['research_steps']:
                text_parts.append(f"  {step}")
            text_parts.append("")
        
        # Add all other fields
        for key, value in lead.items():
            if key not in ['id', 'source', 'lead_type', 'case_status', 'fraud_score', 'fraud_potential_score', 'research_steps', 'data_sources']:
                if isinstance(value, list):
                    if value:
                        text_parts.append(f"{key.upper().replace('_', ' ')}: {', '.join(str(v) for v in value[:10])}")
                elif isinstance(value, dict):
                    continue
                else:
                    text_parts.append(f"{key.upper().replace('_', ' ')}: {value}")
        
        # Add data sources
        if 'data_sources' in lead and lead['data_sources']:
            text_parts.append("")
            text_parts.append("DATA SOURCES FOR RESEARCH:")
            for source_name, url in lead['data_sources'].items():
                text_parts.append(f"  - {source_name}: {url}")
        
        combined_records.append({
            "source": lead.get('source'),
            "filename": lead.get('id', f"lead_{len(combined_records)}"),
            "text": "\n".join(text_parts),
            "metadata": lead
        })
    
    # Save as JSONL for ranker
    output_dir = DATA_DIR.parent / "processed"
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "combined_medical_fraud_data.jsonl"
    
    with open(output_file, 'w') as f:
        for record in combined_records:
            f.write(json.dumps(record) + '\n')
    
    print(f"✅ Combined {len(combined_records)} research leads")
    print(f"   Output: {output_file}")
    
    # Print summary
    print("\n" + "="*60)
    print("TOP 10 RESEARCH LEADS")
    print("="*60)
    
    for i, lead in enumerate(all_leads[:10], 1):
        score = lead.get('fraud_score', 0) or lead.get('fraud_potential_score', 0)
        lead_type = lead.get('lead_type', 'General')
        name = lead.get('provider_name') or lead.get('physician_name') or lead.get('company', 'Unknown')
        
        print(f"\n{i}. Score {score} | {lead_type}")
        print(f"   {name}")
        if 'research_steps' in lead and lead['research_steps']:
            print(f"   Next: {lead['research_steps'][0]}")
    
    return output_file


# ============================================
# MAIN
# ============================================

def main():
    print("\n" + "="*70)
    print(" ENHANCED QUI TAM RESEARCH LEAD SCRAPER")
    print(" Cross-references FDA/CMS with Medicare billing data")
    print("="*70)
    
    print("\n🎯 This gives you LEADS to research, not complete cases")
    print("You still need to:")
    print("  - PACER search (check if already filed)")
    print("  - Get insider documents")
    print("  - Calculate exact damages")
    print("  - Prove systematic pattern")
    print()
    
    # Run scrapers
    # download_medicare_part_d_data()  # Commented out - very large file
    # time.sleep(2)
    
    find_kickback_patterns()
    time.sleep(2)
    
    find_upcoding_outliers()
    time.sleep(2)
    
    # Combine all
    combined_file = combine_all_leads()
    
    print("\n🎉 DONE!")
    print(f"\n📁 Raw leads: {DATA_DIR}")
    print(f"📄 Combined JSONL: {combined_file}")
    
    print("\n" + "="*70)
    print("NEXT STEPS FOR EACH LEAD")
    print("="*70)
    print("\n1. PACER Search")
    print("   - https://pacer.uscourts.gov/")
    print("   - Search provider name + 'qui tam' or 'false claims'")
    print("   - Check if case already filed (even if sealed)")
    
    print("\n2. Download Full Medicare Data")
    print("   - Part D: https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers")
    print("   - Physician: https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners")
    print("   - Filter by NPI from leads")
    
    print("\n3. Calculate Damages")
    print("   - Compare provider billing to specialty average")
    print("   - Multiply excess by number of claims")
    print("   - Need >$500k for viable qui tam")
    
    print("\n4. Find Insider")
    print("   - Billing staff, nurses, former employees")
    print("   - Need documents: billing records, policies, emails")
    print("   - Qui tam requires 'original source' with direct knowledge")
    
    print("\n5. Hire Qui Tam Attorney")
    print("   - Only if: >$1M damages + strong documents + insider")
    print("   - Contingency fee (15-25% of recovery)")
    print("   - They'll do PACER check and full investigation")


if __name__ == "__main__":
    main()