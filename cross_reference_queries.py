#!/usr/bin/env python3
"""
Cross-Reference Queries - Find Red Flags by linking data across sources
"""

import sqlite3
from pathlib import Path

DB_PATH = Path("data/fraud_data.db")


def find_red_flags():
    """Run cross-reference queries to find potential fraud indicators."""
    if not DB_PATH.exists():
        print(f"❌ Database not found: {DB_PATH}")
        print("   Run etl_loader.py first to create the database.")
        return
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print("="*80)
    print("RED FLAG ANALYSIS - Cross-Reference Queries")
    print("="*80)
    print()
    
    # FLAG 1: Names in RetractionWatch AND NIH_Grants
    print("🔴 FLAG 1: Researchers with Retractions who received NIH Grants")
    print("-" * 80)
    cursor.execute("""
        SELECT DISTINCT
            n.pi_name,
            n.org_name,
            n.project_num,
            n.total_cost,
            r.title as retraction_title,
            r.journal as retraction_journal
        FROM nih_grants n
        JOIN retractions r ON n.pi_name_normalized LIKE '%' || LOWER(SUBSTR(r.title, 1, 20)) || '%'
           OR r.title LIKE '%' || n.pi_name || '%'
        WHERE n.pi_name IS NOT NULL AND n.pi_name != ''
        ORDER BY n.total_cost DESC
        LIMIT 20
    """)
    
    results = cursor.fetchall()
    if results:
        for row in results:
            print(f"  PI: {row['pi_name']}")
            print(f"  Organization: {row['org_name']}")
            print(f"  Grant: {row['project_num']} (${row['total_cost']:,.0f})")
            print(f"  Retraction: {row['retraction_title'][:80]}...")
            print(f"  Journal: {row['retraction_journal']}")
            print()
    else:
        print("  No matches found (may need better name matching)")
    print()
    
    # FLAG 2: Doctors in CMS OpenPayments with high payments AND PubMed articles
    print("🔴 FLAG 2: Doctors with >$50k payments who have PubMed articles")
    print("-" * 80)
    cursor.execute("""
        SELECT DISTINCT
            c.physician_first_name || ' ' || c.physician_last_name as doctor_name,
            SUM(c.payment_amount) as total_payments,
            COUNT(DISTINCT c.record_id) as payment_count,
            COUNT(DISTINCT p.pmid) as pubmed_count
        FROM cms_openpayments c
        LEFT JOIN pubmed_articles p ON 
            LOWER(p.text_content) LIKE '%' || LOWER(c.physician_first_name) || '%' ||
            LOWER(p.text_content) LIKE '%' || LOWER(c.physician_last_name) || '%'
        WHERE c.payment_amount > 50000
        GROUP BY doctor_name
        HAVING pubmed_count > 0
        ORDER BY total_payments DESC
        LIMIT 20
    """)
    
    results = cursor.fetchall()
    if results:
        for row in results:
            print(f"  Doctor: {row['doctor_name']}")
            print(f"  Total Payments: ${row['total_payments']:,.2f} ({row['payment_count']} payments)")
            print(f"  PubMed Articles: {row['pubmed_count']}")
            print()
    else:
        print("  No matches found")
    print()
    
    # FLAG 3: NIH Grant recipients with PubPeer comments
    print("🔴 FLAG 3: NIH Grant recipients with PubPeer discussions")
    print("-" * 80)
    cursor.execute("""
        SELECT DISTINCT
            n.pi_name,
            n.org_name,
            n.project_num,
            n.total_cost,
            COUNT(DISTINCT pp.id) as pubpeer_count
        FROM nih_grants n
        JOIN pubpeer_articles pp ON 
            LOWER(pp.text_content) LIKE '%' || LOWER(n.pi_name) || '%' OR
            LOWER(pp.text_content) LIKE '%' || LOWER(n.org_name) || '%'
        WHERE n.pi_name IS NOT NULL
        GROUP BY n.pi_name, n.org_name, n.project_num, n.total_cost
        ORDER BY n.total_cost DESC
        LIMIT 20
    """)
    
    results = cursor.fetchall()
    if results:
        for row in results:
            print(f"  PI: {row['pi_name']}")
            print(f"  Organization: {row['org_name']}")
            print(f"  Grant: {row['project_num']} (${row['total_cost']:,.0f})")
            print(f"  PubPeer Discussions: {row['pubpeer_count']}")
            print()
    else:
        print("  No matches found")
    print()
    
    # FLAG 4: Clinical Trial PIs with FDA Adverse Events
    print("🔴 FLAG 4: Clinical Trial PIs with related FDA Adverse Events")
    print("-" * 80)
    cursor.execute("""
        SELECT DISTINCT
            ct.principal_investigator,
            ct.nct_id,
            ct.title,
            COUNT(DISTINCT f.id) as adverse_event_count
        FROM clinical_trials ct
        JOIN fda_faers f ON 
            LOWER(f.text_content) LIKE '%' || LOWER(ct.principal_investigator) || '%' OR
            LOWER(f.text_content) LIKE '%' || LOWER(SUBSTR(ct.title, 1, 30)) || '%'
        WHERE ct.principal_investigator IS NOT NULL AND ct.principal_investigator != ''
        GROUP BY ct.principal_investigator, ct.nct_id, ct.title
        ORDER BY adverse_event_count DESC
        LIMIT 20
    """)
    
    results = cursor.fetchall()
    if results:
        for row in results:
            print(f"  PI: {row['principal_investigator']}")
            print(f"  Trial: {row['nct_id']}")
            print(f"  Title: {row['title'][:80]}...")
            print(f"  Related Adverse Events: {row['adverse_event_count']}")
            print()
    else:
        print("  No matches found")
    print()
    
    conn.close()
    print("="*80)
    print("✅ Analysis Complete")
    print("\nNote: These queries use text matching. For production, consider:")
    print("  - More sophisticated name normalization")
    print("  - Author extraction from PubMed abstracts")
    print("  - DOI/PMID linking for exact matches")


if __name__ == "__main__":
    find_red_flags()
