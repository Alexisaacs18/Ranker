#!/usr/bin/env python3
"""
PubMed Trending Scraper - Scrapes all trending articles from PubMed
Extracts PMIDs from listing pages and scrapes individual article pages
"""

import requests
from bs4 import BeautifulSoup
import json
import sys
from pathlib import Path
import time
import re

DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

MAX_TEXT_LENGTH = 3000  # Keep text chunks manageable


def extract_pmids_from_listing_page(soup) -> list:
    """Extract all PMIDs from a PubMed listing page."""
    pmids = set()
    
    # Method 1: Extract from saved search terms (comma-separated PMIDs in the page)
    # The saved search contains all PMIDs like "41535475,41519150,4240108,..."
    page_text = str(soup)
    
    # Look for the saved search terms pattern - long comma-separated PMIDs
    # Pattern: "Search terms: 41535475,41519150,4240108,..."
    search_terms_match = re.search(r'Search terms:\s*([\d,]+)', page_text)
    if search_terms_match:
        pmids_str = search_terms_match.group(1)
        # Split by comma and extract PMIDs
        for pmid in pmids_str.split(','):
            pmid = pmid.strip()
            if pmid.isdigit() and 8 <= len(pmid) <= 12:
                pmids.add(pmid)
    
    # Method 2: Extract PMIDs from links on the page
    links = soup.find_all('a', href=True)
    for link in links:
        href = link.get('href', '')
        # Look for /pubmed/{PMID}/ pattern
        pmid_match = re.search(r'/pubmed/(\d{8,})', href)
        if pmid_match:
            pmid = pmid_match.group(1)
            if pmid.isdigit() and 8 <= len(pmid) <= 12:
                pmids.add(pmid)
    
    # Method 3: Extract from any PMID references in text
    # Pattern: PMID: 41545779 or just 8+ digit numbers in certain contexts
    pmid_refs = re.findall(r'\b(\d{8,})\b', page_text)
    for pmid in pmid_refs:
        if 8 <= len(pmid) <= 12:  # PMIDs are usually 8 digits, sometimes more
            pmids.add(pmid)
    
    return sorted(list(pmids), reverse=True)  # Most recent first


def scrape_pubmed_article(pubmed_url: str) -> dict:
    """Scrape a single PubMed article page, focusing on fraud-relevant content."""
    try:
        print(f"  Fetching: {pubmed_url}")
        response = requests.get(pubmed_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Remove unnecessary elements
        for elem in soup(["script", "style", "nav", "header", "footer", "aside"]):
            elem.decompose()
        
        # Extract PMID from URL
        pmid_match = re.search(r'/(\d{8,})/', pubmed_url)
        pmid = pmid_match.group(1) if pmid_match else ""
        
        # Extract title - focus on main heading
        title_elem = soup.find('h1', class_='heading-title') or soup.find('h1') or soup.find('div', class_='abstract-title')
        title = ""
        if title_elem:
            title = title_elem.get_text(strip=True)
        # Also try finding in citation
        if not title:
            citation_elem = soup.find('div', class_='citation') or soup.find('span', class_='docsum-title')
            if citation_elem:
                title = citation_elem.get_text(strip=True)
        
        # Extract abstract - most important content
        abstract_elem = (
            soup.find('div', class_='abstract-content') or
            soup.find('div', {'id': 'abstract'}) or
            soup.find('div', class_='abstract') or
            soup.find('section', class_='abstract')
        )
        abstract = ""
        if abstract_elem:
            # Get all paragraphs in abstract
            abstract_paras = abstract_elem.find_all('p', class_=re.compile('abstract|paragraph'))
            if abstract_paras:
                abstract = '\n'.join([p.get_text(strip=True) for p in abstract_paras])
            else:
                abstract = abstract_elem.get_text(separator=' ', strip=True)
        
        # Extract journal and publication info
        journal_elem = soup.find('button', class_='journal-actions-trigger') or soup.find('span', class_='journal')
        journal = journal_elem.get_text(strip=True) if journal_elem else ""
        
        date_elem = soup.find('span', class_='cit') or soup.find('span', class_='citation-date')
        pub_date = date_elem.get_text(strip=True) if date_elem else ""
        
        # Extract first few authors
        authors = []
        author_section = soup.find('div', class_='authors') or soup.find('div', class_='authors-list')
        if author_section:
            author_elems = author_section.find_all(['a', 'span'], class_=re.compile('author|name'))
            for auth in author_elems[:5]:  # Limit to first 5
                author_text = auth.get_text(strip=True)
                if author_text and len(author_text) > 2:
                    authors.append(author_text)
        
        # Build focused text - prioritize abstract and key metadata
        text_parts = []
        
        if title:
            text_parts.append(f"Title: {title}")
        
        if pmid:
            text_parts.append(f"PMID: {pmid}")
        
        if journal:
            text_parts.append(f"Journal: {journal}")
        
        if pub_date:
            text_parts.append(f"Publication Date: {pub_date}")
        
        if authors:
            text_parts.append(f"Authors: {', '.join(authors)}")
        
        if abstract:
            text_parts.append(f"\nAbstract:\n{abstract}")
        else:
            # If no abstract, try to get summary or first paragraph
            summary_elem = soup.find('div', class_=re.compile('summary|content'))
            if summary_elem:
                text_parts.append(f"\nSummary:\n{summary_elem.get_text(separator=' ', strip=True)}")
        
        full_text = "\n\n".join(text_parts)
        
        # Truncate if needed
        if len(full_text) > MAX_TEXT_LENGTH:
            full_text = full_text[:MAX_TEXT_LENGTH]
            last_period = full_text.rfind('.')
            if last_period > MAX_TEXT_LENGTH * 0.8:
                full_text = full_text[:last_period + 1]
            full_text += f"\n\n[Text truncated - showing first {MAX_TEXT_LENGTH} characters]"
        
        # Generate filename from PMID
        filename = f"pubmed_{pmid}.html" if pmid else pubmed_url.split('/')[-1] + ".html"
        filename = filename[:100]
        
        return {
            'filename': filename,
            'text': f"URL: {pubmed_url}\n\n{full_text}",
            'url': pubmed_url,
            'title': title,
            'pmid': pmid,
            'journal': journal
        }
    except Exception as e:
        print(f"  Error scraping article {pubmed_url}: {e}", file=sys.stderr)
        return None


def scrape_pubmed_trending(base_url: str = "https://pubmed.ncbi.nlm.nih.gov/trending/?sort=date", max_pages: int = 100) -> list:
    """
    Scrape all trending articles from PubMed trending pages.
    
    Iterates through listing pages (1-100) and extracts PMIDs from each,
    then scrapes individual article pages.
    
    Args:
        base_url: The base PubMed trending page URL
        max_pages: Maximum number of listing pages to process (default 100)
    
    Returns:
        List of article records
    """
    all_pmids = set()
    all_records = []
    
    print(f"Step 1: Extracting PMIDs from {max_pages} listing pages...")
    
    # Step 1: Collect all PMIDs from listing pages
    for page_num in range(1, max_pages + 1):
        if page_num == 1:
            listing_url = base_url
        else:
            listing_url = f"{base_url}&page={page_num}"
        
        try:
            print(f"  Page {page_num}/{max_pages}: Fetching listing page...")
            response = requests.get(listing_url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract PMIDs from this page
            pmids = extract_pmids_from_listing_page(soup)
            
            if pmids:
                for pmid in pmids:
                    all_pmids.add(pmid)
                print(f"    Found {len(pmids)} PMIDs (total so far: {len(all_pmids)})")
            else:
                print(f"    No PMIDs found on page {page_num}")
                if page_num > 5:  # If we hit several empty pages, stop
                    break
            
            time.sleep(0.5)  # Be polite between page requests
            
        except Exception as e:
            print(f"    Error on page {page_num}: {e}", file=sys.stderr)
            continue
    
    if not all_pmids:
        print("\n❌ No PMIDs found on any listing page")
        return []
    
    print(f"\nStep 2: Scraping {len(all_pmids)} individual articles...")
    
    # Step 2: Scrape each article page
    pmid_list = sorted(list(all_pmids), reverse=True)  # Most recent first
    
    for i, pmid in enumerate(pmid_list, 1):
        article_url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
        
        if i % 10 == 0:
            print(f"  Progress: {i}/{len(pmid_list)} articles...")
        
        record = scrape_pubmed_article(article_url)
        if record:
            all_records.append(record)
        
        time.sleep(1)  # Be polite - 1 second between article requests
    
    return all_records


def main():
    base_url = "https://pubmed.ncbi.nlm.nih.gov/trending/?sort=date"
    max_pages = 100  # 100 pages * 10 articles = 1000 articles
    
    print(f"PubMed Trending Scraper")
    print(f"Target: {base_url}")
    print(f"Will process up to {max_pages} listing pages (up to 1000 articles)")
    print(f"\nStarting scrape...")
    
    records = scrape_pubmed_trending(base_url, max_pages)
    
    if not records:
        print("\n❌ No articles scraped", file=sys.stderr)
        sys.exit(1)
    
    # Generate output filename
    timestamp = int(time.time())
    output_file = DATA_DIR / f"pubmed_trending_{timestamp}.jsonl"
    
    # Write JSONL file
    print(f"\nWriting {len(records)} article records to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
    
    print(f"✅ Scraped {len(records)} articles from PubMed trending")
    print(f"   Output: {output_file}")
    if output_file.exists():
        size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"   File size: {size_mb:.2f} MB")
    print(f"\nNext step: Run 'Combine Website Scrapes' or 'Run Converter' to process this file")


if __name__ == "__main__":
    main()
