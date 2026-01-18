#!/usr/bin/env python3
"""
Website Scraper - Scrapes a given URL and outputs JSONL format for converter
"""

import requests
from bs4 import BeautifulSoup
import json
import sys
from pathlib import Path
from urllib.parse import urlparse, urljoin
import time

DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}


def scrape_website(url: str, max_pages: int = 1, link_selector: str = None, url_pattern: str = None) -> list:
    """
    Scrape a website and extract text content.
    
    Args:
        url: The URL to scrape
        max_pages: Maximum number of pages to scrape (for following links)
        link_selector: CSS selector for article links (e.g., "a.article-link")
        url_pattern: URL pattern to match (e.g., "/pubmed/", "/article/")
    
    Returns:
        List of records with filename and text
    """
    records = []
    visited = set()
    
    def get_page_content(page_url: str) -> dict:
        """Get content from a single page."""
        try:
            print(f"Fetching: {page_url}")
            response = requests.get(page_url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style", "nav", "header", "footer"]):
                script.decompose()
            
            # Get title
            title = soup.find('title')
            title_text = title.get_text(strip=True) if title else "Untitled"
            
            # Get main content
            # Try to find main content area
            main_content = (
                soup.find('main') or 
                soup.find('article') or 
                soup.find('div', class_=lambda x: x and ('content' in x.lower() or 'main' in x.lower())) or
                soup.find('body')
            )
            
            if main_content:
                text = main_content.get_text(separator='\n', strip=True)
            else:
                text = soup.get_text(separator='\n', strip=True)
            
            # Clean up text
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            text = '\n'.join(lines)
            
            # Truncate text to fit in model context window
            # Reserve space for prompt (~2000 chars) and keep text under 3000 chars for safety
            MAX_TEXT_LENGTH = 3000
            if len(text) > MAX_TEXT_LENGTH:
                # Truncate but try to preserve structure - keep first part
                text = text[:MAX_TEXT_LENGTH]
                # Try to cut at a sentence boundary
                last_period = text.rfind('.')
                last_newline = text.rfind('\n')
                cut_point = max(last_period, last_newline)
                if cut_point > MAX_TEXT_LENGTH * 0.8:  # Only if we found a good break point
                    text = text[:cut_point + 1]
                text += f"\n\n[Text truncated from original - showing first {MAX_TEXT_LENGTH} characters]"
            
            # Generate filename from URL
            parsed = urlparse(page_url)
            filename = parsed.path.strip('/').replace('/', '_') or 'index'
            if not filename.endswith('.html'):
                filename = filename + '.html'
            filename = filename[:100]  # Limit length
            
            return {
                'filename': filename,
                'text': f"{title_text}\n\nURL: {page_url}\n\n{text}",
                'url': page_url,
                'title': title_text
            }
        except Exception as e:
            print(f"Error scraping {page_url}: {e}", file=sys.stderr)
            return None
    
    # Scrape main page
    main_record = get_page_content(url)
    if main_record:
        records.append(main_record)
        visited.add(url)
    
    # Optionally follow links to articles
    if max_pages > 1:
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            base_url = urlparse(url)
            article_links = []
            
            # Find article links using selector or pattern
            if link_selector:
                # Use CSS selector
                links = soup.select(link_selector)
                for link in links:
                    href = link.get('href')
                    if href:
                        full_url = urljoin(url, href)
                        article_links.append(full_url)
            elif url_pattern:
                # Match URL pattern
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href')
                    if href and url_pattern in href:
                        full_url = urljoin(url, href)
                        article_links.append(full_url)
            else:
                # Smart detection: look for common article link patterns
                links = soup.find_all('a', href=True)
                
                # Common patterns for article pages
                article_patterns = [
                    '/pubmed/',  # PubMed
                    '/article/',  # Generic articles
                    '/post/',  # Blog posts
                    '/entry/',  # Entries
                    '/news/',  # News articles
                    '/story/',  # Stories
                    '/paper/',  # Papers
                    '/publication/',  # Publications
                ]
                
                # Also look for links in article containers
                article_containers = soup.find_all(['article', 'div'], class_=lambda x: x and any(
                    keyword in str(x).lower() for keyword in ['article', 'post', 'entry', 'item', 'result']
                ))
                
                for link in links:
                    href = link.get('href', '')
                    if not href:
                        continue
                    
                    full_url = urljoin(url, href)
                    parsed = urlparse(full_url)
                    
                    # Check if it matches article patterns
                    is_article_link = any(pattern in full_url for pattern in article_patterns)
                    
                    # Check if link is in an article container
                    if not is_article_link:
                        parent = link.find_parent(['article', 'div'])
                        if parent and any(keyword in str(parent.get('class', [])).lower() 
                                        for keyword in ['article', 'post', 'entry', 'item', 'result']):
                            is_article_link = True
                    
                    # Check if URL looks like an article (has ID or slug)
                    if not is_article_link:
                        path_parts = parsed.path.strip('/').split('/')
                        # URLs with IDs or slugs (not just index pages)
                        if len(path_parts) >= 2 or (len(path_parts) == 1 and path_parts[0] and 
                                                    path_parts[0] not in ['', 'index', 'home', 'list']):
                            # Avoid navigation, footer, header links
                            link_text = link.get_text(strip=True).lower()
                            if link_text and len(link_text) > 10:  # Substantial link text
                                is_article_link = True
                    
                    if is_article_link and parsed.netloc == base_url.netloc:
                        article_links.append(full_url)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_links = []
            for link in article_links:
                if link not in seen and link not in visited:
                    seen.add(link)
                    unique_links.append(link)
            
            # Limit to max_pages
            links_to_follow = unique_links[:max_pages - 1]
            
            print(f"Found {len(unique_links)} article links, following {len(links_to_follow)}...")
            
            for i, full_url in enumerate(links_to_follow, 1):
                if full_url in visited:
                    continue
                visited.add(full_url)
                print(f"  [{i}/{len(links_to_follow)}] Scraping article: {full_url}")
                time.sleep(1)  # Be polite
                record = get_page_content(full_url)
                if record:
                    records.append(record)
        except Exception as e:
            print(f"Error following links: {e}", file=sys.stderr)
    
    return records


def main():
    if len(sys.argv) < 2:
        print("Usage: website_scraper.py <url> [max_pages] [link_selector] [url_pattern]")
        print("  link_selector: CSS selector for article links (e.g., 'a.article-link')")
        print("  url_pattern: URL pattern to match (e.g., '/pubmed/')")
        sys.exit(1)
    
    url = sys.argv[1]
    max_pages = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    link_selector = sys.argv[3] if len(sys.argv) > 3 else None
    url_pattern = sys.argv[4] if len(sys.argv) > 4 else None
    
    print(f"Scraping website: {url}")
    print(f"Max pages: {max_pages}")
    if link_selector:
        print(f"Link selector: {link_selector}")
    if url_pattern:
        print(f"URL pattern: {url_pattern}")
    
    records = scrape_website(url, max_pages, link_selector, url_pattern)
    
    if not records:
        print("No content scraped", file=sys.stderr)
        sys.exit(1)
    
    # Generate output filename from URL
    parsed = urlparse(url)
    domain = parsed.netloc.replace('.', '_').replace(':', '_')
    timestamp = int(time.time())
    output_file = DATA_DIR / f"website_scrape_{domain}_{timestamp}.jsonl"
    
    # Write JSONL file
    print(f"\nWriting {len(records)} records to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
    
    print(f"✅ Scraped {len(records)} page(s) from {url}")
    print(f"   Output: {output_file}")
    print(f"\nNext step: Run converter to process this file")


if __name__ == "__main__":
    main()
