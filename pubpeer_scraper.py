#!/usr/bin/env python3
"""
PubPeer Scraper - Scrapes all articles and comments from PubPeer.com
Handles JavaScript "Load More" button to get all articles
"""

import json
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse
import re

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from bs4 import BeautifulSoup
except ImportError:
    print("Error: Required packages not installed. Install with:", file=sys.stderr)
    print("  pip install selenium beautifulsoup4", file=sys.stderr)
    sys.exit(1)

DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://pubpeer.com/"
MAX_TEXT_LENGTH = 3000  # Keep text chunks manageable


def setup_driver():
    """Set up Selenium WebDriver with Chrome."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        print(f"Error setting up Chrome driver: {e}", file=sys.stderr)
        print("Make sure ChromeDriver is installed and in PATH", file=sys.stderr)
        sys.exit(1)


def load_all_articles(driver, base_url):
    """Load all articles by clicking 'Load More' button until no more articles."""
    print(f"Loading PubPeer homepage: {base_url}")
    driver.get(base_url)
    time.sleep(3)  # Wait for initial page load
    
    articles_loaded = set()
    load_more_clicked = 0
    max_clicks = 1000  # Safety limit
    
    while load_more_clicked < max_clicks:
        # Parse current page to find article links
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        current_articles = set()
        
        # Find all article links matching pattern /publications/[alphanumeric]
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if '/publications/' in href:
                # Extract the publication ID
                match = re.search(r'/publications/([A-Z0-9]+)', href)
                if match:
                    pub_id = match.group(1)
                    full_url = urljoin(base_url, href)
                    current_articles.add((pub_id, full_url))
        
        new_articles = current_articles - articles_loaded
        if new_articles:
            articles_loaded.update(current_articles)
            print(f"  Found {len(new_articles)} new articles (total: {len(articles_loaded)})")
        
        # Try to find and click "Load More" button
        try:
            # Look for common "Load More" button patterns
            load_more = None
            selectors = [
                "button:contains('Load More')",
                "a:contains('Load More')",
                "button[class*='load']",
                "button[class*='more']",
                "a[class*='load']",
                "a[class*='more']",
                "//button[contains(text(), 'Load More')]",
                "//a[contains(text(), 'Load More')]",
            ]
            
            for selector in selectors:
                try:
                    if selector.startswith("//"):
                        # XPath
                        load_more = driver.find_element(By.XPATH, selector)
                    else:
                        # CSS selector
                        load_more = driver.find_element(By.CSS_SELECTOR, selector)
                    if load_more and load_more.is_displayed():
                        break
                except NoSuchElementException:
                    continue
            
            if load_more and load_more.is_displayed():
                # Scroll to button
                driver.execute_script("arguments[0].scrollIntoView(true);", load_more)
                time.sleep(0.5)
                load_more.click()
                load_more_clicked += 1
                print(f"  Clicked 'Load More' ({load_more_clicked})...")
                time.sleep(2)  # Wait for new content to load
            else:
                # Try JavaScript approach - look for load more function
                try:
                    driver.execute_script("""
                        var buttons = document.querySelectorAll('button, a');
                        for (var i = 0; i < buttons.length; i++) {
                            var text = buttons[i].textContent || buttons[i].innerText || '';
                            if (text.toLowerCase().includes('load more') || 
                                text.toLowerCase().includes('more')) {
                                buttons[i].click();
                                return true;
                            }
                        }
                        return false;
                    """)
                    time.sleep(2)
                    load_more_clicked += 1
                    print(f"  Clicked 'Load More' via JavaScript ({load_more_clicked})...")
                except:
                    # No more "Load More" button found
                    print("  No more 'Load More' button found. All articles loaded.")
                    break
        except Exception as e:
            print(f"  Could not find/click 'Load More' button: {e}")
            # Check if we got new articles in this iteration
            if not new_articles:
                print("  No new articles found. Stopping.")
                break
    
    print(f"\nTotal articles found: {len(articles_loaded)}")
    return list(articles_loaded)


def scrape_article(driver, article_url, pub_id):
    """Scrape a single PubPeer article page including comments."""
    try:
        print(f"  Scraping: {article_url}")
        driver.get(article_url)
        time.sleep(2)  # Wait for page to load
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Remove script and style elements
        for elem in soup(["script", "style"]):
            elem.decompose()
        
        # Extract article title
        title = ""
        title_elem = soup.find('h1') or soup.find('title')
        if title_elem:
            title = title_elem.get_text(strip=True)
        
        # Extract article content/abstract
        content = ""
        # Look for common content containers
        content_selectors = [
            'div[class*="abstract"]',
            'div[class*="content"]',
            'div[class*="article"]',
            'div[class*="publication"]',
            'p',
        ]
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                content = content_elem.get_text(separator=' ', strip=True)
                if len(content) > 100:  # Only use if substantial content
                    break
        
        # Extract all comments
        comments = []
        # Look for comment containers
        comment_selectors = [
            'div[class*="comment"]',
            'div[class*="discussion"]',
            'div[class*="thread"]',
            'li[class*="comment"]',
        ]
        for selector in comment_selectors:
            comment_elems = soup.select(selector)
            for comment_elem in comment_elems:
                comment_text = comment_elem.get_text(separator=' ', strip=True)
                if comment_text and len(comment_text) > 10:
                    comments.append(comment_text)
        
        # If no comments found with selectors, try to find any text blocks that might be comments
        if not comments:
            # Look for divs with user-related classes or timestamps
            all_divs = soup.find_all('div')
            for div in all_divs:
                div_text = div.get_text(strip=True)
                # Heuristic: comments are usually shorter blocks of text
                if 20 < len(div_text) < 5000:
                    # Check if it looks like a comment (has some structure)
                    if any(keyword in div_text.lower() for keyword in ['said', 'wrote', 'note', 'issue', 'problem', 'concern']):
                        comments.append(div_text)
        
        # Build full text
        text_parts = []
        if title:
            text_parts.append(f"Title: {title}")
        if content:
            text_parts.append(f"\nContent:\n{content}")
        if comments:
            text_parts.append(f"\n\nComments ({len(comments)}):")
            for i, comment in enumerate(comments[:10], 1):  # Limit to first 10 comments
                text_parts.append(f"\nComment {i}:\n{comment}")
        
        full_text = "\n".join(text_parts)
        
        # Truncate if needed
        if len(full_text) > MAX_TEXT_LENGTH:
            full_text = full_text[:MAX_TEXT_LENGTH]
            last_period = full_text.rfind('.')
            if last_period > MAX_TEXT_LENGTH * 0.8:
                full_text = full_text[:last_period + 1]
            full_text += f"\n\n[Text truncated - showing first {MAX_TEXT_LENGTH} characters]"
        
        filename = f"pubpeer_{pub_id}.html"
        
        return {
            'filename': filename,
            'text': f"URL: {article_url}\n\n{full_text}",
            'url': article_url,
            'title': title,
            'pub_id': pub_id,
            'comment_count': len(comments)
        }
    except Exception as e:
        print(f"  Error scraping article {article_url}: {e}", file=sys.stderr)
        return None


def main():
    print("PubPeer Scraper")
    print(f"Target: {BASE_URL}")
    print("\nStarting scrape...")
    print("Note: This uses Selenium to handle JavaScript 'Load More' button")
    
    driver = None
    try:
        driver = setup_driver()
        
        # Step 1: Load all articles by clicking "Load More"
        articles = load_all_articles(driver, BASE_URL)
        
        if not articles:
            print("\n❌ No articles found", file=sys.stderr)
            sys.exit(1)
        
        # Step 2: Scrape each article
        print(f"\nScraping {len(articles)} articles...")
        records = []
        
        for i, (pub_id, article_url) in enumerate(articles, 1):
            if i % 10 == 0:
                print(f"  Progress: {i}/{len(articles)} articles...")
            
            record = scrape_article(driver, article_url, pub_id)
            if record:
                records.append(record)
            
            time.sleep(1)  # Be polite between requests
        
        if not records:
            print("\n❌ No articles scraped", file=sys.stderr)
            sys.exit(1)
        
        # Generate output filename
        import time as time_module
        timestamp = int(time_module.time())
        output_file = DATA_DIR / f"pubpeer_{timestamp}.jsonl"
        
        # Write JSONL file
        print(f"\nWriting {len(records)} article records to {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        print(f"✅ Scraped {len(records)} articles from PubPeer")
        print(f"   Output: {output_file}")
        if output_file.exists():
            size_mb = output_file.stat().st_size / (1024 * 1024)
            print(f"   File size: {size_mb:.2f} MB")
        print(f"\nNext step: Run 'Combine Website Scrapes' or 'Run Converter' to process this file")
        
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    main()
