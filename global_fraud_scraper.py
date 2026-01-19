#!/usr/bin/env python3
"""
Global Fraud Scraper - Scrapes 7 different government datasets
Uses async/await for parallel processing and API keys for faster access
"""

import asyncio
import aiohttp
import json
import os
import sys
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

# Import config (will fail gracefully if not present)
try:
    import config
    NCBI_API_KEY = config.NCBI_API_KEY if hasattr(config, 'NCBI_API_KEY') else None
    OPENFDA_API_KEY = config.OPENFDA_API_KEY if hasattr(config, 'OPENFDA_API_KEY') else None
    MY_EMAIL = config.MY_EMAIL if hasattr(config, 'MY_EMAIL') else "fraud.scraper@example.com"
    RATE_LIMIT = config.RATE_LIMIT if hasattr(config, 'RATE_LIMIT') else 1.0
except ImportError:
    print("Warning: config.py not found. Using defaults. Create config.py with your API keys.", file=sys.stderr)
    NCBI_API_KEY = None
    OPENFDA_API_KEY = None
    MY_EMAIL = "fraud.scraper@example.com"
    RATE_LIMIT = 1.0

# Setup output directory
DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ==========================================
# 0. SETUP & LOGGING
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("FraudScraper")

# ==========================================
# 1. BASE CLASS (The Blueprint)
# ==========================================
class DataSource(ABC):
    def __init__(self, name, base_url):
        self.name = name
        self.base_url = base_url
        self.session = None
        self.rate_limit = RATE_LIMIT
        self._existing_records = None  # Cache for duplicate checking

    def _load_existing_records(self):
        """Load existing record IDs to avoid duplicates."""
        if self._existing_records is not None:
            return self._existing_records
        
        existing = set()
        filename = DATA_DIR / f"data_{self.name}.jsonl"
        if filename.exists():
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                            # Extract unique identifier based on record type
                            if 'pmid' in record and record.get('pmid'):
                                existing.add(('pmid', record['pmid']))
                            elif 'doi' in record and record.get('doi'):
                                existing.add(('doi', record['doi']))
                            elif 'nct_id' in record and record.get('nct_id'):
                                existing.add(('nct_id', record['nct_id']))
                            elif 'project_num' in record and record.get('project_num'):
                                existing.add(('project_num', record['project_num']))
                            elif 'report_id' in record and record.get('report_id'):
                                existing.add(('report_id', record['report_id']))
                            elif 'filename' in record and record.get('filename'):
                                existing.add(('filename', record['filename']))
                        except (json.JSONDecodeError, KeyError):
                            continue
            except Exception as e:
                logger.warning(f"[{self.name}] Error loading existing records: {e}")
        
        self._existing_records = existing
        return existing

    def _is_duplicate(self, record) -> bool:
        """Check if record already exists."""
        existing = self._load_existing_records()
        
        # Check for unique identifiers
        if 'pmid' in record and record.get('pmid'):
            return ('pmid', record['pmid']) in existing
        elif 'doi' in record and record.get('doi'):
            return ('doi', record['doi']) in existing
        elif 'nct_id' in record and record.get('nct_id'):
            return ('nct_id', record['nct_id']) in existing
        elif 'project_num' in record and record.get('project_num'):
            return ('project_num', record['project_num']) in existing
        elif 'report_id' in record and record.get('report_id'):
            return ('report_id', record['report_id']) in existing
        elif 'filename' in record and record.get('filename'):
            return ('filename', record['filename']) in existing
        
        return False

    def save_record(self, record):
        """Appends a single record to a JSONL file (crash-safe storage), skipping duplicates."""
        # Skip if duplicate
        if self._is_duplicate(record):
            return False
        
        filename = DATA_DIR / f"data_{self.name}.jsonl"
        # Add a timestamp to every record we save
        record['scraped_at'] = datetime.now(timezone.utc).isoformat()
        record['source'] = self.name
        
        with open(filename, "a", encoding='utf-8') as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        
        # Update cache
        if self._existing_records is not None:
            if 'pmid' in record and record.get('pmid'):
                self._existing_records.add(('pmid', record['pmid']))
            elif 'doi' in record and record.get('doi'):
                self._existing_records.add(('doi', record['doi']))
            elif 'nct_id' in record and record.get('nct_id'):
                self._existing_records.add(('nct_id', record['nct_id']))
            elif 'project_num' in record and record.get('project_num'):
                self._existing_records.add(('project_num', record['project_num']))
            elif 'report_id' in record and record.get('report_id'):
                self._existing_records.add(('report_id', record['report_id']))
            elif 'filename' in record and record.get('filename'):
                self._existing_records.add(('filename', record['filename']))
        
        return True

    async def fetch_json(self, url, params=None, headers=None, method="GET", payload=None, as_text=False):
        """Robust fetcher that handles Retries and Rate Limits.
        
        Args:
            as_text: If True, return text content instead of parsing as JSON (for XML)
        """
        tries = 0
        while tries < 3:
            try:
                if method == "GET":
                    async with self.session.get(url, params=params, headers=headers) as response:
                        if response.status == 200:
                            if as_text:
                                return await response.text()
                            return await response.json()
                        elif response.status == 429:
                            logger.warning(f"[{self.name}] Rate Limited (429). Sleeping 10s...")
                            await asyncio.sleep(10)
                        else:
                            logger.error(f"[{self.name}] Error {response.status}: {url}")
                            return None
                elif method == "POST":
                    async with self.session.post(url, json=payload, headers=headers) as response:
                        if response.status == 200:
                            if as_text:
                                return await response.text()
                            return await response.json()
                        else:
                            logger.error(f"[{self.name}] Error {response.status}")
                            return None
                
            except Exception as e:
                logger.error(f"[{self.name}] Network Exception: {e}")
            
            tries += 1
            await asyncio.sleep(2) # Backoff before retry
        return None

    @abstractmethod
    async def run(self):
        pass

# ==========================================
# 2. THE API STREAMERS
# ==========================================

class PubMedScraper(DataSource):
    """Target: Scientific Articles (NCBI) using your API Key - Fetches full article details"""
    def __init__(self):
        super().__init__("PubMed_US", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi")
        self.efetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

    async def fetch_article_details(self, pmid: str):
        """Fetch full article details using efetch API."""
        params = {
            "db": "pubmed",
            "id": pmid,
            "retmode": "xml",
        }
        if NCBI_API_KEY:
            params["api_key"] = NCBI_API_KEY
        
        # Fetch XML as text (not JSON)
        xml_data = await self.fetch_json(self.efetch_url, params=params, as_text=True)
        if not xml_data or not isinstance(xml_data, str):
            return None
        
        # Parse XML to extract article details
        try:
            import xml.etree.ElementTree as ET
            
            # Parse XML string
            root = ET.fromstring(xml_data)
            
            # Extract title
            title = ""
            title_elem = root.find(".//Article/ArticleTitle")
            if title_elem is not None:
                title = "".join(title_elem.itertext()).strip()
            
            # Extract abstract
            abstract = ""
            abstract_elem = root.find(".//Abstract")
            if abstract_elem is not None:
                abstract_texts = []
                for abs_text in abstract_elem.findall(".//AbstractText"):
                    label = abs_text.get("Label", "")
                    text = "".join(abs_text.itertext()).strip()
                    if label:
                        abstract_texts.append(f"{label}: {text}")
                    else:
                        abstract_texts.append(text)
                abstract = "\n".join(abstract_texts)
            
            # Extract authors
            authors = []
            for author in root.findall(".//AuthorList/Author"):
                last_name = ""
                fore_name = ""
                last_elem = author.find("LastName")
                fore_elem = author.find("ForeName")
                if last_elem is not None:
                    last_name = last_elem.text or ""
                if fore_elem is not None:
                    fore_name = fore_elem.text or ""
                if last_name:
                    author_name = f"{fore_name} {last_name}".strip() if fore_name else last_name
                    authors.append(author_name)
            
            # Extract journal
            journal = ""
            journal_elem = root.find(".//Journal/Title")
            if journal_elem is not None:
                journal = journal_elem.text or ""
            
            # Extract publication date
            pub_date = ""
            pub_date_elem = root.find(".//PubDate")
            if pub_date_elem is not None:
                year_elem = pub_date_elem.find("Year")
                month_elem = pub_date_elem.find("Month")
                day_elem = pub_date_elem.find("Day")
                date_parts = []
                if year_elem is not None:
                    date_parts.append(year_elem.text or "")
                if month_elem is not None:
                    date_parts.append(month_elem.text or "")
                if day_elem is not None:
                    date_parts.append(day_elem.text or "")
                pub_date = " ".join(date_parts)
            
            # Extract DOI if available
            doi = ""
            doi_elem = root.find(".//ArticleId[@IdType='doi']")
            if doi_elem is not None:
                doi = doi_elem.text or ""
            
            # Build full text
            text_parts = []
            if title:
                text_parts.append(f"Title: {title}")
            if pmid:
                text_parts.append(f"PMID: {pmid}")
            if doi:
                text_parts.append(f"DOI: {doi}")
            if journal:
                text_parts.append(f"Journal: {journal}")
            if pub_date:
                text_parts.append(f"Publication Date: {pub_date}")
            if authors:
                text_parts.append(f"Authors: {', '.join(authors[:10])}")  # Limit to first 10 authors
            if abstract:
                text_parts.append(f"\nAbstract:\n{abstract}")
            
            full_text = "\n\n".join(text_parts)
            
            # Truncate if needed
            MAX_TEXT_LENGTH = 3000
            if len(full_text) > MAX_TEXT_LENGTH:
                full_text = full_text[:MAX_TEXT_LENGTH]
                last_period = full_text.rfind('.')
                if last_period > MAX_TEXT_LENGTH * 0.8:
                    full_text = full_text[:last_period + 1]
            
            return {
                "pmid": pmid,
                "title": title,
                "abstract": abstract,
                "authors": ", ".join(authors[:10]),
                "journal": journal,
                "pub_date": pub_date,
                "doi": doi,
                "text": full_text,
                "filename": f"pubmed_{pmid}.html"
            }
        except Exception as e:
            logger.error(f"  Error parsing XML for PMID {pmid}: {e}")
            return None

    async def run(self):
        logger.info(f"[{self.name}] Starting Job...")
        # Load existing records to skip duplicates
        existing_count = len(self._load_existing_records())
        logger.info(f"[{self.name}] Found {existing_count} existing records. Will scrape new data up to 400 total.")
        
        MAX_RECORDS = 400
        record_count = 0
        
        # Search for broad fraud terms or "all" if you remove the term
        retstart = 0
        retmax = 100 
        
        while retstart < 10000 and record_count < MAX_RECORDS: # NCBI often limits deep paging to 10k without special E-Link setup
            params = {
                "db": "pubmed",
                "term": "fraud OR misconduct OR retraction OR falsified",
                "retmode": "json",
                "retstart": retstart,
                "retmax": retmax,
            }
            if NCBI_API_KEY:
                params["api_key"] = NCBI_API_KEY
            
            data = await self.fetch_json(self.base_url, params=params)
            
            if not data or 'esearchresult' not in data:
                break
                
            id_list = data['esearchresult']['idlist']
            if not id_list:
                break

            # Fetch full article details for each PMID
            for i, pmid in enumerate(id_list, 1):
                if record_count >= MAX_RECORDS:
                    break
                    
                if i % 10 == 0:
                    logger.info(f"  Fetching article details: {i}/{len(id_list)}... (Saved: {record_count}/{MAX_RECORDS})")
                
                article_details = await self.fetch_article_details(pmid)
                if article_details:
                    if self.save_record({
                        "pmid": pmid, 
                        "type": "article", 
                        "source": "PubMed",
                        "title": article_details.get("title", ""),
                        "abstract": article_details.get("abstract", ""),
                        "authors": article_details.get("authors", ""),
                        "journal": article_details.get("journal", ""),
                        "pub_date": article_details.get("pub_date", ""),
                        "doi": article_details.get("doi", ""),
                        "filename": article_details.get("filename", f"pubmed_{pmid}.html"),
                        "text": article_details.get("text", f"PMID: {pmid}")
                    }):
                        record_count += 1
                else:
                    # Fallback if efetch fails
                    if self.save_record({
                        "pmid": pmid, 
                        "type": "article_id", 
                        "source": "PubMed",
                        "filename": f"pubmed_{pmid}.html",
                        "text": f"PMID: {pmid}\nSource: PubMed search for fraud/misconduct/retraction"
                    }):
                        record_count += 1
                
                await asyncio.sleep(self.rate_limit)
            
            if record_count >= MAX_RECORDS:
                logger.info(f"[{self.name}] Reached limit of {MAX_RECORDS} records. Stopping.")
                break
            
            retstart += retmax
            await asyncio.sleep(self.rate_limit)
        logger.info(f"[{self.name}] Batch Complete.")

class RetractionWatchScraper(DataSource):
    """Target: Retracted Papers via Crossref"""
    def __init__(self):
        super().__init__("RetractionWatch", "https://api.crossref.org/works")

    async def run(self):
        logger.info(f"[{self.name}] Starting Job...")
        # Load existing records to skip duplicates
        existing_count = len(self._load_existing_records())
        logger.info(f"[{self.name}] Found {existing_count} existing records. Will scrape new data up to 400 total.")
        
        MAX_RECORDS = 400
        record_count = 0
        
        cursor = "*"
        headers = {"User-Agent": f"FraudScraper/1.0 (mailto:{MY_EMAIL})"} # <--- POLITE POOL

        while record_count < MAX_RECORDS:
            params = {
                "filter": "update-type:retraction",
                "rows": "100",
                "cursor": cursor
            }
            data = await self.fetch_json(self.base_url, params=params, headers=headers)
            
            if not data: break
            
            items = data['message']['items']
            if not items: break

            for item in items:
                if record_count >= MAX_RECORDS:
                    break
                    
                title = item.get("title", [""])[0] if item.get("title") else ""
                journal = item.get("container-title", [""])[0] if item.get("container-title") else ""
                doi = item.get("DOI", "")
                
                if self.save_record({
                    "doi": doi,
                    "title": title,
                    "journal": journal,
                    "type": "retraction",
                    "filename": f"retraction_{doi.replace('/', '_')}.html",
                    "text": f"DOI: {doi}\nTitle: {title}\nJournal: {journal}\nType: Retraction"
                }):
                    record_count += 1

            if record_count >= MAX_RECORDS:
                logger.info(f"[{self.name}] Reached limit of {MAX_RECORDS} records. Stopping.")
                break
                
            cursor = data['message'].get('next-cursor')
            if not cursor: break
            await asyncio.sleep(self.rate_limit)

class OpenFDAScraper(DataSource):
    """Target: Adverse Drug Events using API Key"""
    def __init__(self):
        super().__init__("FDA_FAERS", "https://api.fda.gov/drug/event.json")

    async def run(self):
        logger.info(f"[{self.name}] Starting Job...")
        # Load existing records to skip duplicates
        existing_count = len(self._load_existing_records())
        logger.info(f"[{self.name}] Found {existing_count} existing records. Will scrape new data up to 400 total.")
        
        MAX_RECORDS = 400
        record_count = 0
        
        # Note: Even with a key, OpenFDA has a skip limit of 25,000 via API.
        # We scrape the most recent 25,000 events here.
        skip = 0
        limit = 100
        
        while skip < 25000 and record_count < MAX_RECORDS:
            params = {
                "limit": limit,
                "skip": skip,
            }
            if OPENFDA_API_KEY:
                params["api_key"] = OPENFDA_API_KEY
            
            data = await self.fetch_json(self.base_url, params=params)
            
            if not data: break
            
            results = data.get('results', [])
            if not results: break

            for event in results:
                if record_count >= MAX_RECORDS:
                    break
                    
                report_id = event.get("safetyreportid", "")
                drugs = event.get("patient", {}).get("drug", [])
                reactions = event.get("patient", {}).get("reaction", [])
                
                drug_text = ", ".join([d.get("medicinalproduct", "") for d in drugs if isinstance(d, dict)])
                reaction_text = ", ".join([r.get("reactionmeddrapt", "") for r in reactions if isinstance(r, dict)])
                
                if self.save_record({
                    "report_id": report_id,
                    "drug": drug_text,
                    "reaction": reaction_text,
                    "type": "adverse_event",
                    "filename": f"fda_faers_{report_id}.html",
                    "text": f"Report ID: {report_id}\nDrug: {drug_text}\nReaction: {reaction_text}\nType: Adverse Drug Event"
                }):
                    record_count += 1
            
            if record_count >= MAX_RECORDS:
                logger.info(f"[{self.name}] Reached limit of {MAX_RECORDS} records. Stopping.")
                break
                
            skip += limit
            await asyncio.sleep(self.rate_limit)

class NIHReporterScraper(DataSource):
    """Target: Grant Funding"""
    def __init__(self):
        super().__init__("NIH_Grants", "https://api.reporter.nih.gov/v2/projects/search")

    async def run(self):
        logger.info(f"[{self.name}] Starting Job...")
        # Load existing records to skip duplicates
        existing_count = len(self._load_existing_records())
        logger.info(f"[{self.name}] Found {existing_count} existing records. Will scrape new data up to 400 total.")
        
        MAX_RECORDS = 400
        record_count = 0
        
        offset = 0
        limit = 500
        # We search for active grants in fiscal year 2024 as a starting point
        while record_count < MAX_RECORDS:
            payload = {
                "criteria": {"fiscal_years": [2024, 2025]},
                "offset": offset,
                "limit": limit
            }
            data = await self.fetch_json(self.base_url, method="POST", payload=payload)
            
            if not data: break
            
            results = data.get('results', [])
            if not results: break

            for grant in results:
                if record_count >= MAX_RECORDS:
                    break
                    
                project_num = grant.get("project_num", "")
                pi_name = grant.get("contact_pi_name", "")
                org_name = grant.get("org_name", "")
                total_cost = grant.get("award_amount")
                
                # Handle None values for total_cost
                if total_cost is None:
                    total_cost = 0
                try:
                    total_cost = float(total_cost) if total_cost else 0
                except (ValueError, TypeError):
                    total_cost = 0
                
                cost_display = f"${total_cost:,.0f}" if total_cost > 0 else "Not specified"
                
                if self.save_record({
                    "project_num": project_num,
                    "pi_name": pi_name,
                    "org_name": org_name,
                    "total_cost": total_cost,
                    "type": "grant",
                    "filename": f"nih_grant_{project_num}.html",
                    "text": f"Project: {project_num}\nPI: {pi_name}\nOrganization: {org_name}\nAward Amount: {cost_display}\nType: NIH Grant"
                }):
                    record_count += 1

            if record_count >= MAX_RECORDS:
                logger.info(f"[{self.name}] Reached limit of {MAX_RECORDS} records. Stopping.")
                break
                
            offset += limit
            # NIH cap is usually 10k-15k via API without exporting
            if offset > 10000: 
                logger.info(f"[{self.name}] Reached API limit. Switching to Exporter recommended for full history.")
                break
            await asyncio.sleep(self.rate_limit)

class ClinicalTrialsScraper(DataSource):
    """Target: Study Protocols - Fetches full protocol details"""
    def __init__(self):
        super().__init__("ClinicalTrials", "https://clinicaltrials.gov/api/v2/studies")

    async def run(self):
        logger.info(f"[{self.name}] Starting Job...")
        # Load existing records to skip duplicates
        existing_count = len(self._load_existing_records())
        logger.info(f"[{self.name}] Found {existing_count} existing records. Will scrape new data up to 400 total.")
        
        MAX_RECORDS = 400
        record_count = 0
        
        token = None
        page_count = 0
        
        while record_count < MAX_RECORDS:
            # Request full protocol section including descriptions and eligibility
            params = {
                "pageSize": "100",
                "fields": "NCTId,BriefTitle,OfficialTitle,StatusModule,DescriptionModule,EligibilityModule"
            }
            if token: 
                params["pageToken"] = token
            
            data = await self.fetch_json(self.base_url, params=params)
            if not data: break
            
            studies = data.get('studies', [])
            if not studies:
                break
            
            page_count += 1
            logger.info(f"  Processing page {page_count}, {len(studies)} studies... (Saved: {record_count}/{MAX_RECORDS})")
            
            for study in studies:
                if record_count >= MAX_RECORDS:
                    break
                protocol = study.get('protocolSection', {})
                ident = protocol.get('identificationModule', {})
                status = protocol.get('statusModule', {})
                desc = protocol.get('descriptionModule', {})
                eligibility = protocol.get('eligibilityModule', {})
                
                nct_id = ident.get('nctId', '')
                title = ident.get('officialTitle', '') or ident.get('briefTitle', '')
                status_text = status.get('overallStatus', '')
                
                # Extract full protocol details
                brief_summary = desc.get('briefSummary', '')
                detailed_description = desc.get('detailedDescription', '')
                eligibility_criteria = eligibility.get('eligibilityCriteria', '')
                minimum_age = eligibility.get('minimumAge', '')
                maximum_age = eligibility.get('maximumAge', '')
                sex = eligibility.get('sex', '')
                healthy_volunteers = eligibility.get('healthyVolunteers', '')
                
                # Build comprehensive text content
                text_parts = []
                text_parts.append(f"NCT ID: {nct_id}")
                if title:
                    text_parts.append(f"Title: {title}")
                if status_text:
                    text_parts.append(f"Status: {status_text}")
                if brief_summary:
                    text_parts.append(f"\nBrief Summary:\n{brief_summary}")
                if detailed_description:
                    text_parts.append(f"\nDetailed Description:\n{detailed_description}")
                if eligibility_criteria:
                    text_parts.append(f"\nEligibility Criteria:\n{eligibility_criteria}")
                if minimum_age or maximum_age:
                    age_range = f"{minimum_age or 'N/A'} - {maximum_age or 'N/A'}"
                    text_parts.append(f"Age Range: {age_range}")
                if sex:
                    text_parts.append(f"Sex: {sex}")
                if healthy_volunteers is not None:
                    text_parts.append(f"Healthy Volunteers: {healthy_volunteers}")
                
                full_text = "\n\n".join(text_parts)
                
                # Truncate if needed (for GPT context window)
                MAX_TEXT_LENGTH = 3000
                if len(full_text) > MAX_TEXT_LENGTH:
                    full_text = full_text[:MAX_TEXT_LENGTH]
                    last_period = full_text.rfind('.')
                    if last_period > MAX_TEXT_LENGTH * 0.8:
                        full_text = full_text[:last_period + 1]
                
                if self.save_record({
                    "nct_id": nct_id,
                    "title": title,
                    "status": status_text,
                    "brief_summary": brief_summary,
                    "detailed_description": detailed_description,
                    "eligibility_criteria": eligibility_criteria,
                    "minimum_age": minimum_age,
                    "maximum_age": maximum_age,
                    "sex": sex,
                    "healthy_volunteers": healthy_volunteers,
                    "type": "protocol",
                    "filename": f"clinical_trial_{nct_id}.html",
                    "text": full_text
                }):
                    record_count += 1

            if record_count >= MAX_RECORDS:
                logger.info(f"[{self.name}] Reached limit of {MAX_RECORDS} records. Stopping.")
                break
                
            token = data.get('nextPageToken')
            if not token: break
            await asyncio.sleep(self.rate_limit)

class EuropePMCScraper(DataSource):
    """Target: Europe PMC articles - provides abstracts and metadata"""
    def __init__(self):
        super().__init__("Europe_PMC", "https://www.ebi.ac.uk/europepmc/webservices/rest/search")

    async def run(self):
        logger.info(f"[{self.name}] Starting Job...")
        # Load existing records to skip duplicates
        existing_count = len(self._load_existing_records())
        logger.info(f"[{self.name}] Found {existing_count} existing records. Will scrape new data up to 400 total.")
        
        MAX_RECORDS = 400
        record_count = 0
        
        page = 1
        page_size = 100
        headers = {"User-Agent": f"FraudScraper/1.0 (mailto:{MY_EMAIL})"}
        
        while record_count < MAX_RECORDS:
            params = {
                "query": "fraud OR misconduct OR retraction OR falsified OR image manipulation OR data fabrication",
                "format": "json",
                "page": page,
                "pageSize": page_size,
                "resultType": "core"  # Includes abstracts
            }
            
            data = await self.fetch_json(self.base_url, params=params, headers=headers)
            if not data:
                break
            
            result_list = data.get('resultList', {})
            results = result_list.get('result', [])
            if not results:
                break
            
            for article in results:
                if record_count >= MAX_RECORDS:
                    break
                pmid = article.get('pmid', '')
                title = article.get('title', '')
                abstract = article.get('abstractText', '') or article.get('abstract', '')
                authors = article.get('authorString', '')
                journal = article.get('journalTitle', '')
                pub_year = article.get('pubYear', '')
                doi = article.get('doi', '')
                
                # Build full text
                text_parts = []
                if title:
                    text_parts.append(f"Title: {title}")
                if pmid:
                    text_parts.append(f"PMID: {pmid}")
                if doi:
                    text_parts.append(f"DOI: {doi}")
                if journal:
                    text_parts.append(f"Journal: {journal}")
                if pub_year:
                    text_parts.append(f"Publication Year: {pub_year}")
                if authors:
                    text_parts.append(f"Authors: {authors}")
                if abstract:
                    text_parts.append(f"\nAbstract:\n{abstract}")
                
                full_text = "\n\n".join(text_parts)
                
                # Truncate if needed
                MAX_TEXT_LENGTH = 3000
                if len(full_text) > MAX_TEXT_LENGTH:
                    full_text = full_text[:MAX_TEXT_LENGTH]
                    last_period = full_text.rfind('.')
                    if last_period > MAX_TEXT_LENGTH * 0.8:
                        full_text = full_text[:last_period + 1]
                
                if self.save_record({
                    "pmid": pmid,
                    "doi": doi,
                    "title": title,
                    "abstract": abstract,
                    "authors": authors,
                    "journal": journal,
                    "pub_year": pub_year,
                    "type": "article",
                    "source": "Europe_PMC",
                    "filename": f"europmc_{pmid or doi.replace('/', '_')}.html",
                    "text": full_text
                }):
                    record_count += 1
            
            if record_count >= MAX_RECORDS:
                logger.info(f"[{self.name}] Reached limit of {MAX_RECORDS} records. Stopping.")
                break
            
            # Check if there are more pages
            total_hits = result_list.get('hitCount', 0)
            current_page_results = len(results)
            if (page * page_size) >= total_hits or current_page_results < page_size:
                break
            
            page += 1
            await asyncio.sleep(self.rate_limit)

# ==========================================
# 3. THE BULK DOWNLOADERS
# ==========================================
class BulkFileDownloader(DataSource):
    """Downloads massive ZIP files for CMS and MAUDE"""
    def __init__(self, name, download_url):
        super().__init__(name, download_url)

    async def run(self):
        filename = DATA_DIR / f"{self.name}_raw.zip"
        if filename.exists():
            logger.info(f"[{self.name}] File {filename} exists. Skipping download.")
            return

        logger.info(f"[{self.name}] Starting Download (Large File)...")
        logger.info(f"[{self.name}] URL: {self.base_url}")
        try:
            async with self.session.get(self.base_url) as response:
                if response.status == 200:
                    with open(filename, 'wb') as f:
                        # Download in chunks to save RAM
                        async for chunk in response.content.iter_chunked(1024*1024):
                            f.write(chunk)
                    logger.info(f"[{self.name}] Download Complete.")
                elif response.status == 404:
                    logger.warning(f"[{self.name}] File not found (404). URL may be outdated or file may not exist.")
                    logger.warning(f"[{self.name}] Skipping download. You may need to manually download from the source website.")
                else:
                    logger.error(f"[{self.name}] Failed: {response.status}")
        except Exception as e:
            logger.error(f"[{self.name}] Download Error: {e}")
            logger.warning(f"[{self.name}] Continuing with other scrapers...")

# ==========================================
# 4. MAIN ORCHESTRATOR
# ==========================================
async def main():
    # Define the jobs
    jobs = [
        PubMedScraper(),
        EuropePMCScraper(),  # Added Europe PMC scraper
        RetractionWatchScraper(),
        OpenFDAScraper(),
        NIHReporterScraper(),
        ClinicalTrialsScraper(),
        # Bulk Downloads - URLs may need updating yearly
        # CMS OpenPayments: Check https://www.cms.gov/openpayments/data/dataset-downloads for latest
        # Note: OpenPayments 2024 data should be available but URLs change annually
        # FDA MAUDE: Check https://www.fda.gov/medical-devices/mdr-medical-device-reporting for latest
        BulkFileDownloader("CMS_OpenPayments", "https://download.cms.gov/openpayments/PGYR23_P012024.ZIP"),
        BulkFileDownloader("FDA_MAUDE_2024", "https://www.accessdata.fda.gov/MAUDE/ftparea/mdrfoi-2024.zip")
    ]

    # Create shared session
    async with aiohttp.ClientSession() as session:
        tasks = []
        for job in jobs:
            job.session = session
            # Wrap each task to handle errors gracefully
            async def safe_run(job):
                try:
                    await job.run()
                except Exception as e:
                    logger.error(f"[{job.name}] Fatal error: {e}", exc_info=True)
                    logger.warning(f"[{job.name}] Continuing with other scrapers...")
            tasks.append(safe_run(job))

        logger.info(f"🚀 STARTING GLOBAL FRAUD SCRAPER WITH {len(jobs)} JOBS...")
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("✅ ALL DATA COLLECTION COMPLETE")

if __name__ == "__main__":
    asyncio.run(main())
