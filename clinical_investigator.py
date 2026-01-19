#!/usr/bin/env python3
"""
Clinical Investigator - Deep research on fraud leads using Tavily and Claude Sonnet 4.5.
Takes a selected lead from GPT ranker output and performs forensic investigation.
"""

import json
import sys
import csv
from pathlib import Path
from typing import Dict, Any, Optional
import requests

# Import config for API keys
try:
    import config
    TAVILY_API_KEY = getattr(config, 'TAVILY_API_KEY', None)
    ANTHROPIC_API_KEY = getattr(config, 'ANTHROPIC_API_KEY', None)
except ImportError:
    TAVILY_API_KEY = None
    ANTHROPIC_API_KEY = None

CLINICAL_INVESTIGATOR_PROMPT = """You are Dr. Watson, an expert Clinical Forensic Investigator and False Claims Act (Qui Tam) analyst. 

Your goal is to validate "Fraud Leads" by conducting deep research on scientific literature, regulatory timelines, and litigation history. You must determine if the lead represents a viable qui tam case.

### INPUT DATA:
You will be given a LEAD containing:
- Headline/Summary
- Key Facts
- Fraud Type (e.g., Grant Fraud, FDA Fraud, Clinical Trial Fraud)
- Implicated Actors (researchers, institutions, companies)
- Federal Programs Involved (NIH, Medicare, etc.)
- Source information

### INVESTIGATION PROTOCOL (Execute in Order):

**CRITICAL FIRST STEP - CHECK RETRACTION REASON AND SETTLEMENTS:**
Before doing anything else:
1. **CHECK RETRACTION REASON:** Search for the retraction notice using PMID/NCT ID. If retraction reason mentions "permissions", "license", "copyright", "licensing fee", "MMSE instrument", "without proper permissions" → IMMEDIATELY score 0 and stop. These are copyright disputes, NOT fraud.
2. **CHECK FOR RECENT SETTLEMENTS:** Search for recent settlements (2024-2025) using the SPECIFIC identifiers provided (PMID, NCT ID, grant numbers). 
   - **IF settlement is for THIS SPECIFIC case** (same PMID/NCT ID/grant): Case is already resolved → LOW score (0-40)
   - **IF settlement is for a DIFFERENT but similar case**: This is a POSITIVE indicator → Shows DOJ is actively pursuing these cases, establishes precedent, INCREASES viability score
   - **PRECEDENT CASES** (Duke, Dana-Farber, etc.) are STRONG POSITIVE indicators that similar cases are viable
3. **RECENT FRAUD RETRACTIONS (2024-2026):** If retraction is recent (2024-2026) and cites data fabrication, image manipulation, or missing raw data → These are HIGH VALUE leads. Score 85-95 if fraud gap >3 years and federal programs involved. Do NOT downgrade these.

PHASE 1: THE "KILL" CHECK (Eliminate bad leads early)

CRITICAL: Use the provided identifiers (NCT IDs, PMIDs) from the LEAD DATA to search for specific information.

1. If this involves a specific drug and adverse event:
   - Use NCT ID or PMID if provided to find the specific trial/drug.
   - Search for "[Drug Name] [Adverse Event] lawsuit multidistrict litigation".
   - IF widespread litigation exists > 5 years ago -> STOP. Report as "Legacy Case - Not Viable".
   - Search for the *current* FDA Label for [Drug Name].
   - IF the Adverse Event is listed in the "Boxed Warning" or "Warnings and Precautions" -> CHECK DATE.
   - When was this warning added? If > 6 years ago -> STOP. Report as "Statute of Limitations Expired".

2. If this involves research fraud/retractions:
   - Use PMID or NCT ID if provided to find the specific study.
   - Search for "[NCT ID] retraction" or "PMID [PMID] retraction" to find the exact retraction notice.
   - **CRITICAL - COPYRIGHT/PERMISSION CHECK:** If the retraction reason mentions "permissions", "license", "copyright", "licensing fee", "MMSE instrument", or similar copyright/intellectual property issues → IMMEDIATELY set Viability Score to 0. These are NOT fraud cases - they are civil copyright disputes and have NO False Claims Act liability.
   - Search for "[Institution] [Researcher Name] retraction lawsuit settlement".
   - IF settled litigation exists > 6 years ago -> STOP. Report as "Legacy Case - Not Viable".

3. If Clinical Trial ID (NCT) is provided:
   - Search for "[NCT ID] NIH grant" to find grant information and funding amounts.
   - Search for "[NCT ID] ClinicalTrials.gov" to find trial details, start/end dates, and investigators.
   - Search for "[NCT ID] withdrawn terminated" to find exact withdrawal date and reason.

PHASE 2: THE "FRAUD GAP" ANALYSIS (The Core Investigation)

CRITICAL: Use the provided NCT ID or PMID to find specific dates and details.

1. Find the "Date of Knowledge":
   - If NCT ID provided: Search "[NCT ID] ClinicalTrials.gov" to find trial start date and when issues were first reported.
   - If PMID provided: Search "PMID [PMID] retraction" to find exact retraction date and when concerns were first raised.
   - Search for the *earliest* credible reports or evidence of the fraud/alleged misconduct.
   - For research fraud: When was the retraction issued? When were concerns first raised? Check PubPeer, RetractionWatch.
   - For drug issues: Search PubMed/Google Scholar for earliest case reports or studies using the PMID or drug name.
   - Note the date of the first "Safety Signal" or "Fraud Indicator".

2. Find the "Date of Action":
   - If NCT ID provided: Search "[NCT ID] NIH grant termination" or "[NCT ID] withdrawn date" to find exact action date.
   - When did regulatory bodies (FDA, NIH, etc.) take action or update requirements?
   - When were warnings added, grants revoked, or investigations concluded?
   - Search for specific grant numbers if NCT ID is available.

3. Calculate the Gap:
   - Gap = (Date of Action) - (Date of Knowledge).
   - IF Gap > 3 years, this is a HIGH VIABILITY Qui Tam case (Failure to Act/Warn).
   - IF Gap < 3 years but > 1 year, this is MODERATE VIABILITY.
   - IF Gap < 1 year, this is LOW VIABILITY (timely response).

PHASE 3: FINANCIAL IMPACT & STATUTE VIOLATIONS
1. Determine Federal Program Impact:
   - **CRITICAL:** First check the LEAD DATA - it may already contain federal program information (NIH, Medicare, etc.) and grant details. USE THIS INFORMATION.
   - If lead data mentions NIH grants but no specific amounts: Search for grant numbers, amounts, and duration using the NCT ID, PMID, or researcher names.
   - If NCT ID provided: Search "[NCT ID] NIH grant amount" or "[NCT ID] funding" to find specific grant amounts.
   - If NIH grants involved: Search for grant numbers, amounts, and duration using the NCT ID or researcher names.
   - If Medicare/Medicaid involved: Search for drug sales data or procedure reimbursement using drug names or procedure codes.
   - Use ClinicalTrials.gov data (via NCT ID) to find sponsor information and funding sources.
   - **DO NOT IGNORE LEAD DATA:** If the lead says "NIH grant money was misappropriated" or mentions federal programs, this is VALID evidence even if you can't find specific grant numbers in searches.
   - Estimate taxpayer cost of the fraud with specific numbers when available.

2. Identify Statute Violations:
   - False Claims Act (31 U.S.C. § 3729)
   - Anti-Kickback Statute (42 U.S.C. § 1320a-7b)
   - Stark Law (42 U.S.C. § 1395nn)
   - Research Misconduct (if NIH grants involved)

PHASE 4: LITIGATION STATUS (CRITICAL - CHECK FIRST)
- **IMMEDIATE CHECK:** Search for settlements using SPECIFIC identifiers (PMID, NCT ID, grant numbers).
  - **If settlement is for THIS SPECIFIC case** (same PMID/NCT ID): Case already resolved → LOW score (0-40)
  - **If settlement is for a DIFFERENT similar case**: POSITIVE indicator → Shows DOJ enforcement, establishes precedent, INCREASES score
- Search for existing lawsuits, settlements, or ongoing investigations using NCT ID, PMID, or key terms.
- Check if qui tam cases have already been filed FOR THIS SPECIFIC CASE.
- Check if statute of limitations may have expired.
- **IMPORTANT:** Only downgrade if the settlement is for THIS SPECIFIC case. Precedent cases (Duke, Dana-Farber, etc.) are STRONG POSITIVE indicators that increase viability.

### OUTPUT FORMAT (Markdown Report):

# Clinical Investigation Report: [Headline/Focus]

## 1. Executive Summary
- **Viability Score:** [0-100]
  - **Scoring Guidelines:**
    - 85-100: High viability - Clear fraud gap >3 years, significant federal program impact, strong precedent cases, recent retractions (2024-2026) with fraud indicators, no settlement for THIS specific case
    - 70-84: Moderate-high viability - Fraud gap 1-3 years, some federal impact, precedent cases exist, no settlement for THIS specific case
    - 50-69: Moderate viability - Fraud gap 1-3 years, limited federal impact, no blocking issues
    - 40-49: Low viability - Short gap <1 year, limited impact, or settlement found for THIS specific case
    - 0-39: Not viable - Copyright/permission retractions (score 0), settlement for THIS specific case, statute expired, or insufficient evidence
- **Conclusion:** (e.g., "High potential for Qui Tam. 4-year gap found between clinical knowledge and regulatory action." OR "Not viable - recent settlement in 2024 resolves this case.")
- **Recommended Action:** (e.g., "Proceed with qui tam filing" / "Monitor for additional evidence" / "Not viable - recent settlement" / "Not viable - statute expired")

### Evidence Quality Assessment (MANDATORY - Anti-Hallucination Audit Trail)
**Verification Checklist Results:**
- [ ] Documented evidence of fraud? [YES/NO + brief explanation with URL]
- [ ] Documented federal program involvement? [YES/NO + brief explanation with URL]
- [ ] Documented false claims submitted? [YES/NO + brief explanation with URL]
- [ ] Dates and amounts verified through search? [YES/NO + what was verified]
- [ ] Sources are official and credible? [YES/NO + list source types]

**Evidence Found:**
- [List each piece of VERIFIED evidence with URL]
- E.g., "Retraction notice citing image manipulation (URL: ...)"
- E.g., "NIH grant R01CA123456 totaling $2.5M (URL: ...)"

**Evidence NOT Found (searched but unavailable):**
- [Be specific about what you searched for and couldn't find]
- E.g., "Searched for settlement using NCT ID - no results found"
- E.g., "Could not locate specific grant numbers despite multiple searches"

**Lead Data Verification:**
- Verified from lead: [List what you confirmed]
- Could NOT verify from lead: [List what you couldn't confirm]
- Contradictions found: [List any contradictions between lead data and search results]

**Overall Confidence Level:** [HIGH / MODERATE / LOW]
- HIGH: Multiple official government sources confirm fraud + federal involvement
- MODERATE: Some official sources, some unverified claims
- LOW: Mostly unverified, relying primarily on lead data or speculation

## 2. Timeline of Events (The "Fraud Gap")
- **First Indicator of Issue:** [Date] - [Citation/DOI/Source]
  *Context: (e.g., "Retraction notice published citing data fabrication...")*
- **Regulatory/Institutional Action:** [Date] - [Link/Reference]
  *Context: (e.g., "NIH revoked grant 5 years later.")*
- **Gap Duration:** [X Years]
- **Assessment:** [Analysis of whether the gap indicates negligence]

## 3. Financial Impact
- **Federal Program(s) Affected:** [NIH, Medicare, Medicaid, etc.]
- **Estimated Taxpayer Cost:** [Amount if available or "Significant" / "Unknown"]
- **Basis for Estimate:** [How you arrived at this assessment]

## 4. Statute Violations
- **Primary Violation:** [e.g., "False Claims Act - Grant Fraud"]
- **Legal Basis:** [Brief explanation of how this constitutes a violation]

## 5. Litigation Status
- **Existing Lawsuits:** [Yes/No/Unknown]
- **Details:** (e.g., "Class action settled in 2015" or "No major litigation found, but qui tam case filed in 2024")
- **Statute of Limitations:** [Assessment of whether still viable]

## 6. Cited Sources
[List specific URLs, DOIs, or references found during research - you MUST cite actual sources from your search results]

### RULES OF ENGAGEMENT:

**ANTI-HALLUCINATION CRITICAL RULES:**
1. **NO INVENTED FACTS:** If you cannot find a specific date, citation, grant number, or settlement → State "Data Not Found" explicitly. NEVER invent or estimate.
2. **VERIFY BEFORE CLAIMING:** Every factual claim must be verified through search results. Do NOT rely on assumptions or general knowledge.
3. **CITE WITH URLs:** Every factual claim must include the URL or DOI from your search results. Format: "According to [source] (URL: ...)".
4. **DISTINGUISH SEARCH RESULTS FROM LEAD DATA:**
   - Search results = External verification (STRONGEST evidence)
   - Lead data = Initial analysis (USE but verify when possible)
   - If search CONTRADICTS lead data → Search results take precedence
5. **NO SPECULATION IN SCORES:** Scores must be based on DOCUMENTED evidence only. Phrases like "likely", "probably", "appears to be" indicate speculation → Lower score.

**EVIDENCE HIERARCHY (Strongest to Weakest):**
1. **GOLD STANDARD**: Official government documents (ORI findings, DOJ settlements, NIH grant termination notices, FDA warning letters)
2. **HIGH QUALITY**: Journal retraction notices with explicit reasons, institutional investigation reports
3. **MODERATE**: PubPeer discussions from credible researchers (with specific evidence), news articles citing official sources
4. **LOW QUALITY**: Lead data without external verification, speculation in articles, "concerns raised" without findings
5. **INVALID**: Your assumptions, inferences without evidence, "common sense" reasoning

**CRITICAL VERIFICATION REQUIREMENTS:**
- **NO HALLUCINATIONS:** If you cannot find a specific date or citation, state "Data Not Found." Do not invent citations.
- **USE YOUR TOOLS:** You MUST use search results to verify dates and facts. Do not rely on internal memory.
- **CITE EVERYTHING:** Every factual claim must be supported by a citation from your search results.
- **CROSS-CHECK LEAD DATA:** The lead data contains information from initial analysis. Use it as a STARTING POINT but:
  - If lead mentions NIH grants → Search to verify grant numbers and amounts
  - If lead mentions federal programs → Search to verify actual billing/claims
  - If lead suggests fraud → Search to verify official findings (not just suspicion)
  - **CRITICAL**: Lead data may contain false positives from initial ranking. Your job is to VALIDATE, not confirm.
- **SKEPTICISM:** Verify claims independently. If search results do NOT support lead data, note the discrepancy and score conservatively.
- **COPYRIGHT/PERMISSION RETRACTIONS = ZERO SCORE:** If retraction reason mentions "permissions", "license", "copyright", "licensing fee", "MMSE instrument", "without proper permissions", or similar IP/copyright issues → IMMEDIATELY score 0. These are civil copyright disputes, NOT fraud against the government. No False Claims Act liability exists.
- **SETTLEMENTS - DISTINGUISH THIS CASE vs PRECEDENT:**
  - Settlement for THIS SPECIFIC case (same PMID/NCT ID/grant) = Case resolved → Score 0-40
  - Settlement for DIFFERENT similar case = POSITIVE precedent → INCREASES score (shows DOJ enforcement, establishes viability)
  - Always check if settlement matches the specific identifiers (PMID, NCT ID) from the lead data
- **FIND SPECIFIC DATA:** Use NCT IDs and PMIDs to find exact dates, grant amounts, investigator names, and institutions. Don't accept "Data Not Found" without trying multiple search angles.
- **MULTIPLE SEARCHES:** Try different search terms and angles. If one search doesn't find data, try variations (e.g., "NCT12345678", "ClinicalTrials.gov NCT12345678", "NCT12345678 grant").
- **NEW FRAUD CASES:** Recent retractions (2024-2026) with clear fraud indicators (data fabrication, image manipulation, missing raw data) are HIGH VALUE leads, especially if they involve federal funding. Score these 85-95 if fraud gap >3 years and federal programs involved.

**BEFORE ASSIGNING FINAL SCORE - MANDATORY CHECKLIST:**
Ask yourself these questions and answer explicitly in your report:
□ Did I find DOCUMENTED evidence of fraud (official retraction citing fraud, ORI finding, investigation report)? YES/NO
□ Did I find DOCUMENTED federal program involvement (specific NIH grant, Medicare billing evidence, DoD funding)? YES/NO
□ Did I find DOCUMENTED false claims (grant applications with false data, Medicare claims for ineffective treatment, FDA submissions with falsified data)? YES/NO
□ Did I VERIFY dates and amounts through search (not just accept lead data)? YES/NO
□ Are my sources OFFICIAL and CREDIBLE (not speculation or news articles without verification)? YES/NO

**SCORING BASED ON CHECKLIST:**
- 5 YES = Score 85-100 (High viability)
- 4 YES (missing only specific false claims documentation) = Score 70-84 (Moderate-high viability)
- 3 YES (missing false claims + one other) = Score 50-69 (Moderate viability)
- 2 YES or fewer = Score 0-49 (Low viability)
- If "fraud documented" = NO → Automatic score ≤ 40 regardless of other factors
- If "federal involvement documented" = NO → Automatic score ≤ 30 regardless of other factors

**AUDIT TRAIL REQUIREMENT:**
In your Executive Summary, include a section titled "Evidence Quality Assessment" that explicitly states:
- What evidence you FOUND (with URLs)
- What evidence you COULD NOT FIND (be specific)
- What claims from lead data you VERIFIED vs. could not verify
- Your confidence level: HIGH (multiple official sources), MODERATE (some verification), LOW (mostly unverified)

This audit trail is MANDATORY and will be reviewed to prevent hallucinations.
"""


def search_tavily(query: str, max_results: int = 5) -> list:
    """Search using Tavily API and return results."""
    if not TAVILY_API_KEY:
        print(f"ERROR: TAVILY_API_KEY not found in config.py", file=sys.stderr)
        print(f"ERROR: TAVILY_API_KEY not found in config.py", flush=True)  # Also to stdout
        return []
    
    url = "https://api.tavily.com/search"
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": "advanced",
        "max_results": max_results,
        "include_domains": [],
        "exclude_domains": []
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    except Exception as e:
        print(f"Tavily search error for '{query}': {e}", file=sys.stderr)
        return []


def investigate_lead(lead_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform a full investigation on a lead using Tavily search and Claude analysis.
    Returns a dict with 'report' (markdown string) and 'viability_score' (0-100 int).
    
    This is the main function that can be imported and called from other modules.
    """
    try:
        # Validate lead_data
        if not lead_data:
            return {
                "report": "# Error\n\nNo lead data provided.",
                "viability_score": 0,
                "search_results_count": 0
            }
        
        # Perform searches based on lead content
        searches = []
        
        # Extract key terms for searching (with safe defaults)
        headline = lead_data.get('headline', '') or ''
        fraud_type = lead_data.get('fraud_type', '') or ''
        implicated = lead_data.get('implicated_actors', '') or ''
        filename = lead_data.get('filename', '') or ''
        original_text = lead_data.get('original_text', '') or ''
        nct_ids = lead_data.get('nct_ids', []) or []
        pmids = lead_data.get('pmids', []) or []
        
        # Ensure lists are actually lists
        if not isinstance(nct_ids, list):
            nct_ids = []
        if not isinstance(pmids, list):
            pmids = []
        
        # Extract Grant Numbers from original_text using regex
        grant_numbers = []
        if original_text:
            import re
            # Pattern for NIH grant numbers: R01 CA12345, U01 AI98765, etc.
            # Matches: R01 CA12345, R21 HL67890, U01 AI98765
            grant_pattern = r'\b([A-Z]\d{2})\s+([A-Z]{1,3}\d{4,6})\b'
            grant_matches = re.findall(grant_pattern, original_text, re.IGNORECASE)
            # Combine prefix and suffix: "R01 CA12345"
            grant_numbers = [f"{match[0]} {match[1]}" for match in grant_matches]
            # Also look for full grant format: "Grant Number: R01 CA12345"
            full_grant_pattern = r'[Gg]rant\s+[Nn]umber[:\s]+([A-Z]\d{2}\s+[A-Z]{1,3}\d{4,6})'
            full_matches = re.findall(full_grant_pattern, original_text, re.IGNORECASE)
            grant_numbers.extend(full_matches)
            # Remove duplicates and clean up
            grant_numbers = list(set([g.strip().upper() for g in grant_numbers if len(g.strip()) > 5]))
        
        # Build search queries - prioritize most important searches with site: targeting
        # 1. Search by NCT ID if available (most specific) - prioritize key searches
        for nct_id in nct_ids[:1]:  # Limit to 1 NCT ID for speed
            searches.append(f"{nct_id} ClinicalTrials.gov")
            searches.append(f"{nct_id} withdrawn terminated retraction")
            searches.append(f"{nct_id} NIH grant funding")
            searches.append(f"{nct_id} settlement lawsuit qui tam")  # Check settlements first
            # Targeted site searches
            searches.append(f"site:reporter.nih.gov {nct_id}")
            searches.append(f"site:justice.gov {nct_id} qui tam settlement")
        
        # 2. Search by PMID if available - prioritize key searches
        for pmid in pmids[:1]:  # Limit to 1 PMID for speed
            searches.append(f"PMID {pmid} retraction notice")  # CRITICAL: Check retraction reason first
            searches.append(f"PMID {pmid} retraction reason copyright permission license")  # Check for copyright issues
            searches.append(f"PMID {pmid} fraud investigation settlement")
            searches.append(f"PMID {pmid} NIH grant")
            # Targeted site searches
            searches.append(f"site:reporter.nih.gov PMID {pmid}")
            searches.append(f"site:justice.gov PMID {pmid} settlement")
        
        # 3. Search by Grant Numbers if found
        for grant_num in grant_numbers[:3]:  # Limit to 3 grant numbers
            searches.append(f"{grant_num} NIH grant")
            searches.append(f"{grant_num} fraud investigation")
            searches.append(f"site:reporter.nih.gov {grant_num}")
            searches.append(f"site:justice.gov {grant_num} qui tam")
        
        # 4. Extract trial/research identifiers from filename
        if filename:
            # Extract NCT ID from filename if present
            import re
            filename_nct = re.search(r'NCT\d{8}', filename)
            if filename_nct and filename_nct.group(0) not in [nct_id for nct_id in nct_ids]:
                nct_id = filename_nct.group(0)
                searches.append(f"{nct_id} ClinicalTrials.gov details")
                searches.append(f"{nct_id} funding grant")
            
            # Extract other identifiers (DOI, etc.)
            doi_match = re.search(r'10\.\d+/[^\s]+', filename + " " + original_text)
            if doi_match:
                searches.append(f"{doi_match.group(0)} retraction")
                searches.append(f"{doi_match.group(0)} fraud")
        
        # 5. Search by headline - prioritize settlement check with site: targeting
        if headline:
            # Extract key terms from headline
            headline_words = headline.split()[:4]  # First 4 words
            headline_key = " ".join(headline_words)
            searches.append(f"{headline_key} settlement 2024 2025")  # Check settlements first
            searches.append(f"site:justice.gov {headline_key} settlement qui tam")
            searches.append(f"{headline_key} fraud investigation")
            searches.append(f"{headline_key} retraction withdrawal")
        
        # 6. Search by implicated actors - limit to most important with site: targeting
        if implicated and 'Unknown' not in implicated and implicated.strip():
            # Split actors if multiple, take first one only
            actors_list = [a.strip() for a in implicated.split(';') if a.strip()][:1]
            for actor in actors_list:
                searches.append(f"{actor} settlement lawsuit qui tam")  # Check settlements first
                searches.append(f"site:justice.gov {actor} settlement False Claims")
                searches.append(f"site:reporter.nih.gov {actor}")
                searches.append(f"{actor} fraud NIH grant")
        
        # 7. Search by fraud type - simplified with site: targeting
        if fraud_type and 'Grant Fraud' in fraud_type:
            if nct_ids:
                searches.append(f"NIH grant {nct_ids[0]} termination")
                searches.append(f"site:reporter.nih.gov {nct_ids[0]}")
            else:
                searches.append(f"{headline[:40]} NIH grant revocation")
                searches.append(f"site:reporter.nih.gov {headline[:40]}")
        
        if fraud_type and ('FDA' in fraud_type or 'Clinical Trial' in fraud_type):
            if nct_ids:
                searches.append(f"FDA {nct_ids[0]} clinical trial warning")
            else:
                searches.append(f"{headline[:40]} FDA warning")
        
        # Perform searches and collect results - increased for better data coverage
        all_results = []
        for query in searches:
            try:
                results = search_tavily(query, max_results=10)  # Increased to 10 for better coverage
                if results:
                    all_results.extend(results)
            except Exception as e:
                # Continue with other searches if one fails
                print(f"Warning: Search failed for query '{query[:50]}...': {e}", file=sys.stderr)
                continue
        
        # Deduplicate by URL
        seen_urls = set()
        unique_results = []
        for result in all_results:
            try:
                url = result.get('url', '') if result else ''
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    unique_results.append(result)
            except Exception:
                # Skip invalid results
                continue
        
        # Call Claude with lead data and search results - increased context for better analysis
        try:
            report = call_claude_with_search(lead_data, unique_results[:50])  # Increased to 50 for better coverage
        except Exception as e:
            print(f"Error calling Claude: {e}", file=sys.stderr)
            report = f"# Error\n\nFailed to generate investigation report: {str(e)}"
    
        # Extract viability score from report (look for "Viability Score:" pattern)
        viability_score = None
        if report:
            try:
                import re
                # Look for patterns like "- **Viability Score:** 15" or "**Viability Score:** 15" or "Viability Score: 15"
                # Use flexible pattern that finds "Viability Score" followed by any characters (including asterisks, colons) then a number
                patterns = [
                    r'Viability\s+Score[^\d]*?:\s*(\d+)',  # "Viability Score" followed by non-digits, colon, whitespace, then digits
                    r'Viability.*?Score.*?(\d+)',  # Fallback: "Viability" followed by anything, "Score", then first number (be careful - might match wrong number)
                ]
                for pattern in patterns:
                    try:
                        score_match = re.search(pattern, report, re.IGNORECASE)
                        if score_match:
                            potential_score = int(score_match.group(1))
                            if 0 <= potential_score <= 100:
                                viability_score = potential_score
                                break  # Use first valid match
                    except (ValueError, AttributeError):
                        continue
                
                # If still not found, try extracting from Conclusion section
                if viability_score is None:
                    try:
                        conclusion_match = re.search(r'Conclusion[:\-]\s*.*?(\d{1,3})', report, re.IGNORECASE | re.DOTALL)
                        if conclusion_match:
                            potential_score = int(conclusion_match.group(1))
                            if 0 <= potential_score <= 100:
                                viability_score = potential_score
                    except (ValueError, AttributeError):
                        pass
            except Exception as e:
                print(f"Warning: Error extracting viability score: {e}", file=sys.stderr)
                viability_score = None
    
        return {
            "report": report or "# Error\n\nNo investigation report generated.",
            "viability_score": viability_score if viability_score is not None else 0,
            "search_results_count": len(unique_results)
        }
    except Exception as e:
        # Return error report instead of crashing
        error_report = f"# Investigation Error\n\nAn error occurred during investigation: {str(e)}\n\n"
        error_report += "Please check the logs for more details."
        print(f"Error in investigate_lead: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return {
            "report": error_report,
            "viability_score": 0,
            "search_results_count": 0
        }


def call_claude_with_search(lead_data: Dict[str, Any], search_results: list) -> str:
    """Call Claude Sonnet 4.5 with the lead data and search results."""
    if not ANTHROPIC_API_KEY:
        error_msg = "ERROR: ANTHROPIC_API_KEY not found in config.py"
        print(error_msg, file=sys.stderr)
        print(error_msg, flush=True)  # Also to stdout
        return "# Error\n\nAPI key not configured. Please add ANTHROPIC_API_KEY to config.py"
    
    # Format search results as context
    search_context = "## Search Results:\n\n"
    for i, result in enumerate(search_results, 1):
        search_context += f"### Source {i}: {result.get('title', 'Untitled')}\n"
        search_context += f"URL: {result.get('url', 'N/A')}\n"
        search_context += f"Content: {result.get('content', 'No content')}\n\n"
    
    # Format lead data with original identifiers
    lead_context = "## LEAD DATA:\n\n"
    lead_context += f"**Headline:** {lead_data.get('headline', 'N/A')}\n\n"
    lead_context += f"**Qui Tam Score:** {lead_data.get('qui_tam_score', 'N/A')}\n\n"
    
    # Include identifiers if available
    nct_ids = lead_data.get('nct_ids', [])
    pmids = lead_data.get('pmids', [])
    filename = lead_data.get('filename', '')
    
    if nct_ids:
        lead_context += f"**Clinical Trial ID(s):** {', '.join(nct_ids)}\n\n"
    if pmids:
        lead_context += f"**PubMed ID(s):** {', '.join(pmids[:3])}\n\n"  # Limit to 3 PMIDs
    if filename:
        lead_context += f"**Source Filename:** {filename}\n\n"
        # Extract any other identifiers from filename
        import re
        if 'NCT' in filename and not nct_ids:
            nct_match = re.search(r'NCT\d{8}', filename)
            if nct_match:
                lead_context += f"**Clinical Trial ID (from filename):** {nct_match.group(0)}\n\n"
    
    lead_context += f"**Key Facts:** {lead_data.get('key_facts', 'N/A')}\n\n"
    lead_context += f"**Fraud Type:** {lead_data.get('fraud_type', 'N/A')}\n\n"
    lead_context += f"**Implicated Actors:** {lead_data.get('implicated_actors', 'N/A')}\n\n"
    lead_context += f"**Federal Programs:** {lead_data.get('federal_programs_involved', 'N/A')}\n\n"
    lead_context += f"**Reason:** {lead_data.get('reason', 'N/A')}\n\n"
    # CRITICAL: Emphasize that lead data information is VALID and should be used
    lead_context += f"**IMPORTANT:** The lead data above contains information from the initial analysis. If it mentions federal programs (NIH, Medicare, etc.), grant funding, or fraud details, these are VALID and should be incorporated into your analysis. Do not dismiss this information just because search results don't find specific grant numbers.\n\n"
    lead_context += f"**CRITICAL REMINDERS:**\n"
    lead_context += f"- If retraction reason mentions 'permissions', 'license', 'copyright', 'MMSE instrument' → Score 0 (copyright dispute, not fraud)\n"
    lead_context += f"- Recent retractions (2024-2026) with fraud indicators (data fabrication, image manipulation, missing raw data) are HIGH VALUE → Score 85-95 if fraud gap >3 years\n"
    lead_context += f"- Precedent cases (Duke, Dana-Farber) are POSITIVE indicators, not negative\n\n"
    
    # Include original text excerpt if available (for context)
    original_text = lead_data.get('original_text', '')
    if original_text:
        lead_context += f"**Original Source Text (excerpt):**\n{original_text[:1000]}\n\n"
    
    user_message = f"{lead_context}\n\n{search_context}\n\nPlease conduct a thorough investigation using the above lead data and search results. Follow the investigation protocol and generate a detailed report with cited sources."
    
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    
    payload = {
        "model": "claude-sonnet-4-20250514",  # Sonnet 4.5
        "max_tokens": 3000,  # Reduced from 4096 for faster responses
        "system": CLINICAL_INVESTIGATOR_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": user_message
            }
        ]
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=90)  # Reduced from 120 to 90 seconds
        response.raise_for_status()
        data = response.json()
        
        # Extract text from Claude's response
        content = data.get("content", [])
        if content and len(content) > 0:
            return content[0].get("text", "# Error\n\nNo response from Claude")
        return "# Error\n\nUnexpected response format from Claude"
    except Exception as e:
        print(f"Claude API error: {e}", file=sys.stderr)
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}", file=sys.stderr)
        return f"# Error\n\nFailed to call Claude API: {str(e)}"


def load_lead_from_csv(csv_path: Path, row_index_or_source_index: int, use_source_index: bool = False) -> Optional[Dict[str, Any]]:
    """Load a specific row from the ranked CSV file.
    
    Args:
        csv_path: Path to CSV file
        row_index_or_source_index: If use_source_index=False, this is the CSV row index (0-based, excluding header).
                                  If use_source_index=True, this is the source_row_index to search for.
        use_source_index: If True, search by source_row_index column value instead of row position.
    """
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            if use_source_index:
                # Search for row with matching source_row_index
                for row in rows:
                    try:
                        if int(row.get('source_row_index', -1)) == row_index_or_source_index:
                            return row
                    except (ValueError, TypeError):
                        continue
                return None
            else:
                # Use direct row index
                if 0 <= row_index_or_source_index < len(rows):
                    return rows[row_index_or_source_index]
                return None
    except Exception as e:
        print(f"Error reading CSV: {e}", file=sys.stderr)
        return None


def main():
    """Main entry point for clinical investigator."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Clinical Investigator - Research fraud leads using Tavily and Claude")
    parser.add_argument("--csv", type=Path, default=Path("data/results/qui_tam_ranked.csv"),
                       help="Path to ranked CSV file")
    parser.add_argument("--row-index", type=int, required=True,
                       help="Row index (0-based) or source_row_index value to search for")
    parser.add_argument("--use-source-index", action="store_true",
                       help="If set, search CSV by source_row_index column value instead of row position")
    parser.add_argument("--output", type=Path, default=None,
                       help="Output file path (default: data/results/investigation_[row_index].md)")
    
    args = parser.parse_args()
    
    # Check if CSV file exists
    if not args.csv.exists():
        error_msg = f"# Error\n\nCSV file not found: {args.csv}\n\nPlease ensure the GPT ranker has been run and generated {args.csv.name}"
        print(error_msg, file=sys.stderr)
        print(error_msg, flush=True)
        sys.exit(1)
    
    # Load lead data
    lead_data = load_lead_from_csv(args.csv, args.row_index, use_source_index=args.use_source_index)
    if not lead_data:
        if args.use_source_index:
            error_msg = f"# Error\n\nCould not find row with source_row_index={args.row_index} in {args.csv}"
        else:
            error_msg = f"# Error\n\nCould not load row {args.row_index} from {args.csv}\n\nRow index may be out of range. CSV has headers, so first data row is index 0."
        print(error_msg, file=sys.stderr)
        print(error_msg, flush=True)
        sys.exit(1)
    
    print(f"Investigating lead: {lead_data.get('headline', 'N/A')[:80]}...", flush=True)
    
    # Use the improved investigate_lead function which has all the new search logic
    print("Running investigation with improved search queries...", flush=True)
    investigation_result = investigate_lead(lead_data)
    report = investigation_result.get("report", "")
    viability_score = investigation_result.get("viability_score", 0)
    
    print(f"Investigation complete. Viability score: {viability_score}", flush=True)
    
    # Save report
    output_path = args.output or Path(f"data/results/investigation_{args.row_index}.md")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n✅ Investigation complete!")
    print(f"Report saved to: {output_path}", flush=True)
    
    # Also print report to stdout for server streaming
    print("\n" + "="*80, flush=True)
    print(report, flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        error_msg = f"# Error\n\nUnexpected error in clinical_investigator: {str(e)}\n\nTraceback:\n"
        import traceback
        error_msg += traceback.format_exc()
        print(error_msg, file=sys.stderr)
        print(error_msg, flush=True)  # Also print to stdout so it shows in the UI
        sys.exit(1)
