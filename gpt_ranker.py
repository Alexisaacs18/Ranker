#!/usr/bin/env python3
"""Rank medical provider records by querying a local GPT server for qui tam potential."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import re
from typing import Optional
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

import requests
try:
    import sqlite3
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

# Import investigation functions
try:
    from clinical_investigator import investigate_lead
    INVESTIGATION_AVAILABLE = True
except ImportError:
    INVESTIGATION_AVAILABLE = False


try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)


# Scientific Fraud Ranking Prompt - Enhanced with cross-reference analysis
SCIENTIFIC_FRAUD_RANKING_PROMPT = """You are a forensic data analyst and expert in research integrity. Your goal is to analyze scientific abstracts and full-text articles to identify potential Research Misconduct that could lead to False Claims Act (FCA) liability.

CRITICAL CONTEXT:
Government funding (NIH, CDC, DoD) obtained via falsified research is a violation of the False Claims Act (e.g., Duke University $112M settlement).
FDA approval obtained via falsified clinical trial data leads to false claims when Medicare pays for the drug.

CROSS-REFERENCE ANALYSIS:
When cross-referenced data is provided from the database (NIH grants, retractions, PubPeer discussions, FDA adverse events), use it to find PATTERNS:
- An author with a retracted paper who received NIH grants = RED FLAG
- A drug with FDA adverse events but studies claiming safety = RED FLAG  
- PubPeer discussions about data issues + NIH funding = HIGH RISK
- Multiple sources pointing to the same researcher/institution = PATTERN OF FRAUD

BOOST scores when cross-references reveal patterns across multiple data sources.

SCORING FRAMEWORK (0-100):

90-100 (HIGH PROBABILITY OF MISCONDUCT):
- Official "Retraction Notice" citing data fabrication or falsification.
- "Expression of Concern" regarding image manipulation (Western blots, histology).
- "Impossible statistics" explicitly noted (e.g., standard deviations are identical across different groups).
- Funding source is US Gov (NIH/NSF) AND data is flagged as unreliable.
- Author has history of retractions (check "Papermill" indicators).

70-89 (STRONG SUSPICION - "The Duke Pattern"):
- Discrepancies between "Methods" and "Results" (e.g., endpoint switching).
- Study sponsored by Pharma company with UNDISCLOSED conflicts of interest.
- Results are "too good to be true" (100% cure rate in fatal disease).
- "Seeding Trial" indicators: Study designed purely to market a drug, not test it (no clear hypothesis, massive enrollment for simple observation).
- Off-label promotion: Study concludes efficacy for non-approved use based on weak/manipulated data.

50-69 (REQUIRES SCRUTINY):
- Massive self-citation by authors.
- Outlier results compared to all other literature in the field.
- "Ghost authorship" suspicion (industry writer not credited).
- Corrections issued for "coding errors" that flip the conclusion from negative to positive.

0-49 (NORMAL SCIENCE):
- Negative results published (honest science).
- Standard method limitations acknowledged.
- Proper ethical disclosures.

CRITICAL KEYWORDS TO HUNT:
- "Retracted" / "Withdrawn"
- "Image duplication" / "Photoshop" / "Splicing"
- "P-hacking" / "Data dredging"
- "Protocol deviation"
- "Unblinded" (in a double-blind study)
- "Post-hoc analysis" (changing the rules after the game is played)

OUTPUT FORMAT (JSON):
{
    "headline": "One sentence summary of the scientific anomaly",
    "qui_tam_score": 0-100,
    "fraud_vector": "Grant Fraud (NIH)" | "FDA Fraud (Clinical Trial)" | "Off-Label Marketing" | "Kickback (Sham Consulting)",
    "scientific_red_flags": [
        "Identical standard deviations (Impossible Data)",
        "Image reuse across different figures",
        "Endpoint switching (Outcome reporting bias)"
    ],
    "funding_source": "NIH Grant #XYZ" | "Pharma Sponsor" | "Unknown",
    "potential_damages_theory": "How does this bad science steal tax money? (e.g., 'NIH grant repaid' or 'Medicare paid for useless drug')",
    "implicated_institutions": ["University Name", "Pharma Company"],
    "investigation_status": "Retracted" | "Under Investigation" | "Correction Issued" | "Published (Unchallenged)",
    "next_step": "Download raw data" | "Check RetractionWatch" | "Compare with ClinicalTrials.gov protocol"
}
"""

DEFAULT_SYSTEM_PROMPT = SCIENTIFIC_FRAUD_RANKING_PROMPT


# Canonical mappings for fraud types
FRAUD_TYPE_CANONICAL_MAP = {
    "upcoding": {"upcoding", "up-coding", "code inflation", "billing inflation"},
    "phantom billing": {"phantom billing", "billing for services not rendered", "ghost billing"},
    "kickbacks": {"kickbacks", "illegal referrals", "stark law violation", "anti-kickback"},
    "unnecessary procedures": {"unnecessary procedures", "overtreatment", "unneeded services"},
    "false certification": {"false certification", "certification fraud"},
    "off-label marketing": {"off-label marketing", "off label", "unapproved use"},
}

# Canonical mappings for federal programs
PROGRAM_CANONICAL_MAP = {
    "Medicare": {"medicare", "medicare part a", "medicare part b", "medicare part d", "medicare advantage"},
    "Medicaid": {"medicaid", "medi-cal", "masshealth"},
    "TRICARE": {"tricare", "tri-care"},
    "VA": {"va", "veterans affairs", "veterans administration", "department of veterans affairs"},
}


def apply_config_defaults(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    config_path: Path = args.config  # type: ignore[assignment]
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with config_path.open("rb") as handle:
        data = tomllib.load(handle)
    for key, value in data.items():
        if not hasattr(args, key):
            continue
        current = getattr(args, key)
        default = parser.get_default(key)
        if current == default:
            if isinstance(default, Path):
                setattr(args, key, Path(value))
            else:
                setattr(args, key, value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Call a local OpenAI-compatible server (e.g. gpt-oss-120b) to extract "
            "qui tam potential from medical provider records."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("ranker_config.toml"),
        help="Optional TOML config file to supply defaults (see ranker_config.example.toml).",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/processed/combined_qui_tam_data.csv"),
        help="Path to the source CSV with 'filename' and 'text' columns.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/results/qui_tam_ranked.csv"),
        help="Path to write the ranked CSV results.",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=Path("data/results/qui_tam_ranked.jsonl"),
        help="Path to append newline-delimited JSON records for each row.",
    )
    parser.add_argument(
        "--endpoint",
        default="http://127.0.0.1:1234/v1",
        help="Base URL of the OpenAI-compatible server.",
    )
    parser.add_argument(
        "--model",
        default="openai/gpt-oss-20b",
        help="Model identifier exposed by the server (check via --list-models).",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.0,
        help="Temperature for model responses (0.0 = deterministic, higher = more random).",
    )
    parser.add_argument(
        "--prompt-file",
        type=Path,
        default=None,
        help="Path to a text file containing the system prompt (overrides default).",
    )
    parser.add_argument(
        "--system-prompt",
        default=None,
        help="Inline system prompt string (overrides --prompt-file and default).",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Optional API key for servers that require authentication.",
    )
    parser.add_argument(
        "--reasoning-effort",
        choices=["low", "medium", "high"],
        help="If provided, passes reasoning effort hints supported by some models.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip rows already present in the checkpoint or JSON output.",
    )
    parser.add_argument(
        "--overwrite-output",
        action="store_true",
        help="Allow truncating existing output/JSONL files (use with caution).",
    )
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=Path("data/.qui_tam_checkpoint"),
        help="Plain-text file storing processed filenames (used with --resume).",
    )
    parser.add_argument(
        "--known-json",
        action="append",
        default=[],
        help="Additional JSONL files containing already-processed rows to skip.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=0,
        help="If >0, rotate outputs every N source rows and store chunk files in --chunk-dir.",
    )
    parser.add_argument(
        "--chunk-dir",
        type=Path,
        default=Path("contrib"),
        help="Directory to store chunked outputs when --chunk-size > 0.",
    )
    parser.add_argument(
        "--chunk-manifest",
        type=Path,
        default=Path("data/chunks.json"),
        help="Manifest JSON file listing generated chunks (used by the viewer).",
    )
    parser.add_argument(
        "--include-action-items",
        action="store_true",
        help="Request action items from the model and include them in outputs.",
    )
    parser.add_argument(
        "--start-row",
        type=int,
        default=1,
        help="1-based row index to start processing (useful for collaborative chunking).",
    )
    parser.add_argument(
        "--end-row",
        type=int,
        default=None,
        help="1-based row index to stop processing (inclusive).",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=200,
        help="Limit processing to the first N rows (useful for smoke-tests).",
    )
    parser.add_argument(
        "--min-score",
        type=int,
        default=50,
        help="Skip records with existing fraud_potential_score below this threshold (0 to disable).",
    )
    parser.add_argument(
        "--cross-reference",
        action="store_true",
        default=True,
        help="Enable cross-referencing with SQLite database (default: True).",
    )
    parser.add_argument(
        "--no-cross-reference",
        dest="cross_reference",
        action="store_false",
        help="Disable cross-referencing with SQLite database.",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/fraud_data.db"),
        help="Path to SQLite database for cross-referencing (default: data/fraud_data.db).",
    )
    parser.add_argument(
        "--investigate-min-score",
        type=int,
        default=50,
        help="Minimum qui_tam_score to trigger investigation (default: 50). Investigation is automatically enabled for all leads meeting this threshold.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Seconds to sleep between requests to avoid overwhelming the server.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=600.0,
        help="HTTP request timeout in seconds (default: 600 = 10 minutes).",
    )
    parser.add_argument(
        "--list-models",
        action="store_true",
        help="List models exposed by the endpoint and exit.",
    )
    parser.add_argument(
        "--rebuild-manifest",
        action="store_true",
        help="Scan chunk files and rebuild the manifest (data/chunks.json), then exit.",
    )
    parser.add_argument(
        "--power-watts",
        type=float,
        default=None,
        help="If provided, estimate energy usage (average watts).",
    )
    parser.add_argument(
        "--electric-rate",
        type=float,
        default=None,
        help="Electricity cost in USD per kWh for cost estimation.",
    )
    parser.add_argument(
        "--run-hours",
        type=float,
        default=None,
        help="Override elapsed hours for cost estimate (otherwise uses wall time).",
    )
    args = parser.parse_args()
    config_path = None
    if args.config:
        config_path = Path(args.config)
    else:
        for candidate in (Path("ranker_config.toml"), Path("ranker_config.example.toml")):
            if candidate.exists():
                config_path = candidate
                break
    if config_path:
        args.config = config_path
        apply_config_defaults(parser, args)
    return args

def extract_fraud_score_from_text(text: str) -> Optional[int]:
    """Extract fraud potential score from the CSV text field."""
    match = re.search(r'FRAUD POTENTIAL SCORE:\s*(\d+)', text, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def query_database_cross_references(text: str, db_path: Optional[Path] = None) -> str:
    """
    Query the SQLite database for cross-references related to the current record.
    Returns a formatted string with related data from other sources.
    """
    if not SQLITE_AVAILABLE:
        return ""
    
    if db_path is None:
        db_path = Path("data/fraud_data.db")
    
    if not db_path.exists():
        return ""
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cross_refs = []
        text_lower = text.lower()
        
        # Extract potential entity names (simple heuristic: capitalized words)
        # Look for patterns like "Dr. Smith" or "John Smith" or institution names
        name_pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b'
        potential_names = re.findall(name_pattern, text[:2000])  # First 2000 chars to limit
        
        # Extract PMIDs
        pmids = re.findall(r'PMID[:\s]*(\d{8,})|/(\d{8,})/', text)
        pmid_list = [pmid[0] or pmid[1] for pmid in pmids if pmid[0] or pmid[1]]
        
        # Extract DOIs
        dois = re.findall(r'DOI[:\s]*([^\s,]+)|10\.\d{4,}/[^\s,]+', text)
        doi_list = [doi[0] if isinstance(doi, tuple) else doi for doi in dois if doi]
        
        # Extract drug names (common pattern: drug names often capitalized)
        # This is a heuristic - could be improved
        drug_indicators = ['drug', 'medication', 'treatment', 'therapeutic']
        
        # 1. Check for matching NIH Grants (by PI name or institution)
        for name in potential_names[:10]:  # Limit to avoid too many queries
            if len(name) < 4 or len(name.split()) > 5:  # Skip very short or very long names
                continue
            cursor.execute("""
                SELECT project_num, pi_name, org_name, total_cost
                FROM nih_grants
                WHERE pi_name_normalized LIKE ? OR org_name LIKE ?
                LIMIT 5
            """, (f"%{name.lower()}%", f"%{name}%"))
            grants = cursor.fetchall()
            if grants:
                cross_refs.append(f"\n[CROSS-REFERENCE: NIH GRANTS]")
                for grant in grants:
                    cost_display = f"${grant['total_cost']:,.0f}" if grant['total_cost'] else "Not specified"
                    cross_refs.append(f"  - Grant {grant['project_num']}: PI={grant['pi_name']}, Org={grant['org_name']}, Amount={cost_display}")
        
        # 2. Check for retractions (by PMID or DOI)
        for pmid in pmid_list[:5]:
            cursor.execute("""
                SELECT doi, title, journal
                FROM retractions
                WHERE doi LIKE ? OR text_content LIKE ?
                LIMIT 3
            """, (f"%{pmid}%", f"%{pmid}%"))
            retractions = cursor.fetchall()
            if retractions:
                cross_refs.append(f"\n[CROSS-REFERENCE: RETRACTIONS] (PMID {pmid})")
                for ret in retractions:
                    cross_refs.append(f"  - Retracted: {ret['title'][:80]}... (Journal: {ret['journal']})")
        
        for doi in doi_list[:5]:
            cursor.execute("""
                SELECT doi, title, journal
                FROM retractions
                WHERE doi LIKE ?
                LIMIT 3
            """, (f"%{doi}%",))
            retractions = cursor.fetchall()
            if retractions:
                cross_refs.append(f"\n[CROSS-REFERENCE: RETRACTIONS] (DOI {doi})")
                for ret in retractions:
                    cross_refs.append(f"  - Retracted: {ret['title'][:80]}...")
        
        # 3. Check PubPeer articles (by institution or author name)
        for name in potential_names[:10]:
            if len(name) < 4:
                continue
            cursor.execute("""
                SELECT pub_id, title, comment_count, url
                FROM pubpeer_articles
                WHERE text_content LIKE ? AND comment_count > 0
                LIMIT 5
            """, (f"%{name}%",))
            pubpeer = cursor.fetchall()
            if pubpeer:
                cross_refs.append(f"\n[CROSS-REFERENCE: PUBPEER DISCUSSIONS]")
                for pp in pubpeer:
                    cross_refs.append(f"  - PubPeer {pp['pub_id']}: {pp['title'][:60]}... ({pp['comment_count']} comments)")
        
        # 4. Check FDA FAERS (by drug name mentions)
        if any(indicator in text_lower for indicator in drug_indicators):
            # Extract potential drug names (heuristic)
            drug_pattern = r'\b([A-Z][a-z]+(?:[a-z]+)?)\s+(?:drug|medication|treatment|therapy)\b'
            potential_drugs = re.findall(drug_pattern, text[:1000])
            for drug in potential_drugs[:5]:
                cursor.execute("""
                    SELECT report_id, drug, reaction
                    FROM fda_faers
                    WHERE drug LIKE ? OR text_content LIKE ?
                    LIMIT 3
                """, (f"%{drug}%", f"%{drug}%"))
                faers = cursor.fetchall()
                if faers:
                    cross_refs.append(f"\n[CROSS-REFERENCE: FDA ADVERSE EVENTS] (Drug: {drug})")
                    for fda in faers:
                        cross_refs.append(f"  - Report {fda['report_id']}: {fda['drug'][:50]}... → {fda['reaction'][:50]}")
        
        conn.close()
        
        if cross_refs:
            return "\n" + "="*80 + "\nCROSS-REFERENCED DATA FROM DATABASE:\n" + "="*80 + "\n".join(cross_refs) + "\n" + "="*80 + "\n"
        
    except Exception as e:
        # If database query fails, silently continue without cross-references
        pass
    
    return ""


def should_skip_row(row: dict, min_score: int = 50) -> tuple:
    """
    Check if a row should be skipped based on existing fraud score.
    
    Returns:
        tuple: (should_skip: bool, fraud_score: Optional[int])
    """
    text = row.get('text', '')
    fraud_score = extract_fraud_score_from_text(text)
    
    # If there's a score and it's below minimum, skip it
    if fraud_score is not None and fraud_score < min_score:
        return (True, fraud_score)
    
    return (False, fraud_score)


def load_system_prompt(args: argparse.Namespace) -> Tuple[str, str]:
    """Load the system prompt from file or use inline/default prompt.

    Returns:
        Tuple of (prompt_text, prompt_source_description)
    """
    # Priority: inline --system-prompt > --prompt-file > default file > hardcoded default
    if args.system_prompt:
        return args.system_prompt, "inline (--system-prompt)"

    prompt_file = args.prompt_file
    if not prompt_file:
        # Try default prompt file location
        default_prompt_file = Path("prompts") / "qui_tam_system_prompt.txt"
        if default_prompt_file.exists():
            prompt_file = default_prompt_file
        else:
            # Fall back to hardcoded default
            return DEFAULT_SYSTEM_PROMPT, "default (hardcoded)"

    if not prompt_file.exists():
        raise FileNotFoundError(f"Prompt file not found: {prompt_file}")

    with prompt_file.open("r", encoding="utf-8") as f:
        prompt = f.read().strip()

    if not prompt:
        raise ValueError(f"Prompt file is empty: {prompt_file}")

    return prompt, str(prompt_file)


def call_model(
    *,
    endpoint: str,
    model: str,
    filename: str,
    text: str,
    system_prompt: str,
    api_key: Optional[str],
    timeout: float,
    temperature: float,
    reasoning_effort: Optional[str],
    config_metadata: Optional[Dict[str, Any]] = None,
    db_path: Optional[Path] = None,
    enable_cross_reference: bool = True,
) -> Dict[str, Any]:
    """Send the document to the local GPT server and return parsed JSON.
    
    Args:
        enable_cross_reference: If True, queries database for related records and includes in analysis
    """
    # Get cross-reference data if enabled
    cross_ref_data = ""
    if enable_cross_reference and db_path:
        cross_ref_data = query_database_cross_references(text, db_path)
    
    # Build user message with cross-reference context
    user_content_parts = [
        "Analyze the following scientific article or abstract and respond with the JSON schema ",
        "described in the system prompt.",
        "",
        f"Article ID: {filename}",
        "--------",
        text.strip(),
        "--------"
    ]
    
    if cross_ref_data:
        user_content_parts.extend([
            "",
            "IMPORTANT: The following cross-referenced data was found in the database. ",
            "Use this information to identify patterns, connections, and red flags across sources:",
            cross_ref_data,
            "",
            "When analyzing, consider:",
            "- Does the article author appear in NIH grants?",
            "- Is this article or author mentioned in PubPeer discussions?",
            "- Are there related retractions or corrections?",
            "- Do any drugs/treatments have FDA adverse event reports?",
            "- Are there patterns across multiple data sources that indicate fraud?",
            "",
            "Score based on the combination of the article itself AND these cross-references."
        ])
    
    payload = {
        "model": model,
        "temperature": temperature,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": "\n".join(user_content_parts),
            },
        ],
    }
    if reasoning_effort:
        payload["reasoning"] = {"effort": reasoning_effort}

    # Include config metadata in the request if provided
    if config_metadata:
        payload["metadata"] = config_metadata

    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    url = f"{endpoint.rstrip('/')}/chat/completions"
    response = requests.post(url, json=payload, timeout=timeout, headers=headers)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:  # noqa: PERF203
        snippet = response.text[:500].replace("\n", " ")
        raise RuntimeError(f"HTTP {response.status_code} from {url}: {snippet}") from exc
    data = response.json()
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"Unexpected response format: {data}") from exc

    return ensure_json_dict(content)


def ensure_json_dict(content: str) -> Dict[str, Any]:
    """Parse the model response into a dictionary, trimming extra text if needed."""
    candidate = content.strip()
    if not candidate:
        raise ValueError("Model returned an empty message.")

    # Try direct JSON parse first.
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start == -1 or end == -1 or start >= end:
            raise
        parsed = json.loads(candidate[start : end + 1])

    if not isinstance(parsed, dict):
        raise TypeError(f"Expected a JSON object, received: {type(parsed)}")
    return parsed


def iter_rows(path: Path) -> Iterable[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if "text" not in row or "filename" not in row:
                raise ValueError("Input CSV must contain 'filename' and 'text' columns.")
            yield row


def load_checkpoint(path: Optional[Path]) -> Set[str]:
    completed: Set[str] = set()
    if not path or not path.exists():
        return completed
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            entry = line.strip()
            if entry:
                completed.add(entry)
    return completed


def load_jsonl_filenames(path: Optional[Path]) -> Set[str]:
    completed: Set[str] = set()
    if not path or not path.exists():
        return completed
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            filename = record.get("filename")
            if filename:
                completed.add(filename)
    return completed


def ensure_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item is not None]
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    return [str(value)]


def canonicalize_from_map(value: str, mapping: Dict[str, Set[str]], *, title_case: bool = False, upper_case: bool = False) -> Optional[str]:
    cleaned = " ".join(value.strip().split())
    if not cleaned:
        return None
    lookup = cleaned.lower()
    for canonical, synonyms in mapping.items():
        if lookup in synonyms:
            return canonical if not upper_case else canonical.upper()
    if upper_case:
        return cleaned.upper()
    if title_case:
        return cleaned.title()
    return cleaned


def normalize_fraud_types(values: List[str]) -> List[str]:
    """Normalize fraud type values."""
    normalized: List[str] = []
    seen: Set[str] = set()
    for value in values:
        canonical = canonicalize_from_map(value, FRAUD_TYPE_CANONICAL_MAP, title_case=False)
        if not canonical:
            continue
        canonical = canonical.lower()
        if canonical not in seen:
            normalized.append(canonical)
            seen.add(canonical)
    return normalized


def normalize_programs(values: List[str]) -> List[str]:
    """Normalize federal program names."""
    normalized: List[str] = []
    seen: Set[str] = set()
    for value in values:
        canonical = canonicalize_from_map(value, PROGRAM_CANONICAL_MAP, upper_case=False)
        if not canonical:
            continue
        canonical = canonical.strip()
        if canonical not in seen:
            normalized.append(canonical)
            seen.add(canonical)
    return normalized


def clean_entity_label(value: str) -> str:
    cleaned = " ".join(value.strip().split())
    if not cleaned:
        return ""
    cleaned = re.sub(r"\s*\(.*?\)\s*", " ", cleaned)
    for delimiter in (" - ", " – ", " — "):
        if delimiter in cleaned:
            cleaned = cleaned.split(delimiter, 1)[0]
    return " ".join(cleaned.split())


def normalize_text_list(values: List[str], *, strip_descriptor: bool = False) -> List[str]:
    normalized: List[str] = []
    seen: Set[str] = set()
    for value in values:
        cleaned = clean_entity_label(value) if strip_descriptor else " ".join(value.strip().split())
        if not cleaned:
            continue
        if cleaned not in seen:
            normalized.append(cleaned)
            seen.add(cleaned)
    return normalized


def count_total_csv_rows(path: Path) -> int:
    """Count total number of data rows in the CSV (excluding header)."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return sum(1 for _ in reader)


def calculate_workload(
    path: Path,
    *,
    max_rows: Optional[int],
    completed_filenames: Set[str],
    start_row: int,
    end_row: Optional[int],
) -> Dict[str, int]:
    total = 0
    already_done = 0
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for idx, row in enumerate(reader, start=1):
            if idx < start_row:
                continue
            if end_row is not None and idx > end_row:
                break
            total += 1
            if completed_filenames and row.get("filename") in completed_filenames:
                already_done += 1
            if max_rows is not None and total >= max_rows:
                break
    workload = max(0, total - already_done)
    return {"total": total, "already_done": already_done, "workload": workload}


def format_duration(seconds: float) -> str:
    seconds = max(0, int(round(seconds)))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def format_eta(
    start_time: float,
    processed: int,
    total: int,
    power_watts: Optional[float] = None,
    electric_rate: Optional[float] = None,
) -> str:
    if total <= 0:
        return ""
    if processed == 0:
        return "(ETA estimating...)"
    elapsed = time.monotonic() - start_time
    if elapsed <= 0:
        return "(ETA --:--:--)"
    rate = processed / elapsed
    if rate <= 0:
        return "(ETA --:--:--)"
    remaining = max(total - processed, 0)
    eta_seconds = remaining / rate if rate else 0

    # Base ETA message
    eta_msg = f"(ETA {format_duration(eta_seconds)})"

    # Add energy/cost estimates if available
    if power_watts is not None and electric_rate is not None:
        total_estimated_hours = (elapsed + eta_seconds) / 3600
        energy_cost = calculate_energy_cost(power_watts, electric_rate, total_estimated_hours)
        if energy_cost:
            eta_msg += f" | Est. total: {energy_cost['energy_kwh']:.2f} kWh / ${energy_cost['cost_usd']:.2f}"

    return eta_msg


class OutputRouter:
    def __init__(self, args: argparse.Namespace, fieldnames: List[str]):
        self.args = args
        self.fieldnames = fieldnames
        self.chunk_size = max(0, args.chunk_size)
        self.mode = "chunk" if self.chunk_size > 0 else "single"
        self.include_action_items = args.include_action_items
        self.csv_handle = None
        self.json_handle = None
        self.csv_writer = None
        self.current_chunk: Optional[Tuple[int, int]] = None
        self.current_json_path: Optional[Path] = None
        if self.mode == "single":
            self._init_single()
        else:
            self._init_chunk_state()

    def _init_single(self) -> None:
        # Append if files exist, otherwise create new files
        csv_mode = "a" if self.args.output.exists() else "w"
        self.args.output.parent.mkdir(parents=True, exist_ok=True)
        self.csv_handle = self.args.output.open(csv_mode, newline="", encoding="utf-8")
        self.csv_writer = csv.DictWriter(self.csv_handle, fieldnames=self.fieldnames)
        if csv_mode == "w":
            self.csv_writer.writeheader()
        json_mode = "a" if self.args.json_output.exists() else "w"
        self.args.json_output.parent.mkdir(parents=True, exist_ok=True)
        self.json_handle = self.args.json_output.open(json_mode, encoding="utf-8")

    def _init_chunk_state(self) -> None:
        self.chunk_dir: Path = self.args.chunk_dir
        self.chunk_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_manifest: Path = self.args.chunk_manifest
        self.manifest_entries = self._load_manifest()
        self.manifest_dirty = False
        self.total_dataset_rows = None  # Will be set by main()

    def _load_manifest(self) -> Dict[Tuple[int, int], Dict[str, Any]]:
        if not self.chunk_manifest.exists():
            return {}
        try:
            with self.chunk_manifest.open(encoding="utf-8") as handle:
                data = json.load(handle)
        except (json.JSONDecodeError, FileNotFoundError):
            return {}

        # Handle both old format (array) and new format (object with chunks)
        if isinstance(data, list):
            # Old format: array of chunks
            chunk_list = data
        elif isinstance(data, dict) and "chunks" in data:
            # New format: object with metadata and chunks
            chunk_list = data["chunks"]
        else:
            return {}

        entries: Dict[Tuple[int, int], Dict[str, Any]] = {}
        for entry in chunk_list:
            key = (entry.get("start_row"), entry.get("end_row"))
            if not isinstance(key[0], int) or not isinstance(key[1], int):
                continue
            entries[key] = entry
        return entries

    def write(self, row_idx: int, csv_row: Dict[str, Any], json_record: Dict[str, Any]) -> None:
        if self.mode == "single":
            self.csv_writer.writerow(csv_row)
            self.csv_handle.flush()
            self.json_handle.write(json.dumps(json_record, ensure_ascii=False) + "\n")
            self.json_handle.flush()
            return

        chunk_bounds = self._chunk_bounds(row_idx)
        self._ensure_chunk(chunk_bounds)
        self.json_handle.write(json.dumps(json_record, ensure_ascii=False) + "\n")
        self.json_handle.flush()

    def _chunk_bounds(self, row_idx: int) -> Tuple[int, int]:
        chunk_start = ((row_idx - 1) // self.chunk_size) * self.chunk_size + 1
        chunk_end = chunk_start + self.chunk_size - 1
        if self.args.end_row is not None:
            chunk_end = min(chunk_end, self.args.end_row)
        return (chunk_start, chunk_end)

    def _ensure_chunk(self, chunk_bounds: Tuple[int, int]) -> None:
        if self.current_chunk == chunk_bounds:
            return
        self._close_chunk()
        self.current_chunk = chunk_bounds
        chunk_start, chunk_end = chunk_bounds
        base = f"qui_tam_ranked_{chunk_start:05d}_{chunk_end:05d}"
        json_path = self.chunk_dir / f"{base}.jsonl"
        json_exists = json_path.exists()
        if json_exists and not self.args.resume and not self.args.overwrite_output:
            raise FileExistsError(
                f"Chunk JSON {json_path} exists. Use --resume or --overwrite-output."
            )
        json_mode = "a" if self.args.resume and json_exists else "w"
        self.json_handle = json_path.open(json_mode, encoding="utf-8")
        self.current_json_path = json_path

    def _close_chunk(self) -> None:
        if self.json_handle:
            self.json_handle.close()
            self.json_handle = None
        if self.current_chunk and self.current_json_path:
            chunk_start, chunk_end = self.current_chunk
            entry = {
                "start_row": chunk_start,
                "end_row": chunk_end,
                "json": str(self.current_json_path.as_posix()),
            }
            self.manifest_entries[self.current_chunk] = entry
            self.manifest_dirty = True
            # Write manifest immediately after each chunk closes
            self._write_manifest()
        self.current_chunk = None
        self.current_json_path = None

    def _write_manifest(self) -> None:
        """Write the manifest file to disk."""
        if not self.manifest_dirty:
            return
        entries = sorted(self.manifest_entries.values(), key=lambda e: e["start_row"])

        # Calculate total rows processed
        total_processed = 0
        for entry in entries:
            # Count actual lines in the chunk file
            chunk_path = Path(entry["json"])
            if chunk_path.exists():
                with chunk_path.open(encoding="utf-8") as f:
                    total_processed += sum(1 for _ in f)

        # Build manifest with metadata
        manifest = {
            "metadata": {
                "total_dataset_rows": self.total_dataset_rows if self.total_dataset_rows else "unknown",
                "rows_processed": total_processed,
                "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
            },
            "chunks": entries
        }

        self.chunk_manifest.parent.mkdir(parents=True, exist_ok=True)
        with self.chunk_manifest.open("w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2)
        self.manifest_dirty = False

    def close(self) -> None:
        if self.mode == "single":
            if self.csv_handle:
                self.csv_handle.close()
            if self.json_handle:
                self.json_handle.close()
            return
        self._close_chunk()
        self._write_manifest()

def build_config_metadata(args: argparse.Namespace, prompt_source: str) -> Dict[str, Any]:
    """Build metadata dictionary from config for inclusion in requests and outputs."""
    metadata = {
        "endpoint": args.endpoint,
        "model": args.model,
        "temperature": args.temperature,
        "prompt_source": prompt_source,
    }
    if args.reasoning_effort is not None:
        metadata["reasoning_effort"] = args.reasoning_effort
    if args.api_key:
        metadata["api_key_used"] = True
    if args.power_watts is not None:
        metadata["power_watts"] = args.power_watts
    if args.electric_rate is not None:
        metadata["electric_rate"] = args.electric_rate
    return metadata


def calculate_energy_cost(
    power_watts: Optional[float],
    electric_rate: Optional[float],
    hours: float,
) -> Optional[Dict[str, float]]:
    """Calculate energy consumption and cost."""
    if power_watts is None or electric_rate is None or hours <= 0:
        return None
    energy_kwh = (power_watts * hours) / 1000.0
    cost = energy_kwh * electric_rate
    return {"energy_kwh": energy_kwh, "cost_usd": cost}


def format_cost_summary(
    power_watts: Optional[float],
    electric_rate: Optional[float],
    elapsed_hours: float,
    override_hours: Optional[float],
) -> Optional[str]:
    if power_watts is None or electric_rate is None:
        return None
    hours = override_hours if override_hours is not None else elapsed_hours
    if hours <= 0:
        return None
    energy_kwh = (power_watts * hours) / 1000.0
    cost = energy_kwh * electric_rate
    return f"Energy ≈ {energy_kwh:.2f} kWh | Est. cost ≈ ${cost:.2f} (rate ${electric_rate}/kWh)"


def list_models(endpoint: str, api_key: Optional[str], timeout: float) -> None:
    """Print available model IDs from the endpoint."""
    url = f"{endpoint.rstrip('/')}/models"
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    models = data.get("data", [])
    if not models:
        print("No models reported by endpoint.")
        return
    print("Available models:")
    for entry in models:
        model_id = entry.get("id")
        created = entry.get("created")
        extra = f" (created {created})" if created else ""
        print(f" - {model_id}{extra}")


def rebuild_manifest(chunk_dir: Path, manifest_path: Path) -> None:
    """Scan chunk directory and rebuild the manifest file."""
    import re

    # Pattern to match chunk files: qui_tam_ranked_XXXXX_YYYYY.jsonl
    pattern = re.compile(r"qui_tam_ranked_(\d{5})_(\d{5})\.jsonl")

    chunks = []
    if not chunk_dir.exists():
        print(f"Chunk directory not found: {chunk_dir}")
        return

    for file_path in sorted(chunk_dir.glob("qui_tam_ranked_*.jsonl")):
        match = pattern.match(file_path.name)
        if not match:
            print(f"Skipping non-matching file: {file_path.name}")
            continue

        start_row = int(match.group(1))
        end_row = int(match.group(2))

        # Use relative path: chunk_dir/filename
        relative_path = chunk_dir / file_path.name

        chunks.append({
            "start_row": start_row,
            "end_row": end_row,
            "json": str(relative_path.as_posix()),
        })

    if not chunks:
        print(f"No chunk files found in {chunk_dir}")
        return

    # Sort by start_row
    chunks.sort(key=lambda c: c["start_row"])

    # Count total rows processed
    total_processed = 0
    for chunk in chunks:
        chunk_path = Path(chunk["json"])
        if chunk_path.exists():
            with chunk_path.open(encoding="utf-8") as f:
                total_processed += sum(1 for _ in f)

    # Try to get total dataset rows from the source CSV
    csv_candidates = [
        Path("data/processed/combined_qui_tam_data.jsonl"),
        Path("data") / "medical_records.csv",
    ]
    total_dataset_rows = None
    for csv_path in csv_candidates:
        if csv_path.exists():
            print(f"Counting total rows in {csv_path}...")
            try:
                total_dataset_rows = count_total_csv_rows(csv_path)
                print(f"Found {total_dataset_rows:,} total rows in dataset")
                break
            except Exception as e:
                print(f"Error counting rows: {e}")

    # Fall back to highest end_row if CSV not found
    if total_dataset_rows is None:
        total_dataset_rows = max((c["end_row"] for c in chunks), default=0) if chunks else "unknown"
        print(f"Source CSV not found, using highest chunk end_row: {total_dataset_rows}")

    # Build manifest with metadata
    manifest = {
        "metadata": {
            "total_dataset_rows": total_dataset_rows,
            "rows_processed": total_processed,
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        },
        "chunks": chunks
    }

    # Write manifest
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    print(f"Rebuilt manifest with {len(chunks)} chunks:")
    for chunk in chunks:
        print(f"  - Rows {chunk['start_row']:,}–{chunk['end_row']:,}: {chunk['json']}")
    if isinstance(total_dataset_rows, int):
        print(f"Total: {total_processed:,} rows processed out of {total_dataset_rows:,} dataset rows")
    else:
        print(f"Total: {total_processed:,} rows processed")
    print(f"Manifest written to: {manifest_path}")


def main() -> None:
    args = parse_args()
    if args.list_models:
        list_models(args.endpoint, args.api_key, args.timeout)
        return

    if args.rebuild_manifest:
        rebuild_manifest(args.chunk_dir, args.chunk_manifest)
        return

    # Load system prompt from file or use inline/default
    system_prompt, prompt_source = load_system_prompt(args)

    if not args.input.exists():
        sys.exit(f"Input file not found: {args.input}")
    if args.start_row < 1:
        sys.exit("--start-row must be >= 1")
    if args.end_row is not None and args.end_row < args.start_row:
        sys.exit("--end-row must be greater than or equal to --start-row")
    # Always allow appending - no need to check for existing files
    # Files will be automatically appended if they exist

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.json_output.parent.mkdir(parents=True, exist_ok=True)
    if args.checkpoint:
        args.checkpoint.parent.mkdir(parents=True, exist_ok=True)

    # Updated fieldnames for qui tam
    fieldnames = [
        "filename",
        "source_row_index",
        "headline",
        "qui_tam_score",
        "reason",
        "key_facts",
        "statute_violations",
        "implicated_actors",
        "federal_programs_involved",
        "fraud_type",
        "investigation_viability_score",
        "investigation_report",
    ]
    if args.include_action_items:
        fieldnames.append("action_items")

    processed = 0
    # Always load existing filenames to prevent duplicates, regardless of --resume flag
    completed_filenames: Set[str] = set()
    if args.checkpoint and args.checkpoint.exists():
        completed_filenames |= load_checkpoint(args.checkpoint)
    if args.json_output.exists():
        completed_filenames |= load_jsonl_filenames(args.json_output)
    for extra_json in args.known_json:
        completed_filenames |= load_jsonl_filenames(Path(extra_json))
    if completed_filenames:
        print(f"Skipping {len(completed_filenames)} pre-processed records.")

    workload_stats = calculate_workload(
        args.input,
        max_rows=args.max_rows,
        completed_filenames=completed_filenames,  # Always use completed_filenames to prevent duplicates
        start_row=args.start_row,
        end_row=args.end_row,
    )
    total_candidates = workload_stats["total"]
    already_done = workload_stats["already_done"]
    target_total = workload_stats["workload"]
    if target_total <= 0:
        print("No new rows to process. Exiting.")
        return
    range_desc = f"rows {args.start_row}-{args.end_row if args.end_row else 'end'}"
    print(
        f"Processing {target_total} new rows within {range_desc} "
        f"(skipping {already_done} already completed, total considered {total_candidates})."
    )
    if args.min_score > 0:
        print(f"Post-filtering enabled: Skipping GPT results with score < {args.min_score}")
    # Investigation is always enabled for high-scoring leads (score >= 50)
    if not INVESTIGATION_AVAILABLE:
        print("WARNING: clinical_investigator module not available.", file=sys.stderr)
        print("Investigation will be skipped. Ensure clinical_investigator.py is in the same directory.", file=sys.stderr)
    else:
        print(f"Investigation enabled: Will automatically investigate leads with score >= {args.investigate_min_score}")

    output_router = OutputRouter(args, fieldnames)

    # Count total rows in dataset for manifest metadata
    if output_router.mode == "chunk":
        print("Counting total rows in dataset...")
        output_router.total_dataset_rows = count_total_csv_rows(args.input)
        print(f"Total dataset: {output_router.total_dataset_rows:,} rows")

    checkpoint_handle = (
        args.checkpoint.open("a", encoding="utf-8") if args.checkpoint else None
    )

    # Build config metadata to include in requests and outputs
    config_metadata = build_config_metadata(args, prompt_source)

    start_time = time.monotonic()
    try:
        for idx, row in enumerate(iter_rows(args.input), start=1):
            if idx < args.start_row:
                continue
            if args.end_row is not None and idx > args.end_row:
                break
            if args.max_rows is not None and processed >= args.max_rows:
                break

            filename = row["filename"]
            text = row["text"]

            if filename in completed_filenames:
                print(f"[Row {idx}] [skip] {filename} already processed.", flush=True)
                continue

            # Show both source row index and processing progress
            if target_total:
                progress_prefix = f"[Row {idx}] [{processed + 1}/{target_total} new]"
            else:
                progress_prefix = f"[Row {idx}] [{processed + 1}]"

            eta_text = format_eta(
                start_time,
                processed,
                target_total,
                args.power_watts,
                args.electric_rate,
            )
            print(f"{progress_prefix} Processing {filename}... {eta_text}", flush=True)
            
            # Check if cross-referencing is enabled and database exists
            if args.cross_reference and args.db_path and args.db_path.exists():
                print(f"  → Cross-referencing with database: {args.db_path.name}", flush=True)

            try:
                result = call_model(
                    endpoint=args.endpoint,
                    model=args.model,
                    filename=filename,
                    text=text,
                    system_prompt=system_prompt,
                    api_key=args.api_key,
                    timeout=args.timeout,
                    temperature=args.temperature,
                    reasoning_effort=args.reasoning_effort,
                    config_metadata=config_metadata,
                    db_path=args.db_path if args.cross_reference else None,
                    enable_cross_reference=args.cross_reference,
                )
                
                # Filter based on GPT's NEW score (not the old scraper score)
                new_score = result.get("qui_tam_score", 0)
                if args.min_score > 0 and new_score < args.min_score:
                    print(f"  → Skipping {filename}: GPT score {new_score} < {args.min_score}", flush=True)
                    continue
                    
            except Exception as exc:  # noqa: BLE001
                print(f"  ! Failed to analyze {filename}: {exc}", file=sys.stderr)
                continue

            # Map scientific fraud fields to output format
            # New prompt uses "scientific_red_flags" instead of "key_facts"
            key_facts = normalize_text_list(ensure_list(
                result.get("scientific_red_flags") or result.get("key_facts")
            ))
            # New prompt doesn't explicitly list statute violations, derive from fraud_vector
            statute_violations = []
            fraud_vector = result.get("fraud_vector", "")
            if fraud_vector:
                if "NIH" in fraud_vector or "Grant" in fraud_vector:
                    statute_violations.append("False Claims Act (Grant Fraud)")
                if "FDA" in fraud_vector or "Clinical Trial" in fraud_vector:
                    statute_violations.append("False Claims Act (FDA Fraud)")
                if "Off-Label" in fraud_vector or "Marketing" in fraud_vector:
                    statute_violations.append("False Claims Act (Off-Label Marketing)")
                if "Kickback" in fraud_vector:
                    statute_violations.append("Anti-Kickback Statute")
            statute_violations = normalize_text_list(
                statute_violations or ensure_list(result.get("statute_violations"))
            )
            # New prompt uses "implicated_institutions" instead of "implicated_entities"
            implicated_actors = normalize_text_list(
                ensure_list(result.get("implicated_institutions") or result.get("implicated_entities")),
                strip_descriptor=True
            )
            # Derive federal programs from funding_source if available
            federal_programs_involved = []
            funding_source = result.get("funding_source", "")
            if funding_source:
                if "NIH" in funding_source:
                    federal_programs_involved.append("NIH")
                if "CDC" in funding_source:
                    federal_programs_involved.append("CDC")
                if "DoD" in funding_source or "DOD" in funding_source:
                    federal_programs_involved.append("DoD")
            federal_programs_involved = normalize_programs(
                federal_programs_involved or normalize_text_list(
                    ensure_list(result.get("federal_programs_involved")), strip_descriptor=True
                )
            )
            # Use fraud_vector if fraud_type not available
            fraud_type = result.get("fraud_type") or fraud_vector or "Unknown"
            
            # New prompt uses "next_step" instead of "action_items"
            action_items = (
                normalize_text_list(ensure_list(
                    result.get("next_step") or result.get("action_items")
                ))
                if args.include_action_items
                else []
            )

            # Build reason from potential_damages_theory and investigation_status
            reason_parts = []
            if result.get("potential_damages_theory"):
                reason_parts.append(result.get("potential_damages_theory"))
            if result.get("investigation_status"):
                reason_parts.append(f"Status: {result.get('investigation_status')}")
            if result.get("reason"):
                reason_parts.append(result.get("reason"))
            reason = " | ".join(reason_parts) if reason_parts else result.get("reason", "")
            
            # Perform clinical investigation for high-scoring leads (automatically enabled)
            investigation_report = None
            investigation_viability_score = None
            if INVESTIGATION_AVAILABLE and new_score >= args.investigate_min_score:
                print(f"  → Investigating lead (score {new_score} >= {args.investigate_min_score})...", flush=True)
                try:
                    # Extract identifiers from filename and text for better investigation
                    import re
                    nct_ids = re.findall(r'NCT\d{8}', filename + " " + text)
                    pmids = re.findall(r'PMID[:\s]*(\d{8,})|/(\d{8,})/', filename + " " + text)
                    pmid_list = [pmid[0] or pmid[1] for pmid in pmids if pmid[0] or pmid[1]]
                    
                    # Prepare lead data for investigation with original data
                    lead_data = {
                        "headline": result.get("headline", ""),
                        "qui_tam_score": new_score,
                        "key_facts": "; ".join(key_facts) if key_facts else "",
                        "fraud_type": fraud_type,
                        "implicated_actors": "; ".join(implicated_actors) if implicated_actors else "",
                        "federal_programs_involved": "; ".join(federal_programs_involved) if federal_programs_involved else "",
                        "reason": reason,
                        "filename": filename,  # Original filename (may contain NCT ID, PMID, etc.)
                        "original_text": text[:2000] if text else "",  # First 2000 chars of original text for context
                        "nct_ids": nct_ids,  # Extracted NCT IDs
                        "pmids": pmid_list[:5],  # Extracted PMIDs (limit to 5)
                    }
                    investigation_result = investigate_lead(lead_data)
                    investigation_report = investigation_result.get("report", "")
                    investigation_viability_score = investigation_result.get("viability_score", 0)
                    print(f"  ✓ Investigation complete. Viability score: {investigation_viability_score}", flush=True)
                except Exception as exc:
                    print(f"  ! Investigation failed: {exc}", file=sys.stderr, flush=True)
                    investigation_report = f"# Investigation Error\n\nInvestigation failed: {str(exc)}"
                    investigation_viability_score = None
            
            csv_row = {
                "filename": filename,
                "source_row_index": idx,
                "headline": result.get("headline", ""),
                "qui_tam_score": result.get("qui_tam_score", ""),
                "reason": reason,
                "key_facts": "; ".join(key_facts),
                "statute_violations": "; ".join(statute_violations),
                "implicated_actors": "; ".join(implicated_actors),
                "federal_programs_involved": "; ".join(federal_programs_involved),
                "fraud_type": fraud_type,
                "investigation_viability_score": investigation_viability_score if investigation_viability_score is not None else "",
                "investigation_report": (investigation_report[:500] + "...") if investigation_report and len(investigation_report) > 500 else (investigation_report or ""),  # Truncate for CSV
            }
            if args.include_action_items:
                csv_row["action_items"] = "; ".join(action_items)

            json_record: Dict[str, Any] = {
                "filename": filename,
                "headline": result.get("headline", ""),
                "qui_tam_score": result.get("qui_tam_score", ""),
                "reason": reason,
                "key_facts": key_facts,
                "statute_violations": statute_violations,
                "implicated_actors": implicated_actors,
                "federal_programs_involved": federal_programs_involved,
                "fraud_type": fraud_type,
                "investigation_viability_score": investigation_viability_score,
                "investigation_report": investigation_report,
                "metadata": {
                    "source_row_index": idx,
                    "original_row": row,
                    "config": config_metadata,
                    # Include new fields for reference
                    "fraud_vector": fraud_vector,
                    "funding_source": result.get("funding_source", ""),
                    "investigation_status": result.get("investigation_status", ""),
                },
            }
            if args.include_action_items:
                json_record["action_items"] = action_items

            output_router.write(idx, csv_row, json_record)

            if checkpoint_handle:
                checkpoint_handle.write(filename + "\n")
                checkpoint_handle.flush()

            completed_filenames.add(filename)
            processed += 1
            if args.sleep:
                time.sleep(args.sleep)
                
    finally:
        if checkpoint_handle:
            checkpoint_handle.close()
        output_router.close()

    elapsed = time.monotonic() - start_time
    elapsed_hours = elapsed / 3600
    cost_summary = format_cost_summary(args.power_watts, args.electric_rate, elapsed_hours, args.run_hours)
    if args.chunk_size > 0:
        complete_msg = (
            f"Completed {processed} new rows in {format_duration(elapsed)}. "
            f"Chunks saved under {args.chunk_dir} | manifest: {args.chunk_manifest}"
        )
    else:
        complete_msg = (
            f"Completed {processed} new rows in {format_duration(elapsed)}. "
            f"CSV saved to {args.output} | JSONL appended to {args.json_output}"
        )
    if cost_summary:
        complete_msg = f"{complete_msg}\n{cost_summary}"
    print(complete_msg)


if __name__ == "__main__":
    main()