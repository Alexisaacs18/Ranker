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
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)


#!/usr/bin/env python3
"""
Enhanced Qui Tam Ranking Prompt
Focuses on NON-FILED cases with high potential
"""

QUI_TAM_RANKING_PROMPT = """You are a qui tam whistleblower investigator analyzing healthcare provider records to identify UNFILED False Claims Act violations with high success potential.

CRITICAL CONTEXT: Score cases based on their potential for NEW qui tam lawsuits. If a case has already been settled or publicly disclosed, it CANNOT be filed as a new qui tam case (first-to-file rule).

SCORING FRAMEWORK (0-100):

90-100 (SLAM DUNK - File immediately):
- Clear Medicare/Medicaid billing fraud with dollar amounts documented
- Systematic pattern (not isolated) - "routine practice", "standard procedure"
- Recent (within 3 years) - statute of limitations is 6 years but fresher is better
- Large damages ($5M+ potential government losses)
- Strong evidence: billing records, claims data, internal documents mentioned
- NOT already publicly disclosed or settled
- Corporate/institutional defendant (not just individual)
- Multiple fraud types (kickbacks + upcoding, etc.)

70-89 (STRONG POTENTIAL - Investigate further):
- Medicare/Medicaid involvement clearly stated
- Pattern of fraudulent billing across multiple patients
- Damages likely exceed $1M
- Within 5 year window
- Fraud type: kickbacks, upcoding, phantom billing, unnecessary procedures
- Some documentation exists
- NOT already part of known settlement or investigation

50-69 (MEDIUM POTENTIAL - Needs more evidence):
- Federal healthcare program involvement suggested but not explicit
- Some pattern indicators ("multiple patients", "over time")
- Potential damages unclear but possibly significant
- Quality of care issues tied to billing
- May need inside whistleblower with documents

30-49 (LOW PRIORITY):
- Vague connection to federal programs
- Isolated incidents
- Old (5-6 years ago)
- Small dollar amounts
- Weak evidence

0-29 (NOT VIABLE):
- No federal program involvement
- Private insurance only
- Pure malpractice (no fraud)
- Already disclosed publicly
- Beyond statute of limitations (>6 years)
- State-only violations

CRITICAL ANALYSIS - CHECK THESE RED FLAGS:

🚫 ALREADY FILED/SETTLED (Score 0-10):
- "settled for $X million" → ALREADY RESOLVED, score 0-5
- "DOJ announced" or "Justice Department" → PUBLICLY DISCLOSED, score 0-10
- "agreed to pay" or "settlement agreement" → CASE CLOSED, score 5
- "investigation revealed" or "FBI probe" → POSSIBLY PUBLIC, score 10-20
- "former employee reported" or "whistleblower lawsuit" → ALREADY FILED, score 0

✅ HIGH VALUE INDICATORS (Boost score by +20-30):
- "billed Medicare for services not provided"
- "submitted false claims to Medicaid"
- "routine practice of upcoding"
- "kickbacks from pharmaceutical companies"
- "unnecessary procedures to increase billing"
- Specific dollar amounts mentioned in billing
- Multiple years of fraudulent activity
- Corporate policy or directive mentioned

⚖️ STATUTE VIOLATIONS THAT MATTER:
- False Claims Act (31 U.S.C. § 3729) - THE BIG ONE
- Anti-Kickback Statute (42 U.S.C. § 1320a-7b)
- Stark Law (42 U.S.C. § 1395nn)
- FDCA violations tied to Medicare/Medicaid billing

OUTPUT FORMAT (JSON):
{
    "headline": "One sentence describing the unfiled qui tam opportunity",
    "qui_tam_score": 0-100,
    "case_status": "unfiled" | "filed" | "settled" | "under_investigation" | "unknown",
    "reason": "2-3 sentence explanation of score and viability",
    "fraud_type": "Primary type: kickbacks | upcoding | phantom billing | unnecessary procedures | off-label marketing | other",
    "federal_programs_involved": ["Medicare Part A", "Medicare Part B", "Medicare Part D", "Medicaid", "TRICARE", "VA"],
    "estimated_damages": "Best estimate in dollars or 'unknown' - be specific if amounts mentioned",
    "statute_violations": ["False Claims Act", "Anti-Kickback Statute", "Stark Law", "FDCA"],
    "evidence_strength": "strong" | "medium" | "weak",
    "key_facts": ["3-5 specific facts supporting qui tam case - focus on dollar amounts, patterns, timeframes"],
    "implicated_entities": ["Provider name", "Healthcare system", "Company name"],
    "time_period": "When fraud occurred (critical for 6-year statute)",
    "public_disclosure": "yes" | "no" | "possible" - has this been publicly reported/settled?,
    "first_to_file_viable": "yes" | "no" | "unclear" - can someone still file on this?,
    "red_flags": ["List any indicators this is already filed, settled, or too old"],
    "green_flags": ["List any indicators this is a strong unfiled opportunity"],
    "next_steps": "What a potential relator should do to file this case",
    "qui_tam_viability": "high" | "medium" | "low",
    "relator_requirements": "What type of insider knowledge/documents would strengthen this case"
}

SCORING RULES - FOLLOW STRICTLY:

1. If record mentions "settlement", "agreed to pay", "resolved" → Score ≤10 (already filed)
2. If record from DOJ press release or government announcement → Score ≤15 (publicly disclosed)
3. If >6 years old → Score ≤20 (statute of limitations)
4. If no Medicare/Medicaid mention → Score ≤30 (not federal program)
5. If isolated incident ("one patient") → Score ≤40 (need pattern)
6. If damages <$500k → Score ≤50 (too small for qui tam)

BOOST SCORING FOR:
- Explicit billing amounts: +10 points
- "Routine" or "systematic" or "standard practice": +15 points
- Multiple fraud types combined: +10 points
- Corporate defendant with deep pockets: +10 points
- Recent (within 2 years): +15 points
- Clear documentation mentioned: +10 points

EXAMPLES:

EXAMPLE 1 - HIGH SCORE (95):
Record: "Dr. Smith's clinic routinely billed Medicare Part B for comprehensive office visits (CPT 99215) when only basic visits (99213) were provided. Billing records from 2022-2024 show 847 instances of upcoding across 200+ Medicare patients. Clinic administrator noted this was 'standard practice to maximize reimbursement.' Estimated excess billings: $180,000."

Response:
{
    "headline": "Systematic Medicare upcoding scheme with documented pattern across 200+ patients and $180K in damages",
    "qui_tam_score": 95,
    "case_status": "unfiled",
    "reason": "Clear pattern of systematic upcoding with specific dollar amounts, recent timeframe, documentation exists, and no public disclosure. Strong False Claims Act case.",
    "fraud_type": "upcoding",
    "public_disclosure": "no",
    "first_to_file_viable": "yes",
    "red_flags": [],
    "green_flags": ["Specific dollar amount", "Documented pattern", "Recent", "Multiple patients", "Evidence exists"]
}

EXAMPLE 2 - LOW SCORE (5):
Record: "In 2018, XYZ Health settled with DOJ for $3.2 million for billing Medicare for unnecessary cardiac procedures. The settlement resolved allegations that the hospital performed medically unnecessary stent placements from 2015-2017."

Response:
{
    "headline": "Already settled qui tam case - not viable for new filing",
    "qui_tam_score": 5,
    "case_status": "settled",
    "reason": "This case has already been settled with DOJ. First-to-file rule prevents new qui tam cases on publicly disclosed fraud.",
    "fraud_type": "unnecessary procedures",
    "public_disclosure": "yes",
    "first_to_file_viable": "no",
    "red_flags": ["Already settled", "DOJ settlement announced", "Publicly disclosed"],
    "green_flags": []
}

EXAMPLE 3 - MEDIUM SCORE (65):
Record: "State medical board suspended Dr. Johnson's license for 'improper billing practices' in 2023. Board documents mention 'concerns raised about billing for services not documented' and multiple patient complaints about charges for visits that didn't occur."

Response:
{
    "headline": "Potential phantom billing case but needs verification of Medicare involvement",
    "qui_tam_score": 65,
    "case_status": "unknown",
    "reason": "State action suggests billing fraud, but federal program involvement not confirmed. Need to verify if Medicare/Medicaid patients were affected. Recent and pattern suggested.",
    "fraud_type": "phantom billing",
    "public_disclosure": "possible",
    "first_to_file_viable": "unclear",
    "red_flags": ["Federal program involvement unclear", "State board action may have made it public"],
    "green_flags": ["Recent", "Pattern suggested", "Multiple patients"]
}

Now analyze this medical provider record for qui tam potential:
"""

DEFAULT_SYSTEM_PROMPT = QUI_TAM_RANKING_PROMPT


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
        default=50,
        help="Limit processing to the first N rows (useful for smoke-tests).",
    )
    parser.add_argument(
        "--min-score",
        type=int,
        default=50,
        help="Skip records with existing fraud_potential_score below this threshold (0 to disable).",
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
) -> Dict[str, Any]:
    """Send the document to the local GPT server and return parsed JSON."""
    payload = {
        "model": model,
        "temperature": temperature,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    "Analyze the following medical provider record and respond with the JSON schema "
                    "described in the system prompt.\n"
                    f"Record ID: {filename}\n"
                    "---------\n"
                    f"{text.strip()}\n"
                    "---------"
                ),
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
        csv_mode = "a" if self.args.resume and self.args.output.exists() else "w"
        self.args.output.parent.mkdir(parents=True, exist_ok=True)
        self.csv_handle = self.args.output.open(csv_mode, newline="", encoding="utf-8")
        self.csv_writer = csv.DictWriter(self.csv_handle, fieldnames=self.fieldnames)
        if csv_mode == "w":
            self.csv_writer.writeheader()
        json_mode = "a" if self.args.resume and self.args.json_output.exists() else "w"
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
    if args.chunk_size <= 0:
        if (
            args.output.exists()
            and not args.resume
            and not args.overwrite_output
        ):
            sys.exit(
                f"Output file {args.output} already exists. "
                "Use --resume to append/skip or --overwrite-output to replace."
            )
        if (
            args.json_output.exists()
            and not args.resume
            and not args.overwrite_output
        ):
            sys.exit(
                f"JSON output file {args.json_output} already exists. "
                "Use --resume to append/skip or --overwrite-output to replace."
            )

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
    ]
    if args.include_action_items:
        fieldnames.append("action_items")

    processed = 0
    completed_filenames: Set[str] = set()
    if args.resume:
        completed_filenames |= load_checkpoint(args.checkpoint)
        completed_filenames |= load_jsonl_filenames(args.json_output)
    for extra_json in args.known_json:
        completed_filenames |= load_jsonl_filenames(Path(extra_json))
    if completed_filenames:
        print(f"Skipping {len(completed_filenames)} pre-processed records.")

    workload_stats = calculate_workload(
        args.input,
        max_rows=args.max_rows,
        completed_filenames=completed_filenames if args.resume else set(),
        start_row=args.start_row,
        end_row=args.end_row,
    )
    total_candidates = workload_stats["total"]
    already_done = workload_stats["already_done"] if args.resume else 0
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
                )
                
                # Filter based on GPT's NEW score (not the old scraper score)
                new_score = result.get("qui_tam_score", 0)
                if args.min_score > 0 and new_score < args.min_score:
                    print(f"  → Skipping {filename}: GPT score {new_score} < {args.min_score}", flush=True)
                    continue
                    
            except Exception as exc:  # noqa: BLE001
                print(f"  ! Failed to analyze {filename}: {exc}", file=sys.stderr)
                continue

            # Updated field names for qui tam
            key_facts = normalize_text_list(ensure_list(result.get("key_facts")))
            statute_violations = normalize_text_list(ensure_list(result.get("statute_violations")))
            implicated_actors = normalize_text_list(
                ensure_list(result.get("implicated_entities")), strip_descriptor=True
            )
            federal_programs_involved = normalize_programs(
                normalize_text_list(ensure_list(result.get("federal_programs_involved")), strip_descriptor=True)
            )
            fraud_type = result.get("fraud_type", "Unknown")
            
            action_items = (
                normalize_text_list(ensure_list(result.get("action_items")))
                if args.include_action_items
                else []
            )

            csv_row = {
                "filename": filename,
                "source_row_index": idx,
                "headline": result.get("headline", ""),
                "qui_tam_score": result.get("qui_tam_score", ""),
                "reason": result.get("reason", ""),
                "key_facts": "; ".join(key_facts),
                "statute_violations": "; ".join(statute_violations),
                "implicated_actors": "; ".join(implicated_actors),
                "federal_programs_involved": "; ".join(federal_programs_involved),
                "fraud_type": fraud_type,
            }
            if args.include_action_items:
                csv_row["action_items"] = "; ".join(action_items)

            json_record: Dict[str, Any] = {
                "filename": filename,
                "headline": result.get("headline", ""),
                "qui_tam_score": result.get("qui_tam_score", ""),
                "reason": result.get("reason", ""),
                "key_facts": key_facts,
                "statute_violations": statute_violations,
                "implicated_actors": implicated_actors,
                "federal_programs_involved": federal_programs_involved,
                "fraud_type": fraud_type,
                "metadata": {
                    "source_row_index": idx,
                    "original_row": row,
                    "config": config_metadata,
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