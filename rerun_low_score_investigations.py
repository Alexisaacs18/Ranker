#!/usr/bin/env python3
"""
Rerun clinical investigations for all leads with investigation_viability_score < 50.
This script reads the ranked CSV, finds all rows with low investigation scores,
and reruns the investigation using the updated clinical_investigator code.
"""

import csv
import sys
from pathlib import Path
from typing import Dict, Any, List

# Import the investigation function
try:
    from clinical_investigator import investigate_lead
except ImportError:
    print("ERROR: Could not import clinical_investigator module", file=sys.stderr)
    sys.exit(1)

def load_csv_rows(csv_path: Path) -> List[Dict[str, Any]]:
    """Load all rows from CSV file."""
    rows = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        print(f"Error reading CSV: {e}", file=sys.stderr)
        sys.exit(1)
    return rows

def prepare_lead_data(row: Dict[str, Any], original_text: str = "") -> Dict[str, Any]:
    """Prepare lead data for investigation from CSV row."""
    import re
    import json
    
    # Extract identifiers from filename and text
    filename = row.get('filename', '') or ''
    
    # Try to get original_text from metadata if available
    text = original_text
    if not text:
        metadata_str = row.get('metadata', '')
        if metadata_str:
            try:
                if isinstance(metadata_str, str):
                    metadata = json.loads(metadata_str)
                else:
                    metadata = metadata_str
                text = metadata.get('original_row', {}).get('text', '') or ''
            except (json.JSONDecodeError, AttributeError, TypeError) as e:
                # Silently continue if metadata parsing fails
                pass
    
    # Fallback to original_text column if available
    if not text:
        text = row.get('original_text', '') or ''
    
    combined_text = (filename + " " + text).strip()
    
    # Extract identifiers safely
    nct_ids = []
    pmids = []
    try:
        nct_ids = re.findall(r'NCT\d{8}', combined_text)
        pmids_raw = re.findall(r'PMID[:\s]*(\d{8,})|/(\d{8,})/', combined_text)
        pmids = [pmid[0] or pmid[1] for pmid in pmids_raw if (pmid[0] or pmid[1])]
    except Exception:
        # If regex fails, continue with empty lists
        pass
    
    # Parse key_facts if it's a string
    key_facts = []
    try:
        key_facts_str = row.get('key_facts', '')
        if isinstance(key_facts_str, str) and key_facts_str:
            key_facts = [f.strip() for f in key_facts_str.split(';') if f.strip()]
    except Exception:
        pass
    
    # Parse implicated_actors if it's a string
    implicated_actors = []
    try:
        implicated_str = row.get('implicated_actors', '')
        if isinstance(implicated_str, str) and implicated_str:
            implicated_actors = [a.strip() for a in implicated_str.split(';') if a.strip()]
    except Exception:
        pass
    
    # Parse federal_programs_involved if it's a string
    federal_programs = []
    try:
        federal_str = row.get('federal_programs_involved', '')
        if isinstance(federal_str, str) and federal_str:
            federal_programs = [p.strip() for p in federal_str.split(';') if p.strip()]
    except Exception:
        pass
    
    # Safely parse qui_tam_score
    qui_tam_score = 0
    try:
        qui_tam_str = row.get('qui_tam_score', '')
        if qui_tam_str:
            qui_tam_score = int(qui_tam_str)
    except (ValueError, TypeError):
        pass
    
    lead_data = {
        "headline": row.get('headline', '') or '',
        "qui_tam_score": qui_tam_score,
        "key_facts": "; ".join(key_facts) if key_facts else "",
        "fraud_type": row.get('fraud_type', '') or '',
        "implicated_actors": "; ".join(implicated_actors) if implicated_actors else "",
        "federal_programs_involved": "; ".join(federal_programs) if federal_programs else "",
        "reason": row.get('reason', '') or '',
        "filename": filename,
        "original_text": text[:2000] if text else "",  # First 2000 chars
        "nct_ids": nct_ids,
        "pmids": pmids[:5],  # Limit to 5 PMIDs
    }
    
    return lead_data

def update_csv_and_jsonl(csv_path: Path, jsonl_path: Path, source_row_index: int, investigation_report: str, investigation_viability_score: int):
    """Update a specific row in both CSV and JSONL with new investigation data."""
    import json
    import tempfile
    import shutil
    
    if not investigation_report:
        print(f"Warning: Empty investigation report, skipping update", file=sys.stderr)
        return False
    
    # Update CSV
    rows = []
    updated_csv = False
    
    try:
        # Read all rows
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            if not fieldnames:
                print(f"Error: CSV has no headers", file=sys.stderr)
                return False
            rows = list(reader)
        
        # Find and update the row in CSV
        for row in rows:
            try:
                row_source_idx = row.get('source_row_index', '')
                if row_source_idx:
                    if int(row_source_idx) == source_row_index:
                        # Truncate report for CSV (500 chars max)
                        truncated_report = (investigation_report[:500] + "...") if len(investigation_report) > 500 else investigation_report
                        row['investigation_report'] = truncated_report
                        row['investigation_viability_score'] = str(investigation_viability_score) if investigation_viability_score is not None else ""
                        updated_csv = True
                        break
            except (ValueError, TypeError) as e:
                # Skip rows with invalid source_row_index
                continue
        
        if not updated_csv:
            print(f"Warning: Could not find row with source_row_index={source_row_index} in CSV", file=sys.stderr)
            return False
        
        # Write back to CSV using temp file for safety
        temp_csv = csv_path.with_suffix('.csv.tmp')
        try:
            with open(temp_csv, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            # Atomic replace
            shutil.move(str(temp_csv), str(csv_path))
        except Exception as e:
            if temp_csv.exists():
                temp_csv.unlink()
            raise
    
    except Exception as e:
        print(f"Error updating CSV: {e}", file=sys.stderr)
        return False
    
    # Update JSONL (optional - don't fail if JSONL doesn't exist or update fails)
    if jsonl_path.exists():
        try:
            updated_jsonl = False
            jsonl_lines = []
            
            with open(jsonl_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            record = json.loads(line)
                            record_source_idx = record.get('metadata', {}).get('source_row_index')
                            if record_source_idx == source_row_index:
                                record['investigation_report'] = investigation_report
                                record['investigation_viability_score'] = investigation_viability_score
                                updated_jsonl = True
                            jsonl_lines.append(record)
                        except json.JSONDecodeError:
                            # Skip invalid JSON lines
                            continue
            
            if updated_jsonl:
                # Write back to JSONL using temp file
                temp_jsonl = jsonl_path.with_suffix('.jsonl.tmp')
                try:
                    with open(temp_jsonl, 'w', encoding='utf-8') as f:
                        for record in jsonl_lines:
                            f.write(json.dumps(record, ensure_ascii=False) + '\n')
                    # Atomic replace
                    shutil.move(str(temp_jsonl), str(jsonl_path))
                except Exception as e:
                    if temp_jsonl.exists():
                        temp_jsonl.unlink()
                    raise
        except Exception as e:
            # Don't fail if JSONL update fails - CSV update is more important
            print(f"Warning: Could not update JSONL: {e}", file=sys.stderr)
    
    return updated_csv

def main():
    csv_path = Path("data/results/qui_tam_ranked.csv")
    
    if not csv_path.exists():
        print(f"ERROR: CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Loading CSV: {csv_path}")
    rows = load_csv_rows(csv_path)
    
    # Find rows with low investigation scores (< 50) or missing scores
    # IMPORTANT: Only process scores that are < 50, NOT scores >= 50
    low_score_rows = []
    for idx, row in enumerate(rows):
        investigation_score = row.get('investigation_viability_score', '')
        try:
            score = int(investigation_score) if investigation_score else None
            # Only include if score is None (missing) or strictly less than 50
            # Explicitly exclude scores >= 50
            if score is None:
                low_score_rows.append((idx, row))
            elif score < 50:
                low_score_rows.append((idx, row))
            # If score >= 50, skip it (do not add to low_score_rows)
        except (ValueError, TypeError):
            # If score is not a number, treat as missing/low
            low_score_rows.append((idx, row))
    
    print(f"Found {len(low_score_rows)} rows with investigation_viability_score < 50 or missing")
    print(f"(Skipping rows with investigation_viability_score >= 50)")
    
    if not low_score_rows:
        print("No rows to rerun. Exiting.")
        return
    
    # Show summary (no interactive prompt - can run from UI)
    print(f"\nThis will rerun investigations for {len(low_score_rows)} leads.")
    print("Starting rerun...")
    
    # Process each row
    successful = 0
    failed = 0
    
    for idx, (row_idx, row) in enumerate(low_score_rows, 1):
        source_row_index = row.get('source_row_index', '')
        headline = row.get('headline', 'N/A')[:60]
        
        print(f"\n[{idx}/{len(low_score_rows)}] Processing source_row_index={source_row_index}: {headline}...")
        
        try:
            # Validate required fields
            if not source_row_index:
                print(f"  ✗ Skipping: Missing source_row_index", file=sys.stderr)
                failed += 1
                continue
            
            try:
                source_row_index_int = int(source_row_index)
            except (ValueError, TypeError):
                print(f"  ✗ Skipping: Invalid source_row_index '{source_row_index}'", file=sys.stderr)
                failed += 1
                continue
            
            # Prepare lead data (will extract original_text from metadata automatically)
            try:
                lead_data = prepare_lead_data(row)
            except Exception as e:
                print(f"  ✗ Error preparing lead data: {e}", file=sys.stderr)
                import traceback
                traceback.print_exc()
                failed += 1
                continue
            
            # Run investigation
            try:
                investigation_result = investigate_lead(lead_data)
                investigation_report = investigation_result.get("report", "")
                investigation_viability_score = investigation_result.get("viability_score", 0)
                
                if not investigation_report:
                    print(f"  ✗ Warning: No report generated, skipping update", file=sys.stderr)
                    failed += 1
                    continue
                    
            except Exception as e:
                print(f"  ✗ Error during investigation: {e}", file=sys.stderr)
                import traceback
                traceback.print_exc()
                failed += 1
                continue
            
            # Update CSV and JSONL
            try:
                jsonl_path = Path("data/results/qui_tam_ranked.jsonl")
                if update_csv_and_jsonl(csv_path, jsonl_path, source_row_index_int, investigation_report, investigation_viability_score):
                    print(f"  ✓ Updated: viability_score={investigation_viability_score}")
                    successful += 1
                else:
                    print(f"  ✗ Failed to update files (row not found?)", file=sys.stderr)
                    failed += 1
            except Exception as e:
                print(f"  ✗ Error updating files: {e}", file=sys.stderr)
                import traceback
                traceback.print_exc()
                failed += 1
                
        except Exception as e:
            print(f"  ✗ Unexpected error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            failed += 1
    
    print(f"\n{'='*60}")
    print(f"Completed: {successful} successful, {failed} failed")
    print(f"Updated CSV: {csv_path}")

if __name__ == "__main__":
    main()
