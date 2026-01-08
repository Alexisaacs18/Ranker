#!/usr/bin/env python3
"""
Convert JSONL medical fraud data to CSV format for GPT ranker

The ranker expects CSV with 'filename' and 'text' columns.
This script converts the JSONL output from the medical fraud scraper.
"""

import json
import csv
import sys
from pathlib import Path
from typing import Dict, Any

# Fix CSV field size limit for large text fields
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)


def jsonl_to_csv(jsonl_path: Path, csv_path: Path, verbose: bool = True) -> int:
    """
    Convert JSONL file to CSV format expected by ranker.
    
    Args:
        jsonl_path: Path to input JSONL file
        csv_path: Path to output CSV file
        verbose: Print progress messages
        
    Returns:
        Number of records converted
    """
    if not jsonl_path.exists():
        raise FileNotFoundError(f"Input file not found: {jsonl_path}")
    
    records_converted = 0
    
    # Create output directory if needed
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    with jsonl_path.open('r', encoding='utf-8') as infile, \
         csv_path.open('w', newline='', encoding='utf-8') as outfile:
        
        writer = csv.DictWriter(outfile, fieldnames=['filename', 'text'])
        writer.writeheader()
        
        for line_num, line in enumerate(infile, start=1):
            line = line.strip()
            if not line:
                continue
                
            try:
                record = json.loads(line)
                
                # Generate filename from record ID
                filename = record.get('id', f'record_{line_num}')
                
                # Build text from record data
                text = format_record_as_text(record)
                
                writer.writerow({
                    'filename': filename,
                    'text': text
                })
                
                records_converted += 1
                
                if verbose and records_converted % 50 == 0:
                    print(f"  Converted {records_converted} records...", flush=True)
                    
            except json.JSONDecodeError as e:
                print(f"Warning: Skipping invalid JSON on line {line_num}: {e}", 
                      file=sys.stderr)
                continue
            except Exception as e:
                print(f"Warning: Error processing line {line_num}: {e}", 
                      file=sys.stderr)
                continue
    
    return records_converted


def format_record_as_text(record: Dict[str, Any]) -> str:
    """
    Format a JSON record as readable text for analysis.
    
    This creates a narrative text that the GPT ranker can analyze for
    qui tam potential.
    """
    lines = []
    
    # Try to get source from metadata field first, then top-level
    metadata = record.get('metadata', {})
    if isinstance(metadata, dict):
        source = metadata.get('source', record.get('source', 'Unknown Source'))
    else:
        source = record.get('source', 'Unknown Source')
    
    lines.append(f"SOURCE: {source}")
    lines.append("")
    
    # Format based on source type
    if source == "CMS LEIE":
        lines.extend(format_leie_record(metadata if metadata else record))
    elif source == "DOJ":
        lines.extend(format_doj_record(metadata if metadata else record))
    elif source == "CMS Open Payments":
        lines.extend(format_open_payments_record(metadata if metadata else record))
    elif source == "FDA Warning Letters":
        lines.extend(format_fda_warning_record(metadata if metadata else record))
    elif source == "FDA Devices":
        lines.extend(format_fda_device_record(metadata if metadata else record))
    elif source == "FDA Device Recalls":
        lines.extend(format_device_recall_record(metadata if metadata else record))
    elif source == "ClinicalTrials.gov":
        lines.extend(format_clinical_trial_record(metadata if metadata else record))
    elif source == "SEC EDGAR":
        lines.extend(format_sec_record(metadata if metadata else record))
    elif source == "FDA FAERS":
        lines.extend(format_faers_record(metadata if metadata else record))
    else:
        # Generic format for unknown sources
        # Use the 'text' field if it exists (from scraper output)
        if 'text' in record and isinstance(record['text'], str):
            lines.append(record['text'])
        else:
            lines.extend(format_generic_record(metadata if metadata else record))
    
    return "\n".join(lines)


def format_leie_record(record: Dict[str, Any]) -> list:
    """Format CMS LEIE exclusion record."""
    lines = []
    
    # Provider info
    first_name = record.get('first_name', '')
    last_name = record.get('last_name', '')
    business = record.get('business_name', '')
    
    if first_name or last_name:
        lines.append(f"PROVIDER: {first_name} {last_name}".strip())
    if business:
        lines.append(f"BUSINESS: {business}")
    
    # Exclusion details
    lines.append(f"EXCLUSION TYPE: {record.get('exclusion_type', 'Unknown')}")
    lines.append(f"EXCLUSION DATE: {record.get('exclusion_date', 'Unknown')}")
    lines.append(f"STATE: {record.get('state', 'Unknown')}")
    lines.append(f"SPECIALTY: {record.get('specialty', 'Unknown')}")
    lines.append(f"NPI: {record.get('npi', 'Unknown')}")
    
    return lines


def format_doj_record(record: Dict[str, Any]) -> list:
    """Format DOJ press release/settlement record."""
    lines = []
    
    lines.append(f"TITLE: {record.get('title', 'Untitled')}")
    lines.append(f"DATE: {record.get('date', 'Unknown')}")
    lines.append(f"SETTLEMENT AMOUNT: {record.get('settlement_amount', 'Unknown')}")
    lines.append("")
    lines.append("CASE DETAILS:")
    
    content = record.get('content', '')
    # Limit content to first 2000 characters for ranker
    if len(content) > 2000:
        content = content[:2000] + "..."
    lines.append(content)
    
    if record.get('url'):
        lines.append("")
        lines.append(f"URL: {record['url']}")
    
    return lines


def format_open_payments_record(record: Dict[str, Any]) -> list:
    """Format CMS Open Payments record."""
    lines = []
    
    lines.append(f"PHYSICIAN: {record.get('physician_name', 'Unknown')}")
    lines.append(f"SPECIALTY: {record.get('physician_specialty', 'Unknown')}")
    lines.append(f"PAYMENT AMOUNT: ${record.get('amount', 0):,.2f}")
    lines.append(f"PAYMENT TYPE: {record.get('payment_nature', 'Unknown')}")
    lines.append(f"FROM COMPANY: {record.get('submitting_entity', 'Unknown')}")
    lines.append(f"PAYMENT DATE: {record.get('payment_date', 'Unknown')}")
    lines.append(f"PRODUCT: {record.get('product_name', 'Unknown')}")
    
    return lines


def format_fda_warning_record(record: Dict[str, Any]) -> list:
    """Format FDA warning letter record."""
    lines = []
    
    lines.append(f"TITLE: {record.get('title', 'Untitled')}")
    lines.append(f"DATE: {record.get('date', 'Unknown')}")
    lines.append("")
    lines.append("VIOLATION DETAILS:")
    lines.append(record.get('description', 'No description available'))
    
    if record.get('url'):
        lines.append("")
        lines.append(f"URL: {record['url']}")
    
    return lines


def format_fda_device_record(record: Dict[str, Any]) -> list:
    """Format FDA device clearance record."""
    lines = []
    
    lines.append(f"DEVICE: {record.get('device_name', 'Unknown')}")
    lines.append(f"APPLICANT: {record.get('applicant', 'Unknown')}")
    lines.append(f"DECISION: {record.get('decision_description', 'Unknown')}")
    lines.append(f"DECISION DATE: {record.get('decision_date', 'Unknown')}")
    lines.append(f"PRODUCT CODE: {record.get('product_code', 'Unknown')}")
    lines.append(f"K NUMBER: {record.get('k_number', 'Unknown')}")
    
    return lines


def format_device_recall_record(record: Dict[str, Any]) -> list:
    """Format FDA device recall record."""
    lines = []
    
    lines.append(f"PRODUCT: {record.get('product_description', 'Unknown')}")
    lines.append(f"RECALL REASON: {record.get('reason_for_recall', 'Unknown')}")
    lines.append(f"CLASSIFICATION: {record.get('classification', 'Unknown')}")
    lines.append(f"STATUS: {record.get('recall_status', 'Unknown')}")
    lines.append(f"RECALLING FIRM: {record.get('recalling_firm', 'Unknown')}")
    lines.append(f"RECALL DATE: {record.get('recall_date', 'Unknown')}")
    
    return lines


def format_clinical_trial_record(record: Dict[str, Any]) -> list:
    """Format ClinicalTrials.gov record."""
    lines = []
    
    lines.append(f"NCT ID: {record.get('id', 'Unknown')}")
    lines.append(f"TITLE: {record.get('title', 'Untitled')}")
    lines.append(f"CONDITION: {record.get('condition', 'Unknown')}")
    lines.append(f"SPONSOR: {record.get('sponsor', 'Unknown')}")
    lines.append(f"START DATE: {record.get('start_date', 'Unknown')}")
    lines.append(f"COMPLETION DATE: {record.get('completion_date', 'Unknown')}")
    
    return lines


def format_sec_record(record: Dict[str, Any]) -> list:
    """Format SEC EDGAR filing record."""
    lines = []
    
    lines.append(f"COMPANY: {record.get('company', 'Unknown')}")
    lines.append(f"CIK: {record.get('cik', 'Unknown')}")
    lines.append(f"FORM TYPE: {record.get('form_type', 'Unknown')}")
    lines.append(f"FILING DATE: {record.get('filing_date', 'Unknown')}")
    lines.append(f"ACCESSION NUMBER: {record.get('accession_number', 'Unknown')}")
    
    if record.get('url'):
        lines.append(f"URL: {record['url']}")
    
    return lines


def format_faers_record(record: Dict[str, Any]) -> list:
    """Format FDA FAERS adverse event record."""
    lines = []
    
    lines.append(f"DRUG: {record.get('drug_name', 'Unknown')}")
    lines.append(f"ADVERSE REACTION: {record.get('reaction', 'Unknown')}")
    lines.append(f"SERIOUS: {record.get('serious', 'Unknown')}")
    lines.append(f"OUTCOME: {record.get('outcome', 'Unknown')}")
    lines.append(f"REPORT DATE: {record.get('report_date', 'Unknown')}")
    
    return lines


def format_generic_record(record: Dict[str, Any]) -> list:
    """Generic formatter for unknown record types."""
    lines = []
    
    # Format all non-id, non-source fields
    for key, value in record.items():
        if key in ('id', 'source', 'metadata'):
            continue
        
        # Format key nicely
        formatted_key = key.replace('_', ' ').upper()
        lines.append(f"{formatted_key}: {value}")
    
    return lines


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Convert JSONL medical fraud data to CSV format for GPT ranker"
    )
    parser.add_argument(
        '--input',
        type=Path,
        default=Path('data/processed/combined_medical_fraud_data.jsonl'),
        help='Input JSONL file'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=Path('data/processed/combined_qui_tam_data.csv'),
        help='Output CSV file'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress progress messages'
    )
    
    args = parser.parse_args()
    
    print(f"Converting {args.input} to {args.output}...")
    
    try:
        count = jsonl_to_csv(args.input, args.output, verbose=not args.quiet)
        print(f"\n✅ Successfully converted {count} records")
        print(f"   Output: {args.output}")
        print(f"\nNext step: Run your ranker with:")
        print(f"   python gpt_ranker.py --input {args.output}")
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()