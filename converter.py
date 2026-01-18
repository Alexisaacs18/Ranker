#!/usr/bin/env python3
"""
IMPROVED Convert JSONL to CSV - WITH FRAUD SCORE FILTERING
Properly handles FAERS, fraud indicators, and pre-formatted text
FILTERS OUT records with fraud_potential_score < 30
"""

import json
import csv
import sys
from pathlib import Path
from typing import Dict, Any, Optional

# Fix CSV field size limit
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)


def get_fraud_score(record: Dict[str, Any]) -> Optional[int]:
    """Extract fraud score from record, checking both top level and metadata."""
    # Check top level first
    if 'fraud_potential_score' in record:
        try:
            return int(record['fraud_potential_score'])
        except (ValueError, TypeError):
            pass
    
    # Check metadata
    metadata = record.get('metadata', {})
    if isinstance(metadata, dict) and 'fraud_potential_score' in metadata:
        try:
            return int(metadata['fraud_potential_score'])
        except (ValueError, TypeError):
            pass
    
    # No score found - return None to include it (don't filter out)
    return None


def jsonl_to_csv(jsonl_path: Path, csv_path: Path, verbose: bool = True, min_score: int = 50) -> tuple:
    """Convert JSONL to CSV format for ranker, filtering by fraud score.
    
    Returns:
        tuple: (records_converted, records_skipped)
    """
    if not jsonl_path.exists():
        raise FileNotFoundError(f"Input file not found: {jsonl_path}")
    
    records_converted = 0
    records_skipped = 0
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
                
                # FILTER BY FRAUD SCORE - COMMENTED OUT
                # fraud_score = get_fraud_score(record)
                # if fraud_score is not None and fraud_score < min_score:
                #     records_skipped += 1
                #     if verbose and records_skipped % 100 == 0:
                #         print(f"  Skipped {records_skipped} low-score records (< {min_score})...", flush=True)
                #     continue
                
                # Get filename
                filename = record.get('filename') or record.get('id', f'record_{line_num}')
                
                # Get or build text
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
    
    return records_converted, records_skipped


def format_record_as_text(record: Dict[str, Any]) -> str:
    """Format record as text for analysis."""
    
    # PRIORITY 1: Check if record already has pre-formatted 'text' field
    if 'text' in record and isinstance(record['text'], str) and len(record['text']) > 50:
        return record['text']
    
    # PRIORITY 2: Build text from metadata
    metadata = record.get('metadata', {})
    if isinstance(metadata, dict) and metadata:
        source = metadata.get('source', record.get('source', 'Unknown'))
    else:
        source = record.get('source', 'Unknown')
    
    lines = []
    lines.append(f"SOURCE: {source}")
    
    # Add case status if present
    case_status = metadata.get('case_status') or record.get('case_status')
    if case_status:
        lines.append(f"CASE STATUS: {case_status}")
    
    # Add fraud score if present
    fraud_score = metadata.get('fraud_potential_score') or record.get('fraud_potential_score')
    if fraud_score:
        lines.append(f"FRAUD POTENTIAL SCORE: {fraud_score}")
    
    lines.append("")
    
    # Add fraud indicators prominently
    fraud_indicators = metadata.get('fraud_indicators') or record.get('fraud_indicators')
    if fraud_indicators and isinstance(fraud_indicators, list):
        lines.append("FRAUD INDICATORS:")
        for indicator in fraud_indicators:
            lines.append(f"  - {indicator}")
        lines.append("")
    
    # Format based on source
    if source == "FDA FAERS":
        lines.extend(format_faers_record(metadata if metadata else record))
    elif source == "CMS LEIE":
        lines.extend(format_leie_record(metadata if metadata else record))
    elif source == "CMS Open Payments":
        lines.extend(format_open_payments_record(metadata if metadata else record))
    elif source == "FDA Warning Letters":
        lines.extend(format_fda_warning_record(metadata if metadata else record))
    elif source == "DOJ":
        lines.extend(format_doj_record(metadata if metadata else record))
    else:
        lines.extend(format_generic_record(metadata if metadata else record))
    
    # Add next steps if present
    next_steps = metadata.get('next_steps') or record.get('next_steps')
    if next_steps:
        lines.append("")
        lines.append(f"RESEARCH NEXT STEPS: {next_steps}")
    
    return "\n".join(lines)


def format_faers_record(record: Dict[str, Any]) -> list:
    """Format FDA FAERS adverse event record."""
    lines = []
    
    drug_name = record.get('drug_name', 'Unknown')
    lines.append(f"DRUG NAME: {drug_name}")
    
    event_count = record.get('adverse_event_count', 0)
    lines.append(f"ADVERSE EVENT COUNT: {event_count} reports")
    
    # Serious outcomes
    outcomes = record.get('serious_outcomes', [])
    if outcomes:
        unique_outcomes = list(set(outcomes))[:10]  # Limit to 10
        lines.append(f"SERIOUS OUTCOMES: {', '.join(str(o) for o in unique_outcomes)}")
    
    # Indication diversity
    indication_diversity = record.get('indication_diversity', 0)
    if indication_diversity:
        lines.append(f"NUMBER OF DIFFERENT INDICATIONS: {indication_diversity}")
        if indication_diversity >= 10:
            lines.append(f"  ⚠️  High indication diversity suggests potential off-label marketing")
    
    # Add analysis
    lines.append("")
    lines.append("QUI TAM ANALYSIS:")
    if event_count >= 100:
        lines.append(f"  - High volume of adverse events ({event_count}) indicates widespread use")
    if indication_diversity >= 10:
        lines.append(f"  - {indication_diversity} different indications suggests off-label promotion")
        lines.append(f"  - If Medicare/Medicaid covered these off-label uses → False Claims Act violation")
    
    return lines


def format_leie_record(record: Dict[str, Any]) -> list:
    """Format CMS LEIE exclusion record."""
    lines = []
    
    provider_name = record.get('provider_name', '')
    if provider_name:
        lines.append(f"PROVIDER: {provider_name}")
    
    business = record.get('business_name', '')
    if business:
        lines.append(f"BUSINESS: {business}")
    
    lines.append(f"EXCLUSION TYPE: {record.get('exclusion_type', 'Unknown')}")
    lines.append(f"EXCLUSION DATE: {record.get('exclusion_date', 'Unknown')}")
    
    exclusion_year = record.get('exclusion_year')
    if exclusion_year:
        lines.append(f"EXCLUSION YEAR: {exclusion_year}")
    
    lines.append(f"STATE: {record.get('state', 'Unknown')}")
    lines.append(f"SPECIALTY: {record.get('specialty', 'Unknown')}")
    
    npi = record.get('npi', '')
    if npi:
        lines.append(f"NPI: {npi}")
    
    return lines


def format_open_payments_record(record: Dict[str, Any]) -> list:
    """Format CMS Open Payments record."""
    lines = []
    
    physician_name = record.get('physician_name', 'Unknown')
    lines.append(f"PHYSICIAN: {physician_name}")
    
    specialty = record.get('physician_specialty', 'Unknown')
    lines.append(f"SPECIALTY: {specialty}")
    
    npi = record.get('npi', '')
    if npi:
        lines.append(f"NPI: {npi}")
    
    amount = record.get('payment_amount') or record.get('amount', 0)
    try:
        amount_float = float(amount)
        lines.append(f"PAYMENT AMOUNT: ${amount_float:,.2f}")
    except:
        lines.append(f"PAYMENT AMOUNT: {amount}")
        amount_float = 0
    
    nature = record.get('payment_nature', 'Unknown')
    lines.append(f"PAYMENT TYPE: {nature}")
    
    company = record.get('paying_company') or record.get('submitting_entity', 'Unknown')
    lines.append(f"FROM COMPANY: {company}")
    
    date = record.get('payment_date', 'Unknown')
    lines.append(f"PAYMENT DATE: {date}")
    
    product = record.get('product_name', 'Unknown')
    if product and product != 'Unknown':
        lines.append(f"PRODUCT: {product}")
    
    # Add kickback analysis
    if amount_float >= 50000:
        lines.append("")
        lines.append("KICKBACK ANALYSIS:")
        lines.append(f"  - High-value payment (${amount_float:,.0f}) raises kickback concerns")
        lines.append(f"  - Cross-reference with Medicare Part D prescribing data for {physician_name}")
        lines.append(f"  - Check if high prescriber of {company} products")
    
    return lines


def format_fda_warning_record(record: Dict[str, Any]) -> list:
    """Format FDA warning letter record."""
    lines = []
    
    title = record.get('title', 'Untitled')
    lines.append(f"WARNING LETTER: {title}")
    
    date = record.get('date', 'Unknown')
    lines.append(f"DATE: {date}")
    
    url = record.get('url', '')
    if url:
        lines.append(f"URL: {url}")
    
    # Violation summary
    violation = record.get('violation_summary', '')
    if violation:
        lines.append("")
        lines.append("VIOLATION:")
        lines.append(violation)
    
    return lines


def format_doj_record(record: Dict[str, Any]) -> list:
    """Format DOJ settlement record."""
    lines = []
    
    title = record.get('title', 'Untitled')
    lines.append(f"CASE: {title}")
    
    date = record.get('date', 'Unknown')
    lines.append(f"DATE: {date}")
    
    defendant = record.get('defendant', 'Unknown')
    lines.append(f"DEFENDANT: {defendant}")
    
    settlement = record.get('settlement_amount', 'Unknown')
    lines.append(f"SETTLEMENT: {settlement}")
    
    fraud_type = record.get('fraud_type', 'Unknown')
    lines.append(f"FRAUD TYPE: {fraud_type}")
    
    programs = record.get('federal_programs', [])
    if programs:
        lines.append(f"FEDERAL PROGRAMS: {', '.join(programs)}")
    
    # Content
    content = record.get('content', '')
    if content:
        lines.append("")
        lines.append("DETAILS:")
        if len(content) > 1000:
            content = content[:1000] + "..."
        lines.append(content)
    
    return lines


def format_generic_record(record: Dict[str, Any]) -> list:
    """Generic formatter."""
    lines = []
    
    # Skip these meta fields
    skip_fields = {'id', 'source', 'metadata', 'text', 'filename', 'fraud_indicators', 
                   'case_status', 'fraud_potential_score', 'next_steps'}
    
    for key, value in record.items():
        if key in skip_fields or value is None:
            continue
        
        # Format key
        formatted_key = key.replace('_', ' ').upper()
        
        # Format value
        if isinstance(value, list):
            if value:
                value_str = ', '.join(str(v) for v in value[:10])
                lines.append(f"{formatted_key}: {value_str}")
        elif isinstance(value, dict):
            continue  # Skip nested dicts
        else:
            lines.append(f"{formatted_key}: {value}")
    
    return lines


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Convert JSONL medical fraud data to CSV with fraud score filtering"
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
        '--min-score',
        type=int,
        default=50,
        help='Minimum fraud potential score to include (default: 0)'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress progress messages'
    )
    
    args = parser.parse_args()
    
    print(f"Converting {args.input} to {args.output}...")
    # print(f"Filtering: Only including records with fraud_potential_score >= {args.min_score}")  # COMMENTED OUT - NO FILTERING
    
    try:
        count, skipped = jsonl_to_csv(args.input, args.output, verbose=not args.quiet, min_score=0)  # min_score set to 0 to include all
        print(f"\n✅ Successfully converted {count} records")
        # print(f"   Skipped {skipped} records with score < {args.min_score}")  # COMMENTED OUT - NO FILTERING
        print(f"   Output: {args.output}")
        print(f"\nNext step:")
        print(f"   python gpt_ranker.py --chunk-size 0 --max-rows 10")
        
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