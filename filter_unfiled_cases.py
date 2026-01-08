#!/usr/bin/env python3
"""
Filter ranked results to show ONLY unfiled cases with high scores

Usage:
  python filter_unfiled_cases.py --min-score 70
  python filter_unfiled_cases.py --min-score 50 --status unfiled
"""

import json
import csv
import argparse
from pathlib import Path

def filter_cases(input_file, output_file, min_score=70, status_filter="unfiled"):
    """Filter qui tam cases by score and status"""
    
    print(f"\nFiltering cases:")
    print(f"  Min score: {min_score}")
    print(f"  Status: {status_filter}")
    
    filtered = []
    total = 0
    
    # Read JSONL file
    with open(input_file, 'r') as f:
        for line in f:
            total += 1
            try:
                record = json.loads(line)
                score = record.get('qui_tam_score', 0)
                case_status = record.get('case_status', 'unknown').lower()
                public = record.get('public_disclosure', 'unknown').lower()
                viable = record.get('first_to_file_viable', 'unknown').lower()
                
                # Filter logic
                if score >= min_score:
                    if status_filter == "any" or case_status == status_filter:
                        # Extra filter: exclude if publicly disclosed or not viable
                        if public != "yes" and viable != "no":
                            filtered.append(record)
            except json.JSONDecodeError:
                continue
    
    print(f"\nResults:")
    print(f"  Total records: {total}")
    print(f"  Filtered: {len(filtered)}")
    print(f"  Filtered out: {total - len(filtered)}")
    
    if len(filtered) == 0:
        print("\n⚠️  No records match your criteria!")
        print("\nTry:")
        print("  - Lower min-score: --min-score 50")
        print("  - Show all statuses: --status any")
        return
    
    # Save filtered results
    with open(output_file, 'w') as f:
        for record in filtered:
            f.write(json.dumps(record) + '\n')
    
    print(f"\n✅ Saved to: {output_file}")
    
    # Show top 10
    print(f"\nTop 10 Unfiled Cases:")
    print("-" * 80)
    
    sorted_cases = sorted(filtered, key=lambda x: x.get('qui_tam_score', 0), reverse=True)
    
    for i, case in enumerate(sorted_cases[:10], 1):
        score = case.get('qui_tam_score', 0)
        headline = case.get('headline', 'No headline')
        status = case.get('case_status', 'unknown')
        damages = case.get('estimated_damages', 'unknown')
        
        print(f"{i}. Score: {score} | Status: {status}")
        print(f"   {headline}")
        print(f"   Damages: {damages}")
        print()

def main():
    parser = argparse.ArgumentParser(description="Filter qui tam cases")
    parser.add_argument('--input', default='data/results/qui_tam_ranked.jsonl',
                        help='Input JSONL file')
    parser.add_argument('--output', default='data/results/unfiled_high_score.jsonl',
                        help='Output JSONL file')
    parser.add_argument('--min-score', type=int, default=70,
                        help='Minimum qui tam score (default: 70)')
    parser.add_argument('--status', default='unfiled',
                        choices=['unfiled', 'filed', 'settled', 'unknown', 'any'],
                        help='Case status to filter (default: unfiled)')
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"❌ Input file not found: {input_path}")
        print("\nRun ranker first:")
        print("  python gpt_ranker.py --config ranker_config.toml")
        return
    
    filter_cases(input_path, args.output, args.min_score, args.status)

if __name__ == "__main__":
    main()
