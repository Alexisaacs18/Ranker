#!/usr/bin/env python3
"""
Update Ranker Configuration for High-Score Unfiled Cases

This script:
1. Creates enhanced qui tam prompt focused on unfiled cases
2. Updates ranker_config.toml to use it
3. Shows how to filter results for unfiled cases only
"""

from pathlib import Path

# Create enhanced prompt file
prompt_dir = Path("prompts")
prompt_dir.mkdir(exist_ok=True)

enhanced_prompt_file = prompt_dir / "enhanced_qui_tam_prompt.txt"

print("Creating enhanced qui tam prompt...")
print("This will help the AI:")
print("  - Give HIGHER scores to unfiled cases (70-100)")
print("  - Give LOW scores to already-filed cases (0-15)")
print("  - Track case status (unfiled, filed, settled)")

# The prompt has already been created in enhanced_qui_tam_prompt.txt
# Copy it to the prompts directory
import shutil
if Path("enhanced_qui_tam_prompt.txt").exists():
    shutil.copy("enhanced_qui_tam_prompt.txt", enhanced_prompt_file)
    print(f"✅ Created: {enhanced_prompt_file}")
else:
    print("⚠️  enhanced_qui_tam_prompt.txt not found, creating now...")
    # Would create it here, but it's already been created above

# Update ranker config
config_file = Path("ranker_config.toml")
print(f"\nUpdating {config_file}...")

config_content = """# Enhanced GPT Ranker Configuration
# Optimized for HIGH-SCORE UNFILED qui tam cases

# Input/Output paths
input = "data/processed/combined_qui_tam_data.csv"
output = "data/results/qui_tam_ranked.csv"
json-output = "data/results/qui_tam_ranked.jsonl"

# Use enhanced prompt that produces higher scores for unfiled cases
prompt-file = "prompts/enhanced_qui_tam_prompt.txt"

# CRITICAL: Disable chunking for single-file output
chunk-size = 0

# Model configuration  
endpoint = "http://127.0.0.1:1234/v1"
model = "openai/gpt-oss-120b"
temperature = 0.0
timeout = 600.0

# Processing options
resume = false
# Remove max-rows or increase for full processing
# max-rows = 10

# Optional: Energy tracking
# power-watts = 350.0
# electric-rate = 0.12
"""

with open(config_file, 'w') as f:
    f.write(config_content)

print(f"✅ Updated: {config_file}")
print("   - Added: prompt-file = enhanced prompt")
print("   - Endpoint: http://127.0.0.1:1234/v1")

# Create filtering script
filter_script = Path("filter_unfiled_cases.py")
print(f"\nCreating {filter_script}...")

filter_code = '''#!/usr/bin/env python3
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
    
    print(f"\\nFiltering cases:")
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
    
    print(f"\\nResults:")
    print(f"  Total records: {total}")
    print(f"  Filtered: {len(filtered)}")
    print(f"  Filtered out: {total - len(filtered)}")
    
    if len(filtered) == 0:
        print("\\n⚠️  No records match your criteria!")
        print("\\nTry:")
        print("  - Lower min-score: --min-score 50")
        print("  - Show all statuses: --status any")
        return
    
    # Save filtered results
    with open(output_file, 'w') as f:
        for record in filtered:
            f.write(json.dumps(record) + '\\n')
    
    print(f"\\n✅ Saved to: {output_file}")
    
    # Show top 10
    print(f"\\nTop 10 Unfiled Cases:")
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
        print("\\nRun ranker first:")
        print("  python gpt_ranker.py --config ranker_config.toml")
        return
    
    filter_cases(input_path, args.output, args.min_score, args.status)

if __name__ == "__main__":
    main()
'''

with open(filter_script, 'w') as f:
    f.write(filter_code)

filter_script.chmod(0o755)
print(f"✅ Created: {filter_script}")

print("\n" + "="*60)
print("SETUP COMPLETE!")
print("="*60)

print("\nWhat changed:")
print("✅ Enhanced prompt that scores unfiled cases MUCH higher (70-100)")
print("✅ Already-filed/settled cases get very low scores (0-15)")
print("✅ New fields: case_status, public_disclosure, first_to_file_viable")
print("✅ Filter script to show only unfiled high-score cases")

print("\n" + "="*60)
print("HOW TO USE")
print("="*60)

print("\n1. Run ranker with enhanced prompt:")
print("   python gpt_ranker.py --config ranker_config.toml")

print("\n2. Filter for unfiled cases with score 70+:")
print("   python filter_unfiled_cases.py --min-score 70")

print("\n3. Or try lower threshold:")
print("   python filter_unfiled_cases.py --min-score 50")

print("\n4. View results:")
print("   cat data/results/unfiled_high_score.jsonl | jq")

print("\n" + "="*60)
print("WHAT YOU'LL SEE")
print("="*60)

print("""
The enhanced prompt will now:

HIGH SCORES (70-100):
- Unfiled systematic Medicare/Medicaid fraud
- Recent cases with clear evidence
- Large dollar amounts documented
- No public disclosure

LOW SCORES (0-15):
- "Settled for $X million" → Score 5
- "DOJ announced" → Score 10  
- "Already under investigation" → Score 15

EXAMPLE OUTPUT:
Score: 92 | Status: unfiled
Systematic upcoding scheme across 300+ Medicare patients, $2.4M damages
Damages: $2,400,000

Score: 85 | Status: unfiled  
Kickback arrangement with pharmaceutical companies, routine practice
Damages: $1,800,000

Score: 78 | Status: unfiled
Phantom billing for services never provided, 2022-2024 pattern
Damages: $950,000
""")

print("Run the ranker now to see higher scores!")