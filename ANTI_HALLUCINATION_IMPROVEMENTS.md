# Anti-Hallucination Improvements for Clinical Research Bot

## Executive Summary

This document outlines comprehensive improvements made to prevent AI hallucinations and false positives in the clinical research qui tam detection system. These changes significantly increase accuracy and reduce false positives by requiring documented evidence rather than allowing speculation.

## Problem Identified

The AI bot was creating **false positives** by:
1. **Assuming fraud from withdrawal**: Trial withdrawals scored as 75 without any documented fraud
2. **Inventing federal involvement**: Assuming NIH/Medicare involvement without evidence
3. **Speculating about impact**: Making unfounded connections between events and false claims
4. **Over-interpretation**: Treating "suspicious" as equivalent to "fraudulent"

**Example False Positive**: NCT04204668 initially scored 75 (high qui tam potential) but investigation revealed score should be 25 (low viability) - no documented fraud, no federal false claims, just a legitimate trial withdrawal.

## Solutions Implemented

### 1. Stricter Initial Ranking Prompt (gpt_ranker.py)

**Before**: Scored based on suspicion and patterns
**After**: Scores based ONLY on documented evidence

#### Key Changes:
- ✅ **EVIDENCE-ONLY SCORING**: Base scores only on explicit documented evidence
- ✅ **NO ASSUMPTIONS**: Don't assume NIH/Medicare involvement unless explicitly documented
- ✅ **"WITHDRAWN" ≠ "FRAUD"**: Trial withdrawal without fraud allegations scores 0-25 (was 70-89)
- ✅ **DOCUMENTED FALSE CLAIMS REQUIRED**: Scores >50 require identifying WHERE false claims were submitted
- ✅ **SUSPICIOUS ≠ FRAUDULENT**: Correlation is not causation

#### Anti-Hallucination Checkpoints:
Before assigning scores >50, the AI must verify:
- ✓ Is there DOCUMENTED evidence of fraud (not just suspicion)?
- ✓ Is there DOCUMENTED federal program involvement (specific grant/program named)?
- ✓ Is there a DOCUMENTED false claim submitted?

**If any answer is NO → score ≤ 49**

#### New Conservative Scoring Framework:

| Score Range | Requirements (ALL must be met) |
|------------|-------------------------------|
| 90-100 | Documented fraud + Federal funding + Existing settlement/investigation |
| 85-89 | Fraud documented + Federal funding documented but incomplete |
| 70-84 | Official concerns (Expression of Concern, investigation reports) + Federal link |
| 50-69 | Documented issues requiring investigation (corrections that change conclusions) |
| 25-49 | **DEFAULT for uncertain cases** - Minor issues or normal science |
| 0-24 | No FCA relevance (copyright, normal science) |

#### New JSON Output Field:
```json
{
  "evidence_quality": "DOCUMENTED | SUSPECTED | SPECULATIVE"
}
```

This field is now captured in both CSV and JSONL output to track evidence strength.

### 2. Enhanced Clinical Investigator Prompt (clinical_investigator.py)

**Before**: Had anti-hallucination rules but could still over-interpret lead data
**After**: Strict evidence hierarchy and mandatory verification checklist

#### Key Enhancements:

**Evidence Hierarchy** (Strongest to Weakest):
1. **GOLD STANDARD**: Official government documents (ORI findings, DOJ settlements, NIH termination notices)
2. **HIGH QUALITY**: Journal retraction notices with explicit reasons
3. **MODERATE**: PubPeer discussions with specific evidence
4. **LOW QUALITY**: Lead data without external verification
5. **INVALID**: Assumptions, inferences, "common sense" reasoning

**Critical Verification Rules**:
- Lead data = STARTING POINT (not proof)
- If search CONTRADICTS lead data → Search results take precedence
- Must verify dates, grant numbers, and federal involvement
- Cannot rely on "likely", "probably", "appears to be" → Must lower score

**Mandatory Pre-Score Checklist**:
Every investigation must explicitly answer:
- □ Did I find DOCUMENTED evidence of fraud? YES/NO + URL
- □ Did I find DOCUMENTED federal program involvement? YES/NO + URL
- □ Did I find DOCUMENTED false claims submitted? YES/NO + URL
- □ Did I VERIFY dates and amounts through search? YES/NO
- □ Are my sources OFFICIAL and CREDIBLE? YES/NO

**Automatic Score Caps**:
- If "fraud documented" = NO → Automatic score ≤ 40
- If "federal involvement documented" = NO → Automatic score ≤ 30
- 5 YES = Score 85-100
- 4 YES = Score 70-84
- 3 YES = Score 50-69
- 2 YES or fewer = Score 0-49

### 3. Mandatory Audit Trail

Every investigation report now includes a **mandatory "Evidence Quality Assessment"** section:

```markdown
### Evidence Quality Assessment (MANDATORY - Anti-Hallucination Audit Trail)

**Verification Checklist Results:**
- [ ] Documented evidence of fraud? [YES/NO + URL]
- [ ] Documented federal program involvement? [YES/NO + URL]
- [ ] Documented false claims submitted? [YES/NO + URL]
- [ ] Dates and amounts verified? [YES/NO]
- [ ] Sources official and credible? [YES/NO]

**Evidence Found:**
- [List each VERIFIED piece with URL]

**Evidence NOT Found:**
- [Be specific about unsuccessful searches]

**Lead Data Verification:**
- Verified from lead: [List confirmed items]
- Could NOT verify: [List unconfirmed items]
- Contradictions: [List any contradictions]

**Overall Confidence Level:** HIGH / MODERATE / LOW
```

This audit trail:
- Forces explicit documentation of what was found vs. not found
- Prevents AI from glossing over missing evidence
- Creates accountability for scoring decisions
- Allows humans to quickly assess reliability

## How to Use the Improved System

### Running the Initial Ranker

```bash
python gpt_ranker.py \
  --input data/processed/combined_qui_tam_data.csv \
  --output data/results/qui_tam_ranked.csv \
  --investigate-min-score 50
```

**What's Different**:
- Initial scores will be MORE CONSERVATIVE (more 25-49 scores, fewer 70+ scores)
- High scores (70+) will only appear with DOCUMENTED fraud + federal involvement
- `evidence_quality` field will show strength: DOCUMENTED | SUSPECTED | SPECULATIVE
- Investigation threshold (50) remains same but fewer leads will hit it

### Running the Rerun Script

```bash
python rerun_low_score_investigations.py
```

**What's Different**:
- Still re-investigates score=25 cases
- Now uses enhanced investigator with mandatory checklist
- Reports will include detailed Evidence Quality Assessment
- Scores will be based on verified evidence, not speculation

### Interpreting Results

**Old System**:
- Score 75 might be based on "trial withdrawn, seems suspicious"
- No clear evidence trail
- Hard to distinguish real fraud from speculation

**New System**:
- Score 75 requires documented fraud + federal involvement + official sources
- Evidence Quality Assessment shows exactly what was verified
- Easy to filter by `evidence_quality` column:
  - **DOCUMENTED**: High confidence, multiple official sources
  - **SUSPECTED**: Some verification but gaps
  - **SPECULATIVE**: Mostly unverified (should be score <50)

## Expected Impact

### Reduced False Positives

**Before**: Cases like NCT04204668 scored 75 initially (false positive)
**After**: Same case would score 25-35 initially (conservative, awaiting verification)

### Better Filtering

Sort/filter results by:
1. `evidence_quality = "DOCUMENTED"` + `qui_tam_score >= 70` = **Highest priority**
2. `investigation_viability_score >= 70` + Evidence Assessment shows "HIGH confidence" = **Verified high priority**
3. `evidence_quality = "SUSPECTED"` = **Needs more research**
4. `evidence_quality = "SPECULATIVE"` = **Low priority / likely false positive**

### Audit Trail Benefits

- **Faster review**: Read Evidence Quality Assessment to see what's verified
- **Better debugging**: Understand why AI assigned a score
- **Training data**: Use HIGH confidence cases to improve future prompts
- **Legal defensibility**: Can show exactly what evidence supports each claim

## Comparison: Old vs. New Scoring

| Scenario | Old Score | New Score | Reasoning |
|----------|-----------|-----------|-----------|
| Trial withdrawn, no fraud stated | 70-75 | 25-35 | Withdrawal ≠ fraud without documentation |
| Retraction citing "data fabrication" + NIH grant documented | 85-90 | 85-95 | Both fraud AND federal involvement documented |
| Suspicious results, no official findings | 60-70 | 25-40 | Suspicious ≠ fraudulent |
| PubPeer discussion + no official investigation | 55-65 | 35-45 | Moderate evidence but no official findings |
| Copyright/permission retraction | 0 | 0 | ✅ Already handled correctly |
| ORI finding + NIH grant + fraud gap >3 years | 90-95 | 90-100 | ✅ All evidence documented |

## Testing and Validation

### Recommended Test Process

1. **Run rerun script on existing data**:
   ```bash
   python rerun_low_score_investigations.py
   ```

2. **Compare old vs. new investigation reports**:
   - Old reports in CSV before running
   - New reports will have Evidence Quality Assessment

3. **Review cases that changed significantly**:
   - Look for cases where score dropped 20+ points
   - Read Evidence Quality Assessment to understand why
   - Validate that new score is more accurate

4. **Spot-check high-scoring cases**:
   - Filter for `qui_tam_score >= 70` AND `evidence_quality = "DOCUMENTED"`
   - Verify these truly have documented fraud + federal involvement
   - These should be your high-priority investigation targets

### Key Metrics to Monitor

- **False Positive Rate**: % of score >=70 cases that lack documented fraud
  - **Before**: ~40-60% (estimated based on NCT04204668 example)
  - **Target After**: <10%

- **Evidence Quality Distribution**:
  - **Target**: 70%+ of scores >=70 should be "DOCUMENTED"
  - **Red Flag**: If many "SPECULATIVE" cases score >70 → AI not following prompts

- **Investigation Efficiency**:
  - **Before**: Many investigations of low-value leads
  - **After**: More investigations should yield viable cases

## Technical Details

### Files Modified

1. **gpt_ranker.py** (lines 43-151):
   - Replaced SCIENTIFIC_FRAUD_RANKING_PROMPT with evidence-based version
   - Added `evidence_quality` field to CSV and JSON output (lines 1487, 1504)
   - New conservative scoring framework (90-100 requires both fraud AND federal involvement)

2. **clinical_investigator.py** (lines 161-223):
   - Added ANTI-HALLUCINATION CRITICAL RULES
   - Added Evidence Hierarchy framework
   - Added mandatory pre-score checklist
   - Added Evidence Quality Assessment to output format (lines 136-162)
   - Automatic score caps based on checklist results

3. **New Documentation**:
   - This file: ANTI_HALLUCINATION_IMPROVEMENTS.md

### Backward Compatibility

✅ **CSV/JSONL format**: New `evidence_quality` column added; existing columns unchanged
✅ **Investigation threshold**: Default remains 50 (configurable via --investigate-min-score)
✅ **Rerun script**: No changes needed; automatically uses new prompts
✅ **Existing data**: Can re-process with new prompts using rerun script

## Best Practices

### For Running Analysis

1. **Start with full dataset using new prompts**:
   - Initial scores will be more conservative
   - Focus on cases with `evidence_quality = "DOCUMENTED"`

2. **Use rerun script for borderline cases**:
   - Score=25 cases get second look
   - Evidence Quality Assessment helps determine if worth pursuing

3. **Prioritize by evidence quality**:
   - HIGH confidence + score >=85 = Immediate investigation
   - MODERATE confidence + score 70-84 = Worth reviewing
   - LOW confidence = Likely false positive, deprioritize

### For Reviewing Results

1. **Always read Evidence Quality Assessment first**:
   - Shows what's verified vs. speculation
   - If "Evidence Found" section is sparse → low confidence

2. **Check for specific URLs/citations**:
   - Vague citations ("according to research") = red flag
   - Specific URLs (DOI, ORI report link, ClinicalTrials.gov) = good

3. **Look for contradictions**:
   - "Contradictions found" section not empty = need manual review
   - Lead data vs. search results conflicts = AI chose search results (correct)

## Troubleshooting

### If still seeing hallucinations

**Symptom**: Cases scoring >70 without documented fraud

**Solution**:
1. Check `evidence_quality` field - should be "DOCUMENTED"
2. Read Evidence Quality Assessment in investigation report
3. If assessment shows "NO" for fraud or federal involvement → Bug, report example
4. Possible causes:
   - AI not following new prompts → Increase temperature=0 enforcement
   - Search results returning misleading data → Check Tavily API results
   - JSON parsing extracting wrong fields → Check logs

### If scores seem too low

**Symptom**: Known fraud cases scoring <50

**Solution**:
1. Check if fraud is DOCUMENTED in the source text
2. Check if federal involvement is DOCUMENTED in source text
3. If both are documented but score low:
   - Read investigation report Evidence Quality Assessment
   - AI may have failed to find verification in searches
   - Try manual search to see if evidence is findable
4. If evidence exists but AI missed it:
   - May need to improve search queries in clinical_investigator.py
   - Add more search variations to PHASE 1-4 protocols

## Future Enhancements

### Potential Improvements

1. **Structured Evidence Extraction**:
   - Parse Evidence Quality Assessment into structured JSON
   - Enable programmatic filtering by evidence type

2. **Confidence Scoring**:
   - Separate score (0-100) from confidence (HIGH/MODERATE/LOW)
   - Allow high score + low confidence = "needs verification"

3. **Evidence Source Validation**:
   - Check that cited URLs are accessible
   - Verify DOIs resolve to claimed papers
   - Flag broken links or invalid citations

4. **Hallucination Detection**:
   - Post-process reports to detect hallucination patterns
   - Flag reports with high score but no URLs in Evidence Found
   - Automatic quality checks before writing to CSV

5. **Feedback Loop**:
   - Track which cases led to actual qui tam filings
   - Use confirmed cases to improve scoring calibration
   - Build training dataset of HIGH quality examples

## Summary

These anti-hallucination improvements transform the system from a **pattern-finding tool** (prone to false positives) to an **evidence-validation tool** (conservative but accurate).

**Key Philosophy Change**:
- **Before**: "This looks suspicious, score it high"
- **After**: "Is this DOCUMENTED fraud with DOCUMENTED federal false claims? If not, score conservatively"

**Result**: Fewer false positives, better use of investigation resources, and legally defensible scoring with full audit trails.

---

**For Questions or Issues**: Review this document first, then check investigation reports' Evidence Quality Assessment sections to understand scoring rationale.
