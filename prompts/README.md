# Prompts Directory

This directory contains system prompts used by the ranker to analyze documents.

## Using Custom Prompts

You can create your own prompt file and use it with the ranker:

```bash
python gpt_ranker.py \
  --prompt-file prompts/my_custom_prompt.txt \
  --config ranker_config.toml
```

Or set it in your `ranker_config.toml`:

```toml
prompt_file = "prompts/my_custom_prompt.txt"
```

## Prompt Files

### `default_system_prompt.txt`

The default prompt used by the ranker. It instructs the model to:
- Score documents on investigative usefulness, controversy, novelty, and power linkage
- Use a 0-100 scale consistently
- Return structured JSON with specific fields (headline, importance_score, reason, key_insights, tags, power_mentions, agency_involvement, lead_types)

### Creating Your Own Prompt

1. Copy `default_system_prompt.txt` to a new file (e.g., `my_custom_prompt.txt`)
2. Modify the scoring criteria, scale, or output format as needed
3. **Important**: Ensure your prompt requests the same JSON fields expected by the ranker:
   - `headline` (string)
   - `importance_score` (number)
   - `reason` (string)
   - `key_insights` (array)
   - `tags` (array)
   - `power_mentions` (array)
   - `agency_involvement` (array)
   - `lead_types` (array)
   - `action_items` (array, if using `--include-action-items`)

4. Reference your custom prompt via `--prompt-file` or in the config file

## Examples

### Stricter Scoring Prompt

Focus only on verified, high-impact leads with concrete evidence.

### Domain-Specific Prompt

Customize for different document types (financial records, emails, court transcripts, etc.).

### Multi-Language Prompt

Add instructions for handling documents in different languages.

## Notes

- The `--system-prompt` argument can still be used to provide an inline prompt string (overrides `--prompt-file`)
- If neither is provided, the ranker uses `prompts/default_system_prompt.txt`
- Prompt files should be plain text (UTF-8 encoded)
