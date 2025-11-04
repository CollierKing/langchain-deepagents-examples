# MARK: - Imports
from langchain.agents import create_agent
from config import (
    S3_BUCKET_NAME,
    RUN_NAME,
    TRANSCRIPT_S3_KEY,
    MODEL,
    COMPANY_BATCH_SIZE,
    PRESS_RELEASE_BATCH_SIZE,
    TOP_COMPANY_MATCHES,
    CONTEXT_WINDOW_TOTAL,
    MAX_OUTPUT_TOKENS,
)
from middleware import (
    LoggingMiddleware,
    ContentTruncationMiddleware,
    S3Backend,
    ValidationFileTrackerMiddleware,
)
from deepagents.middleware.filesystem import FilesystemMiddleware
from tools import (
    get_companies_from_postgres,
    consolidate_batch_files,
    get_press_releases_from_mongodb,
    get_company_tickers_from_matched_file,
    consolidate_validation_files,
    merge_and_rank_companies,
)
from models import ThemesOutput, CompanyMatchesOutput, ValidationOutput, FinalOutput
import json

# MARK: - Configuration
model = MODEL


# Factory functions to create fresh middleware instances for each subagent
def create_s3_filesystem():
    """Create new S3 filesystem middleware instance"""
    s3_backend_factory = lambda rt: S3Backend(
        bucket_name=S3_BUCKET_NAME, run_name=RUN_NAME
    )
    return FilesystemMiddleware(backend=s3_backend_factory)


def create_content_truncation():
    """Create new content truncation middleware instance"""
    return ContentTruncationMiddleware(
        max_tokens=CONTEXT_WINDOW_TOTAL - MAX_OUTPUT_TOKENS - 5_000
    )


# MARK: - Subagent 1: Transcript Analyzer
analyzer_system_prompt = f"""You are an expert at analyzing transcripts.  
  
1. Use read_file to read '{TRANSCRIPT_S3_KEY}'
2. Analyze it to identify key themes, trends, and focus areas  
3. Write your analysis to 'themes_analysis.json' using write_file

OUTPUT SCHEMA (ThemesOutput from models.py):
{json.dumps(ThemesOutput.model_json_schema(), indent=2)}"""

analyzer_graph = create_agent(
    model=model,
    tools=[],
    system_prompt=analyzer_system_prompt,
    middleware=[
        create_s3_filesystem(),
        create_content_truncation(),
        LoggingMiddleware(),
    ],
)

# MARK: - Subagent 2: Company Matcher
matcher_system_prompt = f"""You are an expert at matching companies to market trends.  

⚠️ ABSOLUTE RULES - NO EXCEPTIONS:
1. Process EVERY SINGLE COMPANY in the database - no sampling, no shortcuts
2. START at offset=0 and process EVERY batch sequentially until has_more=false
3. NEVER skip offsets or jump to arbitrary values
4. NEVER decide "I have enough matches" and stop early
5. The database order is RANDOM - you cannot predict where companies are
6. Write a batch file after EVERY query - this keeps your context manageable

DO NOT rationalize skipping batches because:
- "There are too many companies" → Process them all anyway
- "I have a good sample" → Keep going until has_more=false
- "I'll focus on relevant ones" → Evaluate ALL, then pick top {TOP_COMPANY_MATCHES}

WORKFLOW:

1. Read themes: read_file('themes_analysis.json')

2. Initialize tracking:
   - current_offset = 0
   - batches_processed = 0

3. SEQUENTIAL BATCH PROCESSING LOOP (Process ALL companies):
   
   DO THIS EXACTLY - NO VARIATIONS:
   
   a) Query: get_companies_from_postgres(offset=current_offset, limit={COMPANY_BATCH_SIZE})
   
   b) Evaluate each company in results against themes
   
   c) Write results: write_file('company_matches/batch_{{current_offset:04d}}.json', <matches_json>)
      ↳ Use 4-digit zero-padded offset (e.g., batch_0000.json, batch_0050.json)
      ↳ Write this file even if matches list is empty []
   
   d) Update state:
      - current_offset += {COMPANY_BATCH_SIZE}
      - batches_processed += 1
   
   e) Check has_more field in the response:
      - If has_more == false: ALL companies processed → STOP and go to step 4
      - If has_more == true: More companies remain → GO BACK TO step a) with NEW current_offset
   
   KEEP LOOPING until has_more=false (this is your ONLY stop condition)
   
   ❌ FORBIDDEN:
   - Using offset values that are NOT (batches_processed × {COMPANY_BATCH_SIZE})
   - Skipping any offset values in the sequence
   - Changing the limit parameter
   - Trying to "search" for specific companies
   - Stopping early because you think you have "enough" matches
   - Deciding to "sample" instead of processing all companies

4. Consolidation (only after ALL batches complete):
   - Call consolidate_batch_files() tool
   - This automatically reads all company_matches/batch_*.json files, ranks all matches, and writes matched_companies.json
   - Returns confirmation with total match count

OUTPUT SCHEMA (CompanyMatchesOutput from models.py):
{json.dumps(CompanyMatchesOutput.model_json_schema(), indent=2)}"""

matcher_graph = create_agent(
    model=model,
    tools=[get_companies_from_postgres, consolidate_batch_files],
    system_prompt=matcher_system_prompt,
    middleware=[
        # Sequential enforcement is built into get_companies_from_postgres tool itself
        create_s3_filesystem(),
        create_content_truncation(),
        LoggingMiddleware(),
    ],
)

# MARK: - Subagent 3: Press Release Validator
validator_system_prompt = f"""You are an expert at validating company-theme alignment through press releases.  

YOUR GOAL: Find press releases that SUPPORT and VALIDATE each company's alignment with the identified themes.

IGNORE: Legal issues, accounting matters, lawsuits, or any non-theme-related content.
FOCUS ON: Product announcements, technology developments, partnerships, and initiatives that relate to the themes.

⚠️ ABSOLUTE RULES - NO EXCEPTIONS:
1. Process EVERY SINGLE COMPANY in matched_companies.json - no skipping
2. Process ONE company at a time (symbols parameter must have exactly ONE ticker)
3. NEVER query the same company twice
4. ALWAYS use skip=0 (no pagination - get all press releases in one call)
5. Write validation file for EACH company before moving to next

DO NOT rationalize skipping companies because:
- "I have enough validations" → Process ALL companies in the list
- "This company probably doesn't match" → Validate it anyway
- "I'll just do a sample" → Process the entire matched_companies list

WORKFLOW:

1. Initialize validation queue:
   - Call get_company_tickers_from_matched_file()
   - This extracts ALL ticker symbols from matched_companies.json
   - Returns the ordered list of companies you MUST validate sequentially
   - DO NOT call read_file('matched_companies.json') - use this tool instead

2. SEQUENTIAL COMPANY PROCESSING (Process ALL companies):
   
   DO THIS EXACTLY - NO VARIATIONS:
   
   For EACH and EVERY company in the matched_companies list (loop through entire list):
   
   a) Get next ticker from the company list returned by get_company_tickers_from_matched_file
   
   b) Query press releases: get_press_releases_from_mongodb(symbols="TICKER", skip=0, limit={PRESS_RELEASE_BATCH_SIZE})
      ↳ Use ONLY one ticker at a time, skip=0
   
   c) Analyze the press release results:
      - Review pr_title and content for theme alignment
      - Determine: supports_themes (true/false)
      - Calculate: confidence_adjustment (-1.0 to +1.0)
      - Calculate: adjusted_score = original_score + confidence_adjustment
      - Extract: key evidence with pr_title and pr_link
   
   d) IMMEDIATELY write validation: write_file('validations/company_{{TICKER}}.json', <validation_json>)
      ↳ DO NOT SKIP THIS STEP - you cannot query next company without writing this file first
      ↳ REQUIRED fields: ticker, company_name, original_themes, original_score, 
                         press_release_validation, supports_themes, evidence_summary,
                         validation_status, confidence_adjustment, notes
      ↳ OPTIONAL fields: adjusted_score, key_evidence, relevance_score
      ↳ key_evidence format: [{{"evidence": "...", "pr_title": "...", "pr_link": "..."}}]
      ↳ Use exact ticker (e.g., company_NVDA.json, company_MSFT.json)
      ↳ Write this file IMMEDIATELY after analyzing PRs
      ↳ Even if no evidence found, still write with supports_themes=false
   
   e) Move to next company in list
   
   ❌ FORBIDDEN:
   - Querying multiple companies at once (comma-separated symbols)
   - Using skip > 0 for pagination
   - Querying the same company twice
   - Skipping companies in the list
   - Stopping early because you think you have "enough" validations
   - Deciding to validate only a "sample" of companies

3. Consolidation (only after ALL companies complete):
   - Call consolidate_validation_files() tool
   - This automatically reads all validations/company_*.json files, combines them, and writes validated_results.json
   - Returns confirmation with total validation count

OUTPUT SCHEMA (ValidationOutput from models.py):
{json.dumps(ValidationOutput.model_json_schema(), indent=2)}"""

validator_graph = create_agent(
    model=model,
    tools=[
        get_company_tickers_from_matched_file,
        get_press_releases_from_mongodb,
        consolidate_validation_files,
    ],
    system_prompt=validator_system_prompt,
    middleware=[
        # Sequential enforcement is built into get_press_releases_from_mongodb tool itself
        create_s3_filesystem(),
        ValidationFileTrackerMiddleware(),  # Tracks when validation files are written
        create_content_truncation(),  # Safe now - ticker extraction happens in tool
        LoggingMiddleware(),
    ],
)

# MARK: - Subagent 4: Final Ranker
ranker_system_prompt = f"""You are an expert at consolidating and ranking analysis results.

YOUR GOAL: Create final company rankings by merging match and validation data.

WORKFLOW:

1. Call merge_and_rank_companies() tool
   - This automatically:
     * Reads matched_companies.json and validated_results.json from S3
     * Merges data for each company (uses adjusted_score if available)
     * Re-ranks all companies by final_score
     * Takes top {TOP_COMPANY_MATCHES}
     * Writes final_rankings.json

2. After merge completes:
   - Read final_rankings.json to see the results
   - Create a brief summary highlighting top companies and key findings
   - Write summary to 'ranking_summary.txt' using write_file

That's it! The merge_and_rank_companies tool does the heavy lifting automatically.

OUTPUT SCHEMA (FinalOutput from models.py):
{json.dumps(FinalOutput.model_json_schema(), indent=2)}"""

ranker_graph = create_agent(
    model=model,
    tools=[merge_and_rank_companies],
    system_prompt=ranker_system_prompt,
    middleware=[
        create_s3_filesystem(),
        create_content_truncation(),
        LoggingMiddleware(),
    ],
)

# MARK: - Subagent Definitions
subagents = [
    {
        "name": "transcript-analyzer",
        "description": "Analyzes transcripts to identify key themes, trends and focus areas",
        "runnable": analyzer_graph,
    },
    {
        "name": "company-matcher",
        "description": "Matches companies to identified themes and trends",
        "runnable": matcher_graph,
    },
    {
        "name": "press-release-validator",
        "description": "Validates company matches using press releases",
        "runnable": validator_graph,
    },
    {
        "name": "final-ranker",
        "description": "Merges matches and validations, re-ranks companies by final score",
        "runnable": ranker_graph,
    },
]
