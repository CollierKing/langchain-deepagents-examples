from dotenv import load_dotenv, find_dotenv
from deepagents import create_deep_agent
from config import MODEL, CONTEXT_WINDOW_TOTAL, MAX_OUTPUT_TOKENS, S3_BUCKET_NAME, RUN_NAME
from middleware import LoggingMiddleware, ContentTruncationMiddleware, S3Backend
from subagents import subagents
from langgraph.checkpoint.sqlite import SqliteSaver

# MARK: - Configuration
load_dotenv(find_dotenv(), override=False)

# Persistent checkpointer - saves to SQLite database
import sqlite3
db_conn = sqlite3.connect("checkpoints.db", check_same_thread=False)
checkpointer = SqliteSaver(db_conn)

# MARK: - Agent
agent = create_deep_agent(
    model=MODEL,
    tools=[],
    checkpointer=checkpointer,
    backend=lambda rt: S3Backend(bucket_name=S3_BUCKET_NAME, run_name=RUN_NAME),  # S3 backend for filesystem
    system_prompt="""You are a sequential analysis orchestrator.  
  
Execute this 4-step pipeline:  
  
1. Call transcript-analyzer subagent  
   - Reads: transcripts/transcript.txt
   - Writes: themes_analysis.json
     
2. Call company-matcher subagent  
   - Reads: themes_analysis.json
   - Queries: PostgreSQL for company data (uses get_companies_from_postgres tool)
   - Writes: matched_companies.json
     
3. Call press-release-validator subagent  
   - Reads: matched_companies.json
   - Queries: MongoDB for press releases (uses get_press_releases_from_mongodb tool)
   - Writes: validated_results.json

4. Call final-ranker subagent
   - Reads: matched_companies.json AND validated_results.json
   - Merges both datasets:
     * For each company in matched_companies.json, find its validation (if exists)
     * Use adjusted_score from validation if available, else use original score
     * Combine all data (themes, alignment_factors, evidence, validation status)
   - Re-rank top 100 companies by final score
   - Writes: final_rankings.json

5. After ALL subagents complete:
   - Read final_rankings.json
   - Create a summary report with key findings
   - Write summary to 'pipeline_summary.txt' using write_file

All files are automatically stored in S3: deepagent_runs/{{run_name}}/
  
Execute SEQUENTIALLY. Wait for each step to complete before proceeding.  
Use write_todos to track your progress.""",
    subagents=subagents,
    middleware=[
        ContentTruncationMiddleware(max_tokens=CONTEXT_WINDOW_TOTAL - MAX_OUTPUT_TOKENS - 5_000),
        LoggingMiddleware()
    ],
)
