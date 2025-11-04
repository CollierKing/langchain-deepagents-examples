from agent import agent
from config import RUN_NAME, S3_BUCKET_NAME
from middleware import S3Backend
import logging
import sys

# MARK: - Logging Setup
# Create local log file
log_file = f"run_{RUN_NAME}.log"

# Get root logger (already configured by middleware.py)
root_logger = logging.getLogger()

# Add file handler to existing logger
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
root_logger.addHandler(file_handler)

print(f"📝 Logging to: {log_file}")

print("\n🚀 Starting Deep Agent Pipeline...")
print("=" * 60)
print(f"Run Name: {RUN_NAME}")
print(f"S3 Bucket: {S3_BUCKET_NAME}")
print(f"Output Path: deepagent_runs/{RUN_NAME}/")
print(f"Log File: {log_file}")
print("=" * 60)

# Use run name as thread_id for checkpointing
config = {"configurable": {"thread_id": RUN_NAME}}

result = agent.invoke(
    {"messages": [{"role": "user", "content": "Execute the 4-step analysis pipeline"}]},
    config=config
)

print("\n" + "=" * 60)
print("✅ Pipeline Complete!")
print("=" * 60)

# Print message count
messages = result.get("messages", [])
print(f"\nTotal messages: {len(messages)}")

# Print last message (full content)
if messages:
    last_msg = messages[-1]
    print(f"\nLast message type: {type(last_msg).__name__}")
    if hasattr(last_msg, 'content'):
        content = str(last_msg.content)
        print(f"\n{'=' * 60}")
        print("FINAL RESULT:")
        print('=' * 60)
        print(content)
        print('=' * 60)
    
# Check todos
if "todos" in result:
    print(f"\nTodos completed: {len(result['todos'])}")

print(f"\n✅ Check S3 bucket at: s3://{S3_BUCKET_NAME}/deepagent_runs/{RUN_NAME}/")
print("   Expected outputs:")
print("   - themes_analysis.json")
print("   - company_matches/batch_*.json (all batches)")
print("   - matched_companies.json")
print("   - validations/company_*.json (per company)")
print("   - validated_results.json")
print("   - final_rankings.json")
print("   - pipeline_summary.txt (main agent's final report)")
print("   - run.log (execution logs)")

# Upload log file to S3
print(f"\n📤 Uploading log file to S3...")
try:
    # Flush all log handlers to ensure log file is complete
    for handler in logging.root.handlers:
        handler.flush()
    
    s3_backend = S3Backend(bucket_name=S3_BUCKET_NAME, run_name=RUN_NAME)
    
    # Read log file
    with open(log_file, 'r') as f:
        log_content = f.read()
    
    # Write to S3
    s3_backend.write('run.log', log_content)
    print(f"   ✅ Uploaded {log_file} → s3://{S3_BUCKET_NAME}/deepagent_runs/{RUN_NAME}/run.log")
    print(f"   📊 Log size: {len(log_content):,} bytes, {len(log_content.splitlines()):,} lines")
except Exception as e:
    print(f"   ⚠️  Failed to upload log: {e}")

print("\n" + "=" * 60)