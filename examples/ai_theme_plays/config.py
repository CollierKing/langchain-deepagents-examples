from datetime import datetime
from dotenv import load_dotenv, find_dotenv
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI

# Load environment variables first
load_dotenv(find_dotenv(), override=False)

# MARK: - S3 configuration
S3_BUCKET_NAME = "ai-theme-plays"
TRANSCRIPT_S3_KEY = "transcripts/transcript.txt"

# MARK: - Batch sizes for context management
COMPANY_BATCH_SIZE = 100
PRESS_RELEASE_BATCH_SIZE = 100
TOP_COMPANY_MATCHES = 100

# MARK: - Context window settings (Claude Sonnet 4.5 has 200K context)
# CONTEXT_WINDOW_TOTAL = 200_000
CONTEXT_WINDOW_TOTAL = 1_000_000
MAX_OUTPUT_TOKENS = 16_000  # Reduced from 64K to leave more room for input
CONTEXT_TRIM_LIMIT = CONTEXT_WINDOW_TOTAL - MAX_OUTPUT_TOKENS - 5_000  # Safety buffer

# MARK: - ANTHROPIC
# Shared model configuration with retry logic
# max_retries will automatically retry on:
# - 408 (Request Timeout), 429 (Rate Limit), 500 (Server Error)
# - 502 (Bad Gateway), 503 (Service Unavailable), 504 (Gateway Timeout)
# If you see a 500 error, it means all 10 retries failed (Anthropic outage)
# Model configuration
# MODEL_ID = "claude-sonnet-4-20250514"
# MODEL = ChatAnthropic(
#     model=MODEL_ID,
#     max_retries=10,
#     timeout=600,
#     max_tokens=MAX_OUTPUT_TOKENS
# )

# # MARK: - OPENAI
# MODEL_ID = "gpt-5"
# MODEL = ChatOpenAI(
#     model=MODEL_ID,
#     max_retries=10,
#     timeout=600,
#     max_tokens=MAX_OUTPUT_TOKENS
# )

# # MARK: - GOOGLE GENAI
MODEL_ID = "gemini-2.5-pro"
MODEL = ChatGoogleGenerativeAI(
    model=MODEL_ID,
    max_retries=10,
    timeout=600,
    max_tokens=MAX_OUTPUT_TOKENS
)

# MARK: - RUN NAME
# Create S3-safe run name with model ID (replace hyphens with underscores)
model_slug = MODEL_ID.replace("-", "_")
RUN_NAME = f"run_{datetime.now().strftime('%Y_%m_%d_%H%M%S')}_{model_slug}"