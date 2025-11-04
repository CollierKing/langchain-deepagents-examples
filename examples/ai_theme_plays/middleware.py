# MARK: - Imports
from dotenv import load_dotenv, find_dotenv
import os
import logging
import json
from langchain.agents.middleware import AgentMiddleware, wrap_model_call
from langchain_core.tools import tool
import boto3
from typing import Optional
from colorama import Fore, Back, Style, init
from config import MODEL

# MARK: - Environment
load_dotenv(find_dotenv(), override=False)

# MARK: - Logging Setup
init(autoreset=True)

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("deepagents")

# Suppress httpx logs
logging.getLogger("httpx").setLevel(logging.WARNING)


# MARK: - Content Truncation Middleware
class ContentTruncationMiddleware(AgentMiddleware):
    """Truncates message CONTENT to fit within token limits."""
    
    def __init__(self, max_tokens: int):
        super().__init__()
        self.max_tokens = max_tokens
        print(f"{Back.CYAN}{Fore.WHITE} ContentTruncationMiddleware initialized (max_tokens={max_tokens:,}) {Style.RESET_ALL}")
    
    def before_model(self, state, runtime):
        """Truncate message content if it exceeds token limit."""
        messages = state.get("messages", [])
        
        if not messages:
            return None
        
        try:
            # Calculate current token count
            try:
                current_tokens = MODEL.get_num_tokens_from_messages(messages)
            except:
                # Fallback: rough estimate (4 chars per token)
                total_chars = sum(len(str(getattr(m, 'content', ''))) for m in messages)
                current_tokens = total_chars // 4
            
            if current_tokens <= self.max_tokens:
                return None
            
            # Calculate how much we need to reduce
            tokens_to_remove = current_tokens - self.max_tokens
            reduction_ratio = self.max_tokens / current_tokens
            
            logger.warning(f"Content over limit: {current_tokens:,} > {self.max_tokens:,} tokens (need to remove {tokens_to_remove:,})")
            print(f"{Back.RED}{Fore.WHITE} Content exceeds limit: {current_tokens:,} > {self.max_tokens:,} tokens {Style.RESET_ALL}")
            print(f"{Fore.YELLOW}Reduction ratio: {reduction_ratio:.2%}{Style.RESET_ALL}")
            
            # Truncate message contents proportionally
            for message in messages:
                content = message.content
                
                # Handle list content (tool results)
                if isinstance(content, list):
                    for item in content:
                        if isinstance(item, dict) and 'text' in item:
                            text = item['text']
                            try:
                                # Try to parse as JSON and truncate arrays
                                data = json.loads(text)
                                if isinstance(data, dict):
                                    for key, value in data.items():
                                        if isinstance(value, list) and len(value) > 0:
                                            # Calculate target array size based on reduction ratio
                                            target_size = max(1, int(len(value) * reduction_ratio))
                                            if target_size < len(value):
                                                original_len = len(value)
                                                data[key] = value[:target_size]
                                                logger.info(f"Truncated {key} array: {original_len} → {target_size} items ({reduction_ratio:.2%})")
                                    item['text'] = json.dumps(data, indent=2)
                            except:
                                # Not JSON, truncate text proportionally
                                target_chars = max(1000, int(len(text) * reduction_ratio))
                                item['text'] = text[:target_chars] + "\n...[TRUNCATED]"
                                logger.info(f"Truncated text: {len(text):,} → {target_chars:,} chars ({reduction_ratio:.2%})")
                
                # Handle string content
                elif isinstance(content, str):
                    target_chars = max(1000, int(len(content) * reduction_ratio))
                    if target_chars < len(content):
                        message.content = content[:target_chars] + "\n...[TRUNCATED]"
                        logger.info(f"Truncated string: {len(content):,} → {target_chars:,} chars ({reduction_ratio:.2%})")
            
            # Verify we're now under limit
            try:
                new_tokens = MODEL.get_num_tokens_from_messages(messages)
            except:
                total_chars = sum(len(str(getattr(m, 'content', ''))) for m in messages)
                new_tokens = total_chars // 4
            
            saved_tokens = current_tokens - new_tokens
            print(f"{Back.YELLOW}{Fore.BLACK} TRUNCATED: {current_tokens:,} → {new_tokens:,} tokens (saved {saved_tokens:,}) {Style.RESET_ALL}")
            
            if new_tokens > self.max_tokens:
                logger.warning(f"Still over limit after truncation: {new_tokens:,} > {self.max_tokens:,}")
                print(f"{Back.RED}{Fore.WHITE} WARNING: Still over limit! {new_tokens:,} > {self.max_tokens:,} {Style.RESET_ALL}")
            
        except Exception as e:
            logger.warning(f"Error truncating content: {e}")
        
        return None


# MARK: - LoggingMiddleware
class LoggingMiddleware(AgentMiddleware):
    """Comprehensive logging middleware that tracks agent state, messages, tool calls, and more."""

    def __init__(self):
        super().__init__()
        print(
            f"{Back.MAGENTA}{Fore.WHITE} LoggingMiddleware initialized {Style.RESET_ALL}"
        )

    def after_model(self, state, runtime):
        """Log after model response - this is where we capture comprehensive logs"""
        self._log_agent_state(state)
        return None

    def _log_agent_state(self, state):
        """Comprehensive logging of agent state"""

        # Keep structured logging
        logger.info("=== AGENT STATE LOG ===")

        # Add colored console output for visual debugging
        print(f"\n{Back.BLUE}{Fore.WHITE} DEEP AGENT LOG {Style.RESET_ALL}")

        # Log current state keys
        state_keys = list(state.keys())
        logger.info(f"State keys: {state_keys}")
        print(f"{Fore.CYAN}State: {Fore.WHITE}{state_keys}")

        # Log messages
        messages = state.get("messages", [])
        logger.info(f"Total messages: {len(messages)}")

        if messages:
            last_message = messages[-1]
            msg_type = type(last_message).__name__
            content = getattr(last_message, "content", "No content")

            logger.info(f"Last message type: {msg_type}")
            logger.info(f"Last message content: {content}")

            print(f"{Fore.GREEN}Message: {Fore.YELLOW}{msg_type}")

            # Tool calls with error highlighting
            if hasattr(last_message, "tool_calls") and last_message.tool_calls:
                tool_count = len(last_message.tool_calls)
                logger.info(f"Tool calls found: {tool_count}")

                print(
                    f"{Back.GREEN}{Fore.BLACK} {tool_count} TOOL CALLS {Style.RESET_ALL}"
                )

                for i, tool_call in enumerate(last_message.tool_calls):
                    logger.info(f"Tool call {i}: {json.dumps(tool_call, indent=2)}")
                    tool_name = tool_call.get("name", "unknown")
                    print(f"  {Fore.MAGENTA}▶ {tool_name}")
            else:
                logger.info("No tool calls in last message")
                print(f"{Back.RED}{Fore.WHITE} NO TOOL CALLS {Style.RESET_ALL}")

        # File system and todos with visual indicators
        files = state.get("files", {})
        todos = state.get("todos", [])

        logger.info(f"Files in state: {list(files.keys())}")
        logger.info(f"Todos count: {len(todos)}")

        print(f"{Fore.BLUE}Files: {len(files)} | Todos: {len(todos)}")
        print(f"{Fore.CYAN}{'─' * 40}{Style.RESET_ALL}\n")


# MARK: - Validation File Tracker Middleware
class ValidationFileTrackerMiddleware(AgentMiddleware):
    """Tracks when validation files are written to ensure every queried company gets a file."""
    
    def __init__(self):
        super().__init__()
        print(f"{Back.CYAN}{Fore.WHITE} ValidationFileTrackerMiddleware initialized {Style.RESET_ALL}")
    
    def after_tool(self, state, runtime, tool_call, tool_result):
        """Detect when validation files are written and mark company as complete."""
        # Log ALL tool calls for debugging
        tool_name = tool_call.get("name", "unknown")
        logger.info(f"🔧 ValidationFileTracker: after_tool called for '{tool_name}'")
        
        if tool_name != "write_file":
            return None
        
        # Check if this is a validation file
        args = tool_call.get("args", {})
        file_path = args.get("file_path", "")
        
        # Normalize path (remove leading slash if present)
        normalized_path = file_path.lstrip("/")
        
        logger.info(f"🔧 write_file detected: path='{file_path}' normalized='{normalized_path}'")
        
        # Extract ticker from validation file path: validations/company_NVDA.json → NVDA
        if normalized_path.startswith("validations/company_") and normalized_path.endswith(".json"):
            ticker = normalized_path.replace("validations/company_", "").replace(".json", "")
            
            logger.info(f"🎯 Validation file detected for ticker: {ticker}")
            
            # Mark this company's validation file as written
            from tools import _company_state
            
            logger.info(f"🔧 Before mark_file_written: last_queried={_company_state.last_queried_company}, current_index={_company_state.current_index}")
            
            _company_state.mark_file_written(ticker)
            
            logger.info(f"🔧 After mark_file_written: last_queried={_company_state.last_queried_company}, current_index={_company_state.current_index}")
            logger.info(f"✓ Validation file written for {ticker}")
            print(f"{Back.GREEN}{Fore.BLACK} ✓ Saved validations/company_{ticker}.json {Style.RESET_ALL}")
        else:
            logger.info(f"⚠️ write_file path doesn't match validation pattern: '{normalized_path}'")
        
        return None


# MARK: - S3 Backend for FilesystemMiddleware
class S3Backend:
    """Backend that stores files in S3, implementing BackendProtocol for FilesystemMiddleware."""
    
    def __init__(self, bucket_name: str, run_name: str):
        from botocore.config import Config
        from datetime import datetime as dt
        
        self.datetime = dt  # Store for later use
        
        # Read AWS credentials from environment
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_endpoint_url = os.getenv("AWS_ENDPOINT_URL")
        aws_session_token = os.getenv("AWS_SESSION_TOKEN")

        # Create session
        session_kwargs = {}
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key
        if aws_session_token:
            session_kwargs["aws_session_token"] = aws_session_token

        s3_session = boto3.Session(**session_kwargs)

        # Create S3 client
        client_kwargs = {}
        if aws_endpoint_url:
            client_kwargs["endpoint_url"] = aws_endpoint_url
        client_kwargs["config"] = Config(signature_version="s3v4")

        self.s3_client = s3_session.client("s3", **client_kwargs)
        self.bucket = bucket_name
        self.run_prefix = f"deepagent_runs/{run_name}"
        
        print(f"{Back.CYAN}{Fore.WHITE} S3Backend initialized (bucket={bucket_name}, prefix={self.run_prefix}) {Style.RESET_ALL}")
    
    def _get_s3_key(self, file_path: str) -> str:
        """Convert virtual file path to S3 key."""
        # Handle both absolute-style paths and relative paths
        clean_path = file_path.lstrip('/')
        
        # If path starts with known input prefixes, use as-is
        if clean_path.startswith(('transcripts/', 'company_descriptions/', 'press_releases/')):
            return clean_path
        
        # Otherwise scope to run directory
        return f"{self.run_prefix}/{clean_path}"
    
    def ls_info(self, path: str) -> list[dict]:
        """List S3 objects in directory."""
        try:
            s3_key = self._get_s3_key(path)
            if not s3_key.endswith('/'):
                s3_key += '/'
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=s3_key,
                Delimiter='/'
            )
            
            results = []
            
            # Directories
            for prefix in response.get('CommonPrefixes', []):
                dir_path = '/' + prefix['Prefix'].removeprefix(self.run_prefix + '/').rstrip('/')
                results.append({
                    'path': dir_path + '/',
                    'is_dir': True,
                    'size': 0,
                    'modified_at': self.datetime.now().isoformat()
                })
            
            # Files
            for obj in response.get('Contents', []):
                file_path = '/' + obj['Key'].removeprefix(self.run_prefix + '/')
                # Skip the directory itself
                if file_path.rstrip('/') == path.rstrip('/'):
                    continue
                results.append({
                    'path': file_path,
                    'is_dir': False,
                    'size': obj['Size'],
                    'modified_at': obj['LastModified'].isoformat()
                })
            
            return results
        except Exception as e:
            logger.error(f"Error listing S3 path {path}: {e}")
            return []
    
    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> str:
        """Read file from S3 with line numbers."""
        try:
            s3_key = self._get_s3_key(file_path)
            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            
            lines = content.splitlines()
            
            # Apply offset and limit
            selected_lines = lines[offset:offset + limit] if limit else lines[offset:]
            
            # Format with line numbers (1-indexed)
            formatted_lines = [
                f"{i + offset + 1:6d}|{line}"
                for i, line in enumerate(selected_lines)
            ]
            
            return '\n'.join(formatted_lines) if formatted_lines else 'File is empty.'
        except Exception as e:
            return f"Error reading file: {str(e)}"
    
    def grep_raw(self, pattern: str, path: str | None = None, glob: str | None = None) -> list[dict] | str:
        """Search S3 files for pattern (simplified - downloads and searches)."""
        # For simplicity, just return error - grep in S3 is complex
        return "grep not supported for S3 backend (files are remote)"
    
    def glob_info(self, pattern: str, path: str = "/") -> list[dict]:
        """Glob match S3 objects."""
        import fnmatch
        
        try:
            # List all objects under path
            s3_key = self._get_s3_key(path)
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=s3_key
            )
            
            results = []
            for obj in response.get('Contents', []):
                file_path = '/' + obj['Key'].removeprefix(self.run_prefix + '/')
                
                # Match against pattern
                if fnmatch.fnmatch(file_path, pattern) or fnmatch.fnmatch(file_path.lstrip('/'), pattern):
                    results.append({
                        'path': file_path,
                        'is_dir': False,
                        'size': obj['Size'],
                        'modified_at': obj['LastModified'].isoformat()
                    })
            
            return results
        except Exception as e:
            logger.error(f"Error globbing S3 pattern {pattern}: {e}")
            return []
    
    def write(self, file_path: str, content: str):
        """Write file to S3."""
        from deepagents.backends.protocol import WriteResult
        
        try:
            s3_key = self._get_s3_key(file_path)
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=content.encode('utf-8')
            )
            
            logger.info(f"Wrote {s3_key} to S3")
            return WriteResult(path=file_path, files_update=None)  # None = external storage
        except Exception as e:
            error_msg = f"Error writing to S3: {str(e)}"
            logger.error(error_msg)
            return WriteResult(error=error_msg)
    
    def edit(self, file_path: str, old_string: str, new_string: str, replace_all: bool = False):
        """Edit file in S3 (read, modify, write back)."""
        from deepagents.backends.protocol import EditResult
        
        try:
            # Read current content
            s3_key = self._get_s3_key(file_path)
            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            
            # Perform replacement
            if replace_all:
                new_content = content.replace(old_string, new_string)
                occurrences = content.count(old_string)
            else:
                # Replace first occurrence only
                new_content = content.replace(old_string, new_string, 1)
                occurrences = 1 if old_string in content else 0
            
            if occurrences == 0:
                return EditResult(error=f"String not found in {file_path}")
            
            # Write back
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=new_content.encode('utf-8')
            )
            
            logger.info(f"Edited {s3_key} in S3 ({occurrences} occurrences)")
            return EditResult(path=file_path, files_update=None, occurrences=occurrences)
        except Exception as e:
            error_msg = f"Error editing file in S3: {str(e)}"
            logger.error(error_msg)
            return EditResult(error=error_msg)
