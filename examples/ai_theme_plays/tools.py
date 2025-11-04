# MARK: - Imports
from langchain_core.tools import tool
from utils import query_postgres, query_mongodb
from models import Company, PressRelease, CompanyBatchResponse, PressReleaseBatchResponse
import json


# MARK: - Sequential Batch State
class SequentialBatchState:
    """Global state to enforce sequential batch processing."""
    def __init__(self, batch_size: int):
        self.batch_size = batch_size
        self.expected_offset = 0
        self.completed = False
        self.recovered = False
    
    def recover_from_s3(self):
        """Recover state by checking which batch files already exist on S3."""
        if self.recovered:
            return
        
        try:
            from config import S3_BUCKET_NAME, RUN_NAME
            from middleware import S3Backend
            
            s3_backend = S3Backend(bucket_name=S3_BUCKET_NAME, run_name=RUN_NAME)
            
            # Find all existing batch files
            existing_files = s3_backend.glob_info('company_matches/batch_*.json')
            
            if not existing_files:
                self.recovered = True
                return
            
            # Find highest batch number (extract offset from filenames)
            max_offset = -1
            for file_info in existing_files:
                # Extract offset from path: company_matches/batch_0050.json → 50
                path = file_info['path'].lstrip('/')
                if path.startswith('company_matches/batch_') and path.endswith('.json'):
                    try:
                        offset_str = path.replace('company_matches/batch_', '').replace('.json', '')
                        offset = int(offset_str)
                        max_offset = max(max_offset, offset)
                    except ValueError:
                        pass
            
            if max_offset >= 0:
                # Resume from next batch after highest found
                self.expected_offset = max_offset + self.batch_size
                print(f"🔄 Recovered batch state: found batches up to offset {max_offset}, resuming at {self.expected_offset}")
                import logging
                logger = logging.getLogger("deepagents")
                logger.info(f"🔄 Batch state recovery: max_offset={max_offset}, expected_offset={self.expected_offset}")
            
            self.recovered = True
            
        except Exception as e:
            import logging
            logger = logging.getLogger("deepagents")
            logger.warning(f"Could not recover batch state from S3: {e}")
            self.recovered = True
    
    def validate_and_update(self, requested_offset: int) -> tuple[bool, str]:
        """Validate offset and update state. Returns (is_valid, error_message)."""
        # Try to recover state on first call
        if not self.recovered:
            self.recover_from_s3()
        
        if self.completed:
            return False, "❌ ERROR: All batches already processed (has_more=false was returned)"
        
        if requested_offset != self.expected_offset:
            error = (
                f"❌ SEQUENTIAL BATCH VIOLATION ❌\n"
                f"Expected offset: {self.expected_offset}\n"
                f"Requested offset: {requested_offset}\n"
                f"You MUST process batches sequentially.\n"
                f"Next valid offset is: {self.expected_offset}"
            )
            return False, error
        
        # Valid - increment expected offset immediately
        self.expected_offset += self.batch_size
        return True, ""
    
    def mark_complete(self):
        """Mark sequential processing as complete."""
        self.completed = True

# Global instance - will be initialized when tool is created
_batch_state = None


# MARK: - Batch Consolidation Tool
@tool
def consolidate_batch_files() -> str:
    """
    Automatically consolidate all company_matches/batch_*.json files into matched_companies.json.
    Reads all batch files from S3, extracts matches, ranks them, and writes the top companies.
    
    Call this AFTER processing all batches (when has_more=false).
    
    Returns:
        Confirmation message with match count
    """
    try:
        from config import S3_BUCKET_NAME, RUN_NAME, TOP_COMPANY_MATCHES
        from middleware import S3Backend
        from models import CompanyMatchesOutput, MatchMetadata, SummaryStatistics, CompanyMatch
        from datetime import datetime
        
        s3_backend = S3Backend(bucket_name=S3_BUCKET_NAME, run_name=RUN_NAME)
        
        # List all batch files
        batch_files = s3_backend.glob_info('company_matches/batch_*.json')
        
        if not batch_files:
            return json.dumps({"error": "No batch files found"}, indent=2)
        
        # Read and parse each batch file
        all_matches = []
        themes_set = set()
        
        for file_info in batch_files:
            file_path = file_info['path']
            content = s3_backend.read(file_path, offset=0, limit=999999)
            
            # Strip line numbers
            lines = content.split('\n')
            clean_content = '\n'.join(line.split('|', 1)[1] if '|' in line else line for line in lines)
            
            # Parse and extract matches
            batch_data = json.loads(clean_content)
            matches = batch_data.get('matches', batch_data.get('preliminary_matches', []))
            all_matches.extend(matches)
            
            # Collect themes
            for match in matches:
                themes_set.update(match.get('matched_themes', []))
        
        # Sort by score and take top N
        all_matches.sort(key=lambda x: x.get('score', 0), reverse=True)
        top_matches_dicts = all_matches[:TOP_COMPANY_MATCHES]
        
        # Convert to Pydantic models and assign ranks
        top_matches = []
        for i, match_dict in enumerate(top_matches_dicts, start=1):
            company_match = CompanyMatch(
                ticker=match_dict['ticker'],
                company_name=match_dict['company_name'],
                rank=i,
                score=match_dict['score'],
                matched_themes=match_dict.get('matched_themes', []),
                alignment_factors=match_dict.get('alignment_factors', [])
            )
            top_matches.append(company_match)
        
        # Calculate statistics
        theme_counts = {}
        for match in top_matches:
            for theme in match.matched_themes:
                theme_counts[theme] = theme_counts.get(theme, 0) + 1
        
        score_ranges = {
            '0.95-1.00': len([m for m in top_matches if m.score >= 0.95]),
            '0.90-0.94': len([m for m in top_matches if 0.90 <= m.score < 0.95]),
            '0.85-0.89': len([m for m in top_matches if 0.85 <= m.score < 0.90]),
            '0.80-0.84': len([m for m in top_matches if 0.80 <= m.score < 0.85]),
        }
        
        avg_score = sum(m.score for m in top_matches) / len(top_matches) if top_matches else 0
        
        # Create Pydantic models
        metadata = MatchMetadata(
            total_companies_analyzed=len(all_matches),
            total_batches_processed=len(batch_files),
            analysis_completion_date=datetime.now().strftime('%Y-%m-%d'),
            themes_analyzed=sorted(list(themes_set))
        )
        
        summary_stats = SummaryStatistics(
            theme_distribution=theme_counts,
            average_score=round(avg_score, 2),
            score_ranges=score_ranges,
            industry_representation={}
        )
        
        # Create final output using Pydantic model
        consolidated = CompanyMatchesOutput(
            metadata=metadata,
            matches=top_matches,
            summary_statistics=summary_stats
        )
        
        # Write to S3
        s3_backend.write('matched_companies.json', consolidated.model_dump_json(indent=2))
        
        return json.dumps({
            'status': 'success',
            'total_matches_found': len(all_matches),
            'top_companies_selected': len(top_matches),
            'batches_processed': len(batch_files),
            'message': f'Consolidated {len(all_matches)} matches from {len(batch_files)} batches into matched_companies.json'
        }, indent=2)
    except Exception as e:
        return json.dumps({'error': f'Consolidation failed: {str(e)}'}, indent=2)


# MARK: - Company Query Tools
@tool
def get_companies_from_postgres(offset: int = 0, limit: int = 100) -> str:
    """
    Query PostgreSQL for company data in chunks to avoid context overflow.
    ENFORCES sequential offset processing - you MUST start at 0 and increment by batch_size.
    
    Args:
        offset: Starting position (MUST be sequential: 0, 50, 100, 150...)
        limit: Number of companies to return (default: 100, max: 500)
    
    Returns:
        JSON with companies, total_count, offset, and has_more fields
    """
    global _batch_state
    
    import logging
    logger = logging.getLogger("deepagents")
    
    # Initialize state on first call
    from config import COMPANY_BATCH_SIZE
    logger.info(f"🔧 get_companies_from_postgres: offset={offset}, limit={limit}, COMPANY_BATCH_SIZE={COMPANY_BATCH_SIZE}")
    logger.info(f"🔧 State before: _batch_state exists={_batch_state is not None}, expected_offset={_batch_state.expected_offset if _batch_state else 'N/A'}")
    
    if _batch_state is None:
        _batch_state = SequentialBatchState(batch_size=COMPANY_BATCH_SIZE)
        logger.info(f"🔄 Created new batch state with batch_size={COMPANY_BATCH_SIZE}")
        print(f"🔄 Created new batch state with batch_size={COMPANY_BATCH_SIZE}")
    
    # ENFORCE sequential processing
    is_valid, error_msg = _batch_state.validate_and_update(offset)
    logger.info(f"🔧 Validation: is_valid={is_valid}")
    
    if not is_valid:
        logger.error(f"❌ BATCH REJECTED: {error_msg}")
        print(f"❌ BATCH REJECTED: {error_msg}")
        # Return error in same format but with error field
        error_response = {
            "error": error_msg,
            "companies": [],
            "total_count": 0,
            "offset": offset,
            "limit": limit,
            "returned": 0,
            "has_more": False
        }
        return json.dumps(error_response, indent=2)
    
    logger.info(f"✅ Batch accepted, querying database offset={offset}, limit={limit}")
    
    limit = min(limit, 500)  # Cap at 500 to prevent overflow
    
    sql = '''
    SELECT ticker, industry, company_name, company_desc
    FROM cc_ticker_company_detail
    WHERE COALESCE(no_refresh_flag, 1) <> 1
    AND sector = 'Technology'
    ORDER BY ticker
    LIMIT %s OFFSET %s
    '''
    
    # Also get total count
    count_sql = '''
    SELECT COUNT(*) as total
    FROM cc_ticker_company_detail
    WHERE COALESCE(no_refresh_flag, 1) <> 1
    AND sector = 'Technology'
    '''
    
    companies_raw = query_postgres(sql, (limit, offset))
    count_result = query_postgres(count_sql)
    total_count = count_result[0]['total'] if count_result else 0
    
    # Filter and validate companies using the Company model
    valid_companies = [
        Company(
            ticker=row["ticker"],
            company_name=row["company_name"],
            company_desc=row["company_desc"],
            industry=row.get("industry")
        )
        for row in companies_raw
        if Company.is_valid_record(row)
    ]
    
    has_more = (offset + len(valid_companies)) < total_count
    
    # Mark as complete if no more batches
    if not has_more:
        _batch_state.mark_complete()
    
    # Create response using the response model
    response = CompanyBatchResponse(
        companies=valid_companies,
        total_count=total_count,
        offset=offset,
        limit=limit,
        returned=len(valid_companies),
        has_more=has_more
    )
    
    return response.model_dump_json(indent=2)


# MARK: - Sequential Company State  
class SequentialCompanyState:
    """Global state to enforce sequential company processing from matched_companies list."""
    def __init__(self):
        self.companies_to_validate = []  # Ordered list of companies from matched_companies.json
        self.current_index = 0  # Which company we're currently on
        self.last_queried_company = None  # Last company queried (must write file before next query)
        self.initialized = False
    
    def recover_from_s3(self):
        """Recover state by checking which validation files already exist on S3."""
        try:
            from config import S3_BUCKET_NAME, RUN_NAME
            from middleware import S3Backend
            
            s3_backend = S3Backend(bucket_name=S3_BUCKET_NAME, run_name=RUN_NAME)
            
            # Find all existing validation files
            existing_files = s3_backend.glob_info('validations/company_*.json')
            completed_tickers = set()
            
            for file_info in existing_files:
                # Extract ticker from path: validations/company_NVDA.json → NVDA
                path = file_info['path'].lstrip('/')
                if path.startswith('validations/company_') and path.endswith('.json'):
                    ticker = path.replace('validations/company_', '').replace('.json', '')
                    completed_tickers.add(ticker)
            
            # Update current_index to skip already completed companies
            for i, ticker in enumerate(self.companies_to_validate):
                if ticker not in completed_tickers:
                    self.current_index = i
                    break
            else:
                # All companies completed
                self.current_index = len(self.companies_to_validate)
            
            # Clear last_queried_company since we're recovering
            self.last_queried_company = None
            
            if completed_tickers:
                print(f"🔄 Recovered state: {len(completed_tickers)} companies already validated, resuming at index {self.current_index}")
                import logging
                logger = logging.getLogger("deepagents")
                logger.info(f"🔄 State recovery: completed={completed_tickers}, current_index={self.current_index}/{len(self.companies_to_validate)}")
            
        except Exception as e:
            import logging
            logger = logging.getLogger("deepagents")
            logger.warning(f"Could not recover state from S3: {e}")
    
    def initialize_from_matches(self, matched_companies_data: dict):
        """Initialize company list from matched_companies.json data."""
        if self.initialized:
            return
        
        # Extract company list - could be in 'matches' or 'top_100_matches'
        matches = matched_companies_data.get('matches') or matched_companies_data.get('top_100_matches', [])
        self.companies_to_validate = [company['ticker'] for company in matches]
        self.initialized = True
        
        # Recover state from S3 (check what's already done)
        self.recover_from_s3()
        
        remaining = len(self.companies_to_validate) - self.current_index
        print(f"📋 Initialized validation queue: {len(self.companies_to_validate)} companies total, {remaining} remaining to validate")
    
    def validate_and_update(self, symbols: str, skip: int, matched_companies_json: str = None) -> tuple[bool, str]:
        """Validate company query. Returns (is_valid, error_message)."""
        # Initialize list on first call if provided
        if not self.initialized and matched_companies_json:
            try:
                data = json.loads(matched_companies_json)
                self.initialize_from_matches(data)
            except:
                pass  # Will error below if not initialized
        
        if not self.initialized:
            error = (
                f"❌ NOT INITIALIZED ❌\n"
                f"You must first read matched_companies.json to initialize the validation queue.\n"
                f"Call read_file('matched_companies.json') before querying companies."
            )
            return False, error
        
        symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
        
        # Must process ONE company at a time
        if len(symbol_list) != 1:
            error = (
                f"❌ COMPANY VALIDATION VIOLATION ❌\n"
                f"You must process ONE company at a time.\n"
                f"Requested symbols: {symbols}\n"
                f"Split into separate calls, one symbol each."
            )
            return False, error
        
        symbol = symbol_list[0]
        
        # Check if last company has been written to file
        # BUT FIRST: Check S3 to see if file actually exists (state recovery)
        if self.last_queried_company is not None:
            # Check S3 to see if the file was written
            try:
                from config import S3_BUCKET_NAME, RUN_NAME
                from middleware import S3Backend
                
                s3_backend = S3Backend(bucket_name=S3_BUCKET_NAME, run_name=RUN_NAME)
                validation_path = f'validations/company_{self.last_queried_company}.json'
                
                # Try to read the file - if it exists, clear last_queried and move on
                try:
                    s3_backend.read(validation_path, offset=0, limit=1)
                    # File exists! Clear the block
                    import logging
                    logger = logging.getLogger("deepagents")
                    logger.info(f"✅ Found {validation_path} in S3 - clearing block")
                    print(f"✅ Found {validation_path} in S3 - clearing block and moving to next company")
                    self.mark_file_written(self.last_queried_company)
                except:
                    # File doesn't exist - block the query
                    error = (
                        f"❌ CANNOT QUERY {symbol} - MUST WRITE FILE FIRST ❌\n\n"
                        f"You already queried press releases for: {self.last_queried_company}\n"
                        f"Before you can query {symbol}, you MUST:\n\n"
                        f"1. Call write_file('validations/company_{self.last_queried_company}.json', <validation_data>)\n"
                        f"2. Include fields: ticker, company_name, original_themes, original_score, etc.\n"
                        f"3. Even if no evidence found, write with supports_themes=false\n\n"
                        f"DO NOT re-query {self.last_queried_company}. WRITE THE FILE."
                    )
                    return False, error
            except Exception as e:
                import logging
                logger = logging.getLogger("deepagents")
                logger.warning(f"Error checking S3 for validation file: {e}")
        
        # Check if we've processed all companies
        if self.current_index >= len(self.companies_to_validate):
            error = (
                f"✅ ALL COMPANIES VALIDATED ✅\n"
                f"You have already processed all {len(self.companies_to_validate)} companies.\n"
                f"Move to consolidation step."
            )
            return False, error
        
        # Must process companies in order
        expected_company = self.companies_to_validate[self.current_index]
        if symbol != expected_company:
            error = (
                f"❌ WRONG COMPANY ORDER ❌\n"
                f"Expected company: {expected_company} (position {self.current_index + 1}/{len(self.companies_to_validate)})\n"
                f"Requested company: {symbol}\n"
                f"You MUST process companies in the order they appear in matched_companies.json"
            )
            return False, error
        
        # No pagination (must use skip=0)
        if skip != 0:
            error = (
                f"❌ PAGINATION VIOLATION ❌\n"
                f"Do not paginate press releases (skip must be 0).\n"
                f"Requested skip: {skip}\n"
                f"Fetch all needed releases in a single call."
            )
            return False, error
        
        # Valid - set as last_queried (will be cleared when file is written)
        self.last_queried_company = symbol
        print(f"✓ Querying {symbol} ({self.current_index + 1}/{len(self.companies_to_validate)})")
        return True, ""
    
    def mark_file_written(self, symbol: str):
        """Mark that validation file has been written for this company."""
        if self.last_queried_company == symbol:
            self.last_queried_company = None  # Clear - can now query next company
            self.current_index += 1  # Move to next company
            print(f"✓ Validation file written for {symbol}, ready for next company")

# Global instance
_company_state = SequentialCompanyState()


# MARK: - Validation Consolidation Tool
@tool
def consolidate_validation_files() -> str:
    """
    Automatically consolidate all validations/company_*.json files into validated_results.json.
    Reads all validation files from S3, combines them, and writes the result.
    
    Call this AFTER validating all companies.
    
    Returns:
        Confirmation message with validation count
    """
    try:
        from config import S3_BUCKET_NAME, RUN_NAME
        from middleware import S3Backend
        
        s3_backend = S3Backend(bucket_name=S3_BUCKET_NAME, run_name=RUN_NAME)
        
        # List all validation files
        validation_files = s3_backend.glob_info('validations/company_*.json')
        
        if not validation_files:
            return json.dumps({"error": "No validation files found"}, indent=2)
        
        # Read and parse each validation file
        all_validations = []
        for file_info in validation_files:
            file_path = file_info['path']
            content = s3_backend.read(file_path, offset=0, limit=999999)
            
            # Strip line numbers
            lines = content.split('\n')
            clean_content = '\n'.join(line.split('|', 1)[1] if '|' in line else line for line in lines)
            
            # Parse and add to list
            validation_data = json.loads(clean_content)
            if isinstance(validation_data, list):
                all_validations.extend(validation_data)
            else:
                all_validations.append(validation_data)
        
        # Create consolidated output using Pydantic model
        from datetime import datetime
        from models import ValidationOutput, ValidationMetadata, CompanyValidation
        
        # Convert dict validations to Pydantic models
        validated_companies = [CompanyValidation(**v) if isinstance(v, dict) else v for v in all_validations]
        
        # Create output using model
        consolidated = ValidationOutput(
            total_validations=len(validated_companies),
            validations=validated_companies,
            metadata=ValidationMetadata(
                consolidation_date=datetime.now().strftime('%Y-%m-%d'),
                files_processed=len(validation_files)
            )
        )
        
        # Write to S3
        s3_backend.write('validated_results.json', consolidated.model_dump_json(indent=2))
        
        return json.dumps({
            "status": "success",
            "total_validations": len(all_validations),
            "message": f"Consolidated {len(all_validations)} validations from {len(validation_files)} files into validated_results.json"
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Consolidation failed: {str(e)}"}, indent=2)


# MARK: - Validation Initialization Tool
@tool  
def get_company_tickers_from_matched_file() -> str:
    """
    Extract ONLY the ticker symbols from matched_companies.json.
    MUST be called FIRST to initialize the validation queue.
    
    This bypasses reading the full file content (which would be truncated).
    Instead, it reads directly from S3 and extracts just the ticker list.
    
    Returns:
        JSON with ticker list and count
    """
    global _company_state
    
    try:
        # Use S3Backend to read the file
        from config import S3_BUCKET_NAME, RUN_NAME
        from middleware import S3Backend
        
        # Create S3Backend instance
        s3_backend = S3Backend(bucket_name=S3_BUCKET_NAME, run_name=RUN_NAME)
        
        # Read matched_companies.json (returns formatted with line numbers)
        raw_content = s3_backend.read('matched_companies.json', offset=0, limit=999999)
        
        # Strip line numbers and parse JSON
        lines = raw_content.split('\n')
        content = '\n'.join(line.split('|', 1)[1] if '|' in line else line for line in lines)
        data = json.loads(content)
        
        # Extract ONLY tickers from matches array
        matches = data.get('matches', [])
        tickers = [company['ticker'] for company in matches]
        
        # Initialize validation queue
        _company_state.companies_to_validate = tickers
        _company_state.initialized = True
        
        return json.dumps({
            "status": "initialized",
            "total_companies": len(tickers),
            "companies": tickers,
            "message": f"Validation queue initialized with {len(tickers)} companies. Process them IN ORDER."
        }, indent=2)
    except Exception as e:
        return json.dumps({
            "error": f"Failed to extract tickers: {str(e)}"
        }, indent=2)


# MARK: - Final Ranking Tool
@tool
def merge_and_rank_companies() -> str:
    """
    Merge matched_companies.json and validated_results.json, re-rank by final score.
    Does the heavy lifting without loading full files into agent context.
    
    Returns:
        Confirmation message with path to final_rankings.json
    """
    try:
        from config import S3_BUCKET_NAME, RUN_NAME, TOP_COMPANY_MATCHES
        from middleware import S3Backend
        from models import FinalCompanyRanking, FinalOutputMetadata, FinalOutput, SummaryStatistics
        
        s3_backend = S3Backend(bucket_name=S3_BUCKET_NAME, run_name=RUN_NAME)
        
        # Read matched_companies.json
        matches_content = s3_backend.read('matched_companies.json', offset=0, limit=999999)
        lines = matches_content.split('\n')
        clean = '\n'.join(line.split('|', 1)[1] if '|' in line else line for line in lines)
        matched_data = json.loads(clean)
        
        # Read validated_results.json
        try:
            validated_content = s3_backend.read('validated_results.json', offset=0, limit=999999)
            lines = validated_content.split('\n')
            clean = '\n'.join(line.split('|', 1)[1] if '|' in line else line for line in lines)
            validated_data = json.loads(clean)
            validations = {v['ticker']: v for v in validated_data.get('validations', [])}
        except:
            validations = {}  # No validations available
        
        # Merge data using Pydantic models
        merged = []
        for company in matched_data.get('matches', []):
            ticker = company['ticker']
            validation = validations.get(ticker, {})
            
            # Use adjusted_score if available, else original score
            final_score = validation.get('adjusted_score') or company['score']
            
            # Create FinalCompanyRanking instance
            merged_company = FinalCompanyRanking(
                ticker=ticker,
                company_name=company['company_name'],
                rank=0,  # Will be set after sorting
                final_score=final_score,
                matched_themes=company['matched_themes'],
                alignment_factors=company['alignment_factors'],
                validation_status=validation.get('validation_status'),
                press_release_validation=validation.get('press_release_validation'),
                evidence_summary=validation.get('evidence_summary'),
                key_evidence=validation.get('key_evidence'),
                confidence_adjustment=validation.get('confidence_adjustment'),
                notes=validation.get('notes')
            )
            merged.append(merged_company)
        
        # Re-rank by final_score
        merged.sort(key=lambda x: x.final_score, reverse=True)
        
        # Take top N and assign ranks
        top_companies = merged[:TOP_COMPANY_MATCHES]
        for i, company in enumerate(top_companies, start=1):
            company.rank = i
        
        # Create metadata using Pydantic model
        from datetime import datetime
        
        metadata = FinalOutputMetadata(
            total_companies_analyzed=matched_data.get('metadata', {}).get('total_companies_analyzed', 0),
            total_companies_validated=len(validations),
            analysis_completion_date=datetime.now().strftime('%Y-%m-%d'),
            themes_analyzed=matched_data.get('metadata', {}).get('themes_analyzed', []),
            validation_summary=f'Validated {len(validations)} of {len(merged)} matched companies'
        )
        
        # Create summary statistics
        summary_stats = SummaryStatistics(**matched_data.get('summary_statistics', {
            'theme_distribution': {},
            'average_score': 0.0,
            'score_ranges': {},
            'industry_representation': {}
        }))
        
        # Create final output using Pydantic model
        final_output = FinalOutput(
            metadata=metadata,
            companies=top_companies,
            summary_statistics=summary_stats
        )
        
        # Write final_rankings.json
        s3_backend.write('final_rankings.json', final_output.model_dump_json(indent=2))
        
        return json.dumps({
            'status': 'success',
            'total_ranked': len(top_companies),
            'validations_applied': len(validations),
            'message': f'Created final_rankings.json with top {TOP_COMPANY_MATCHES} companies re-ranked by final scores'
        }, indent=2)
    except Exception as e:
        return json.dumps({'error': f'Merge failed: {str(e)}'}, indent=2)


# MARK: - Press Release Query Tools
@tool
def get_press_releases_from_mongodb(symbols: str, skip: int = 0, limit: int = 50) -> str:
    """
    Query MongoDB for press releases filtered by ticker symbols.
    ENFORCES one company at a time, no duplicates, no pagination (skip must be 0).
    
    Args:
        symbols: Single ticker symbol (e.g., "NVDA") - only ONE at a time
        skip: MUST be 0 (no pagination allowed)
        limit: Number of releases to return (default: 50, max: 200)
    
    Returns:
        JSON with press releases, total_count, skip, and has_more fields
    """
    global _company_state
    
    # ENFORCE sequential company processing
    is_valid, error_msg = _company_state.validate_and_update(symbols, skip)
    if not is_valid:
        import logging
        logger = logging.getLogger("deepagents")
        logger.error(f"🚫 get_press_releases_from_mongodb BLOCKED: {error_msg}")
        print(f"\n{'='*60}")
        print(f"🚫 TOOL ERROR: get_press_releases_from_mongodb")
        print(f"{'='*60}")
        print(error_msg)
        print(f"{'='*60}\n")
        
        # Return error in same format but with error field
        error_response = {
            "error": error_msg,
            "press_releases": [],
            "total_count": 0,
            "skip": skip,
            "limit": limit,
            "returned": 0,
            "has_more": False
        }
        return json.dumps(error_response, indent=2)
    
    limit = min(limit, 200)  # Cap at 200 to prevent overflow
    
    # Parse comma-separated symbols
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    
    query = {
        "doc_type": "press_release",
        "symbol": {"$in": symbol_list},
        "announcements": {"$exists": True, "$ne": None}
    }
    projection = {
        "_id": 0,
        "symbol": 1,
        "date": 1,
        "pr_title": 1,
        "content": 1,
        "pr_link": 1
    }
    
    # Get total count
    from pymongo import MongoClient
    import os
    client = MongoClient(os.getenv("MONGODB_URI"))
    db = client[os.getenv("MONGODB_DATABASE")]
    total_count = db["documents"].count_documents(query)
    client.close()
    
    # Get paginated results
    releases_raw = query_mongodb(
        collection_name="documents",
        query=query,
        projection=projection,
        limit=limit + skip  # Get all up to skip+limit, then slice
    )
    
    # Skip and limit
    releases_raw = releases_raw[skip:skip+limit]
    
    # Filter and validate press releases using the PressRelease model
    valid_releases = [
        PressRelease(
            symbol=row.get("symbol"),
            date=row.get("date"),
            pr_title=row.get("pr_title"),
            content=row.get("content"),
            pr_link=row.get("pr_link")
        )
        for row in releases_raw
        if PressRelease.is_valid_record(row)
    ]
    
    has_more = (skip + len(valid_releases)) < total_count
    
    # Create response using the response model
    response = PressReleaseBatchResponse(
        press_releases=valid_releases,
        total_count=total_count,
        skip=skip,
        limit=limit,
        returned=len(valid_releases),
        has_more=has_more
    )
    
    return response.model_dump_json(indent=2)

