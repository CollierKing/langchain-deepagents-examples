# MARK: - Imports
from pydantic import BaseModel, Field
from typing import List, Optional


# MARK: - Database Record Models
class Company(BaseModel):
    """Model for a single company from PostgreSQL"""
    ticker: str = Field(..., description="Stock ticker symbol")
    company_name: str = Field(..., description="Company name")
    company_desc: str = Field(..., description="Company description")
    industry: Optional[str] = Field(None, description="Industry/sector")
    
    @classmethod
    def is_valid_record(cls, row: dict) -> bool:
        """Check if a database row has all required fields (not NULL)"""
        return bool(
            row.get("ticker") and 
            row.get("company_name") and 
            row.get("company_desc")
        )


class CompanyBatchResponse(BaseModel):
    """Response model for get_companies_from_postgres tool"""
    companies: List[Company] = Field(..., description="List of companies in this batch")
    total_count: int = Field(..., description="Total number of companies in database")
    offset: int = Field(..., description="Starting offset for this batch")
    limit: int = Field(..., description="Maximum number of companies requested")
    returned: int = Field(..., description="Actual number of companies returned")
    has_more: bool = Field(..., description="Whether more companies exist beyond this batch")


class PressRelease(BaseModel):
    """Model for a single press release from MongoDB"""
    symbol: str = Field(..., description="Stock ticker symbol")
    date: Optional[str] = Field(None, description="Publication date")
    pr_title: str = Field(..., description="Press release title")
    content: str = Field(..., description="Full content of press release")
    pr_link: Optional[str] = Field(None, description="Link to full release")
    
    @classmethod
    def is_valid_record(cls, row: dict) -> bool:
        """Check if a database document has all required fields"""
        return bool(
            row.get("symbol") and 
            row.get("pr_title") and 
            row.get("content")
        )


class PressReleaseBatchResponse(BaseModel):
    """Response model for get_press_releases_from_mongodb tool"""
    press_releases: List[PressRelease] = Field(..., description="List of press releases for this company")
    total_count: int = Field(..., description="Total number of press releases for this company")
    skip: int = Field(..., description="Number of documents skipped (always 0)")
    limit: int = Field(..., description="Maximum number of press releases requested")
    returned: int = Field(..., description="Actual number of press releases returned")
    has_more: bool = Field(..., description="Whether more press releases exist for this company")


# MARK: - Output Models

# MARK: Themes Output
class Theme(BaseModel):
    theme: str = Field(..., description="Name of the identified theme")
    description: str = Field(..., description="Detailed explanation of the theme")
    key_points: List[str] = Field(..., description="Main points and characteristics of this theme")
    business_impact: str = Field(..., description="How this theme impacts businesses and markets")


class Trend(BaseModel):
    trend: str = Field(..., description="Name of the identified trend")
    description: str = Field(..., description="Detailed explanation of the trend")
    drivers: List[str] = Field(..., description="Key factors driving this trend")
    implications: str = Field(..., description="Implications and consequences of this trend")


class FocusArea(BaseModel):
    area: str = Field(..., description="Name of the focus area")
    description: str = Field(..., description="Detailed explanation of the focus area")
    key_initiatives: List[str] = Field(..., description="Major initiatives and projects in this area")
    market_size: str = Field(..., description="Estimated market size and opportunity")


class StrategicInsights(BaseModel):
    market_readiness: str = Field(..., description="Assessment of market maturity and readiness")
    competitive_moats: List[str] = Field(..., description="Key competitive advantages and barriers to entry")
    growth_drivers: List[str] = Field(..., description="Primary factors driving growth in this space")
    key_partnerships: List[str] = Field(..., description="Important strategic partnerships and collaborations")


class ThemesOutput(BaseModel):
    themes: List[Theme] = Field(..., description="Major themes identified from the transcript")
    trends: List[Trend] = Field(..., description="Key trends shaping the industry and market")
    focus_areas: List[FocusArea] = Field(..., description="Strategic focus areas for investment and development")
    strategic_insights: StrategicInsights = Field(..., description="High-level strategic insights and analysis")


# MARK: Company Matches Output
class BaseCompanyInfo(BaseModel):
    """Base model with common company fields"""
    ticker: str = Field(..., description="Stock ticker symbol")
    company_name: str = Field(..., description="Full company name")


class CompanyMatch(BaseCompanyInfo):
    rank: int = Field(..., description="Ranking position (1-100)")
    score: float = Field(..., description="Match score from 0.0 to 1.0 indicating alignment strength")
    matched_themes: List[str] = Field(..., description="List of themes this company aligns with")
    alignment_factors: List[str] = Field(..., description="Specific reasons why this company matches the themes")


class MatchMetadata(BaseModel):
    total_companies_analyzed: int = Field(..., description="Total number of companies reviewed across all batches")
    total_batches_processed: int = Field(..., description="Number of batches processed")
    analysis_completion_date: str = Field(..., description="Date analysis was completed (YYYY-MM-DD)")
    themes_analyzed: List[str] = Field(..., description="List of all themes used for matching")


class SummaryStatistics(BaseModel):
    theme_distribution: dict = Field(..., description="Count of companies per theme")
    average_score: float = Field(..., description="Average match score across all top companies")
    score_ranges: dict = Field(..., description="Distribution of scores by range (e.g., '0.90+': 20)")
    industry_representation: dict = Field(..., description="Count of companies by industry")


class CompanyMatchesOutput(BaseModel):
    metadata: MatchMetadata = Field(..., description="Analysis metadata including counts and themes")
    matches: List[CompanyMatch] = Field(..., description="Top companies ranked by match score")
    summary_statistics: SummaryStatistics = Field(..., description="Statistical summary of matching results")


# MARK: Validation Output
class EvidenceItem(BaseModel):
    """Single piece of evidence from a press release"""
    evidence: str = Field(..., description="The key evidence or finding from this press release")
    pr_title: str = Field(..., description="Title of the press release containing this evidence")
    pr_link: Optional[str] = Field(None, description="Link to the full press release")


class CompanyValidation(BaseCompanyInfo):
    original_themes: List[str] = Field(..., description="Themes originally matched to this company")
    original_score: float = Field(..., description="Original match score before press release validation")
    press_release_validation: str = Field(..., description="Summary assessment of validation quality (e.g., EXCEPTIONAL, STRONG, WEAK)")
    supports_themes: str | bool = Field(..., description="Whether press releases support the matched themes (true/false or descriptive string)")
    evidence_summary: str = Field(..., description="Comprehensive summary of all evidence found in press releases")
    validation_status: str = Field(..., description="Final validation status (e.g., VALIDATED, UNVALIDATED, CONTRADICTED)")
    confidence_adjustment: float = Field(..., description="Adjustment to confidence score based on press release evidence (-1.0 to +1.0)")
    notes: str = Field(..., description="Additional context, observations, or caveats about the validation")
    adjusted_score: Optional[float] = Field(None, description="Final score after applying confidence adjustment")
    key_evidence: Optional[List[EvidenceItem]] = Field(None, description="List of the most important evidence with associated press release details")
    relevance_score: Optional[float] = Field(None, description="How relevant the press releases are to the themes (0.0 to 1.0)")


class ValidationMetadata(BaseModel):
    consolidation_date: str = Field(..., description="Date validations were consolidated")
    files_processed: int = Field(..., description="Number of individual validation files processed")


class ValidationOutput(BaseModel):
    total_validations: int = Field(..., description="Total number of companies validated")
    validations: List[CompanyValidation] = Field(..., description="Detailed validation results for each company")
    metadata: ValidationMetadata = Field(..., description="Consolidation metadata")


# MARK: Final Combined Output
class FinalCompanyRanking(BaseCompanyInfo):
    """Combined company match and validation data with final ranking"""
    rank: int = Field(..., description="Final ranking position (1-100)")
    final_score: float = Field(..., description="Final score (adjusted_score from validation if available, else original_score)")
    # From CompanyMatch
    matched_themes: List[str] = Field(..., description="List of themes this company aligns with")
    alignment_factors: List[str] = Field(..., description="Specific reasons why this company matches the themes")
    # From CompanyValidation (all optional since not all companies may have validation)
    validation_status: Optional[str] = Field(None, description="Validation status if press release validation was performed")
    press_release_validation: Optional[str] = Field(None, description="Quality of validation (EXCEPTIONAL, STRONG, WEAK)")
    evidence_summary: Optional[str] = Field(None, description="Summary of press release evidence if available")
    key_evidence: Optional[List[EvidenceItem]] = Field(None, description="Key evidence with press release details if available")
    confidence_adjustment: Optional[float] = Field(None, description="Confidence adjustment from validation if available")
    notes: Optional[str] = Field(None, description="Combined notes from matching and validation")


class FinalOutputMetadata(BaseModel):
    """Metadata for final combined analysis"""
    total_companies_analyzed: int = Field(..., description="Total companies reviewed in matching phase")
    total_companies_validated: int = Field(..., description="Number of companies that went through press release validation")
    analysis_completion_date: str = Field(..., description="Date analysis was completed (YYYY-MM-DD)")
    themes_analyzed: List[str] = Field(..., description="List of all themes used for analysis")
    validation_summary: str = Field(..., description="Summary of validation process and findings")


class FinalOutput(BaseModel):
    """Final combined output merging matches and validations with re-ranked companies"""
    metadata: FinalOutputMetadata = Field(..., description="Combined analysis metadata")
    companies: List[FinalCompanyRanking] = Field(..., description="Top companies re-ranked by final scores")
    summary_statistics: SummaryStatistics = Field(..., description="Statistical summary of final rankings")
