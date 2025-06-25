from pydantic import BaseModel, Field
from typing import List, Dict, Tuple, Optional, Any
import pandas as pd

# --- Individual Component Response Models ---

class ReviewRhythmData(BaseModel):
    """
    Represents the 7x24 heatmap data for review rhythm.
    Keys are DayOfWeek (1-7), values are dicts of HourOfDay (0-23) to reviewCount.
    """
    data: Dict[int, Dict[int, int]] = Field(..., description="Heatmap data: {DayOfWeek: {HourOfDay: Count}}")

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame):
        # Convert DataFrame to the nested dictionary structure
        # Ensure index and columns are named as expected by c1_review_rhythm
        # df.index.name = 'DayOfWeek'
        # df.columns.name = 'HourOfDay'
        return cls(data=df.to_dict(orient='index'))

class CuisineDiversityData(BaseModel):
    category_counts: Dict[str, int] = Field(..., description="Counter object with category names as keys and review counts as values.")
    diversity_score: float = Field(..., description="Calculated Shannon entropy (diversity score).")

class SentimentTimelineData(BaseModel):
    timeline: Dict[int, float] = Field(..., description="Dictionary where keys are years (int) and values are the average mood score (float).")

class WordSignatureTerm(BaseModel):
    term: str
    score: float

class WordSignatureData(BaseModel):
    signature: List[WordSignatureTerm] = Field(..., description="List of top TF-IDF terms/bigrams and their scores.")

class HiddenGemBusiness(BaseModel):
    business_id: str
    business_name: str
    user_review_date: str # Assuming date is a string, adjust if it's datetime
    reviews_at_time: int
    current_review_count: int
    # percent_increase: Optional[float] = None # This was in the query but not in the example output dict

class HiddenGemsData(BaseModel):
    gems: List[HiddenGemBusiness] = Field(..., description="List of hidden gem businesses.")

class TasteClusterCategory(BaseModel):
    category: str
    count: int

class TasteClusterData(BaseModel):
    cluster_id: Optional[Any] = Field(None, description="The integer ID of the cluster. Can be None if not found.") # Changed to Any to match component
    top_categories: List[TasteClusterCategory] = Field(default_factory=list, description="List of top non-generic categories in the cluster.")

class RecommendedBusiness(BaseModel):
    business_id: str
    name: str
    avgStar: Optional[float] = Field(None, alias="avgStar", description="Average star rating of the business.")
    categories: List[str] = Field(default_factory=list)

class RecommendationsData(BaseModel):
    recommendations: List[RecommendedBusiness] = Field(..., description="List of recommended businesses.")

class InfluencePercentileData(BaseModel):
    overall_influence_percentile: Optional[float] = Field(None, description="User's overall influence percentile (0-100). Can be None.")

# --- Composite Model for Full User Profile ---

class UserProfileData(BaseModel):
    user_id: str
    review_rhythm: Optional[ReviewRhythmData] = None
    cuisine_diversity: Optional[CuisineDiversityData] = None
    sentiment_timeline: Optional[SentimentTimelineData] = None
    word_signature: Optional[WordSignatureData] = None
    hidden_gems: Optional[HiddenGemsData] = None
    taste_cluster: Optional[TasteClusterData] = None
    recommendations: Optional[RecommendationsData] = None
    influence_percentile: Optional[InfluencePercentileData] = None
    
    # Field to capture any errors encountered during processing for a component
    errors: Dict[str, str] = Field(default_factory=dict, description="Dictionary of errors encountered for specific components.")
