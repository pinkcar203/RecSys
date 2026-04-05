from pydantic import BaseModel, Field

class RecommendationItem(BaseModel):
    item_id: str
    score: float
    rank: int
    explanation: str | None = None

class RecommendationResponse(BaseModel):
    user_id: str
    items: list[RecommendationItem] = Field(default_factory=list)
    source: str = "score"  # "score" or "llm"
