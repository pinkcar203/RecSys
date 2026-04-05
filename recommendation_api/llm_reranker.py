from __future__ import annotations

import asyncio
import json
import os
from typing import List

import structlog
from groq import AsyncGroq

from common.models.recommendations import RecommendationItem

log = structlog.get_logger()

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
LLM_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "3.0"))
MODEL = "llama-3.3-70b-versatile"


def _build_prompt(candidates: List[RecommendationItem], recent_events: List[dict]) -> str:
    # Build the prompt for LLM re-ranking
    candidates_str = json.dumps(
        [{"item_id": c.item_id, "score": c.score, "rank": c.rank} for c in candidates],
        indent=2,
    )
    events_str = json.dumps(recent_events[:10], indent=2) if recent_events else "[]"

    return f"""You are a recommendation engine. Given a user's recent behavioral events and a list of candidate items ranked by engagement score, re-rank the items based on semantic relevance and provide a brief explanation for each recommendation.

## User's Recent Events
{events_str}

## Candidate Items (current ranking by score)
{candidates_str}

## Instructions
- Re-rank the candidates based on the user's behavior patterns
- Provide a short explaination (1 sentence) for each item explaining why it's recommended
- Return ONLY valid JSON, no markdown, no extra text

## Required Output Format
Return a JSON array:
[
  {{"item_id": "...", "rank": 1, "explanation": "Recommended because..."}},
  {{"item_id": "...", "rank": 2, "explanation": "Recommended because..."}}
]"""


async def _call_groq(prompt: str) -> list[dict]:
    # Call Groq API and parse JSON response
    client = AsyncGroq(api_key=GROQ_API_KEY)
    response = await client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": "You are a JSON-only recommendation engine. Return only valid JSON arrays."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.3,
        max_tokens=1024,
    )
    content = response.choices[0].message.content.strip()
    # Strip markdown code fences if present
    if content.startswith("```"):
        content = content.split("\n", 1)[1]
        if content.endswith("```"):
            content = content[:-3].strip()
    return json.loads(content)


async def rerank(
    candidates: List[RecommendationItem],
    recent_events: List[dict] | None = None,
) -> List[RecommendationItem]:
    """Re-rank candidates using Groq LLM with timeout and fallback.

    Returns re-ranked items with explanations on success.
    Returns original candidates unchanged on timeout or error.
    """
    if not GROQ_API_KEY or not candidates:
        return candidates

    recent_events = recent_events or []
    prompt = _build_prompt(candidates, recent_events)

    try:
        llm_results = await asyncio.wait_for(_call_groq(prompt), timeout=LLM_TIMEOUT)
    except asyncio.TimeoutError:
        log.warning("llm_rerank_timeout", timeout=LLM_TIMEOUT)
        return candidates
    except Exception as exc:
        log.warning("llm_rerank_failed", error=str(exc))
        return candidates

    # Build a lookup from original candidates for scores
    score_map = {c.item_id: c.score for c in candidates}

    # Map LLM results back to RecommendationItems
    reranked = []
    for item in llm_results:
        item_id = item.get("item_id", "")
        if item_id in score_map:
            reranked.append(
                RecommendationItem(
                    item_id=item_id,
                    score=score_map[item_id],
                    rank=item.get("rank", len(reranked) + 1),
                    explanation=item.get("explanation"),
                )
            )

    # If LLM dropped some items, append them at the end
    reranked_ids = {r.item_id for r in reranked}
    for c in candidates:
        if c.item_id not in reranked_ids:
            reranked.append(
                RecommendationItem(
                    item_id=c.item_id,
                    score=c.score,
                    rank=len(reranked) + 1,
                    explanation=None,
                )
            )

    log.info("llm_rerank_success", original_count=len(candidates), reranked_count=len(reranked))
    return reranked
