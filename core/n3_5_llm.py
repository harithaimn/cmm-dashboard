# notebook 3- 5- llm generation

from __future__ import annotations

import json
from typing import Dict, Any, List

from openai import OpenAI


# ========================================================
# CONSTANTS / CONTRACT
# ========================================================
LLM_OUTPUT_KEYS = {"reason", "recommendation", "summary"}

SUPPROTED_METRICS = {
    "ctr_link",
    "ctr_all",
    "cpc_link",
    "cpc_all",
    "cpa",
    "cpm",
    "cost_per_1000_reach",
}

# ========================================================
# LLM CLIENT (dependency injection friendly)
# ========================================================
def get_openai_client(api_key: str) -> OpenAI:
    return OpenAI(api_key=api_key)

# ========================================================
# PAYLOAD VALIDATION
# ========================================================
def validate_payload(payload: Dict[str, Any]) -> None:
    """
    Hard validation to avoid garbage-in -> garbage-out.
    """

    if not isinstance(payload, dict):
        raise TypeError("Payload must be a dictionary.")
    
    if "metrics_flagged" not in payload:
        raise ValueError("Payload missing 'metrics_flagged'")
    
    if not isinstance(payload["metrics_flagged"], list):
        raise TypeError("'metrics_flagged' must be a list")
    
    unknown = set(payload["metrics_flagged"]) - SUPPROTED_METRICS
    if unknown:
        raise ValueError(f"Unsupported metrics in payload: {unknown}")
    
# ========================================================
# PROMPT BUILDER
# ========================================================
def build_llm_prompt(payload: Dict[str, Any]) -> list[Dict[str, str]]:
    """
    Build guiderailed prompt for campaign performance explanation and suggestion
    A strict, metric-aware, non-hallucinating prompt.
    """

    system_msg = (
        "You are a senior digital marketing strategist advising a non-technical marketing team.\n\n"

        "You will receive STRUCTURED campaign performance data and diagnostic signals.\n"
        "All inputs are factual. Do not assume missing information.\n\n"

        "Your job is to:\n"
        "1) Explain clearly what is happening\n"
        "2) Explain why it matters for performance, budget, or opportunity\n"
        "3) Recommend specific, concrete actions\n\n"

        "Strict rules:\n"
        "- Do NOT invent numbers, trends, or causes\n"
        "- Use ONLY the provided fields and signals\n"
        "- Numbers are allowed ONLY if they clarify severity, scale, or urgency\n"
        "- Do NOT list or paraphrase metrics for their own sake\n"
        "- Every number mentioned must directly support a recommendation\n"
        "- Do NOT mention models, ML, algorithms, or data science\n"
        "- Avoid technical or platform-specific jargon\n"
        "- If signals are weak, conflicting, or insufficient, say so clearly and recommend no action\n\n"

        "Tone and style:\n"
        "- Plain English\n"
        "- Professional and confident\n"
        "- Short sentences\n"
        "- No hedging language unless evidence is weak\n\n"

        "Output format (MANDATORY):\n"
        "What’s happening:\n"
        "<2–4 short sentences explaining the situation>\n\n"
        "Why it matters:\n"
        "<1–2 sentences explaining impact on results, cost, or opportunity>\n\n"
        "What to do:\n"
        "<1–3 concrete actions written as imperatives>\n\n"
        "Summary:\n"
        "<One short sentence takeaway>"
    )

    user_msg = json.dumps(payload, indent=2)

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]

# ========================================================
# MAIN LLM EXPLANATION FUNCTION
# ========================================================
def generate_llm_explanation(
        client: OpenAI,
        payload: Dict[str, Any],
        model: str = "gpt-4.1-mini",
        temperature: float = 0.3,
) -> Dict[str, str]:
    """
    Generate human-readable explanation using LLM.

    Args:
        client: OpenAI client instance.
        payload: Dictionary containing campaign metrics and signals.
        model: LLM model to use.
        temperature: Sampling temperature for response variability.

    # Returns:
    #     {
    #       "reason": str,
    #       "recommendation": str,
    #       "summary": str
    #     }

    Returns:
        A single formatted string ready for:
        - dashboard cards
        - Slack
        - WhatsApp
        - reports
    """

    validate_payload(payload)

    messages = build_llm_prompt(payload)

    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            #response_format={"type": "json_object"},
        )
    except Exception:
        return (
            "What’s happening:\n"
            "Unable to generate explanation due to a system issue.\n\n"
            "What to do:\n"
            "Review campaign performance manually.\n\n"
            "Summary:\n"
            "LLM explanation unavailable."
        )

    text = response.choices[0].message.content.strip()

    # Very light safety check
    if len(text) < 50:
        return (
            "What’s happening:\n"
            "Performance signals detected, but explanation was unclear.\n\n"
            "What to do:\n"
            "Review campaign performance manually.\n\n"
            "Summary:\n"
            "Manual review recommended."
        )

    return text

    # content = response.choices[0].message.content

    # try:
    #     parsed = json.loads(content)
    # except json.JSONDecodeError:
    #     raise ValueError(f"LLM returned invalid JSON")
    
    # # Hard validation
    # for key in ("reason", "recommendation", "summary"):
    #     if key not in parsed:
    #         raise ValueError(f"LLM response missing key: {key}")
    
    # return {
    #     "reason": parsed["reason"],
    #     "recommendation": parsed["recommendation"],
    #     "summary": parsed["summary"],
    # }


# ========================================================
# OPTIONAL: BATCH HELPER
# ========================================================
def generate_llm_explanations_batch(
        *,
        client: OpenAI,
        rows: List[Dict[str, Any]],
        **kwargs,
) -> List[str]:
    """
    Batch-safe wrapper.
    Returns one readable text block per row
    """

    outputs: List[str] = []

    for payload in rows:
        try:
            explanation = generate_llm_explanation(
                client=client,
                payload=payload,
                **kwargs,
            )
        except Exception:
            explanation = (
                "What's happening:\n"
                "LLM failed to generate explanation.\n\n"
                "What to do:\n"
                "Manual review required.\n\n"
                "Summary:\n"
                "LLM error."
            )
        outputs.append(explanation)
    
    return outputs
