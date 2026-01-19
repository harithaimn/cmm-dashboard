# notebook 3- 5- llm generation

from __future__ import annotations

import json
from typing import Dict, Any

from openai import OpenAI

# ========================================================
# LLM CLIENT (dependency injection friendly)
# ========================================================
def get_openai_client(api_key: str) -> OpenAI:
    return OpenAI(api_key=api_key)


# ========================================================
# PROMPT BUILDER
# ========================================================
def build_llm_prompt(payload: Dict[str, Any]) -> list[Dict[str, str]]:
    """
    Build guiderailed prompt for campaign performance explanation and suggestion
    """

    system_msg = (
        "You are a senior paid media strategist.\n"
        "Given the campaign performance data, provide a concise explanation "
        "and actionable suggestions for improvement."
        "You are given campaign metrics and boolean signals.\n"
        "You must explain WHAT is happening and WHAT should be done.\n\n"
        "Rules:\n"
        "- Do NOT invent numbers\n"
        "- Do NOT restate raw metrics unnecessarily\n"
        "- Base reasoning ONLY on provided inputs\n"
        "- Be concise and professional\n"
        "- Output valid JSON only\n\n"
        "Output JSON schema:\n"
        "{\n}"
        '  "reason": string,\n'
        '  "recommendation": string,\n'
        '  "summary": string\n'
        "}"
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

    Returns:
        {
          "reason": str,
          "recommendation": str,
          "summary": str
        }
    """

    messages = build_llm_prompt(payload)

    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
        response_format={"type": "json_object"},
    )

    content = response.choices[0].message.content

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        raise ValueError(f"LLM returned invalid JSON")
    
    # Hard validation
    for key in ("reason", "recommendation", "summary"):
        if key not in parsed:
            raise ValueError(f"LLM response missing key: {key}")
    
    return {
        "reason": parsed["reason"],
        "recommendation": parsed["recommendation"],
        "summary": parsed["summary"],
    }