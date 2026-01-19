# notebook 1- 1-cleaning.

from __future__ import annotations

import re
from datetime import date
from typing import Optional

import pandas as pd


# ----------------------------------
# Internal: canonical objective matcher (SSOT)
# ----------------------------------
def _match_objective_token(token: str) -> Optional[str]:
    """
    Match a single token against the campaign objective taxonomy.
    Returns canonical objective name or None if no match.
    """
    low = token.lower().strip()

    # WhatsApp / Messaging
    if low in {
        "wa", "wa messaging", "whatsapp", "whatsapp campaign",
        "messaging", "message"
    }:
        return "WhatsApp"

    # Traffic
    if low in {"traf", "traffic", "trafic", "traff"}:
        return "Traffic"

    # Engagement
    if low in {
        "eng", "engagement", "page engagement", "post engagement",
        "post engagement [visual]", "post engagement [video]",
        "post engagement m10", "post engagement m11", "post engagement m12",
        "eng maple market", "eng 11.11", "m2 others"
    }:
        return "Engagement"

    # Leads
    if low in {
        "lg", "leads", "lead gen", "lead gen m52-m55",
        "leadgen", "lead", "always on"
    }:
        return "Leads"

    # Brand Awareness
    if low in {"brand awareness", "awareness", "ba"}:
        return "Awareness"

    # Conversion
    if low in {"conversion", "conversions"}:
        return "Conversion"

    # Link Click
    if low in {"link click", "link clicks"}:
        return "Link Click"

    # Video Views
    if "video views" in low:
        return "Video Views"

    # Month tags (M1, M2, M10, etc.) → preserve original
    if re.fullmatch(r"m\d+.*", low):
        return token.strip()

    return None

# ----------------------------------
# 1. Campaign name cleaning
# ----------------------------------
def clean_campaign_name(x: Optional[str]) -> Optional[str]:
    """
    Normalize campaign name text:
    - strip leading/trailing whitespace
    - collapse multiple spaces

    Does NOT:
    - lowercase
    - split
    - infer meaning
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None

    x = str(x).strip()
    x = re.sub(r"\s+", " ", x)
    
    return x

# ----------------------------------
# 2. Objective extraction 
# ----------------------------------

def extract_objective(campaign_name: Optional[str]) -> Optional[str]:
    """
    Extract campaign objective assuming pipe
    e.g: 'Brand | Traffic | M10' -> 'Traffic'
    """
    if campaign_name is None or (isinstance(campaign_name, float) and pd.isna(campaign_name)):
        return None
    
    parts = [p.strip() for p in str(campaign_name).split("|")]
    
    if len(parts) < 2:
        return None
    
    return parts[1]

# -----------------------------------
# 3. Objective normalization
# -----------------------------------
def normalize_objective(obj: Optional[str]) -> Optional[str]:
    """
    Normalize messy campaign objectives into a canonical taxonomy.
    """
    if obj is None or (isinstance(obj, float) and pd.isna(obj)):
        return None
    
    token = obj.strip()
    matched = _match_objective_token(token)

    # fallback: title case original token
    return matched if matched else token.title()

# -------------------------------
# 4. Objective extraction
# -------------------------------
def extract_objective_dynamic(campaign_name: Optional[str]) -> Optional[str]:
    """
    Scan all segments of a campaign name & infer objective
    using same normalization rules.
    """
    if campaign_name is None or (isinstance(campaign_name, float) and pd.isna(campaign_name)):
        return None
    
    parts = [p.strip() for p in str(campaign_name).split("|")]

    for p in parts:
        matched = _match_objective_token(p)
        if matched:
            return matched
    
    return None

# -----------------------------------------------
# 5. Campaign activity status (semantic override)
# -----------------------------------------------
def derive_campaign_activity_status(
        effective_status: Optional[str],
        date_start: Optional[pd.Timestamp],
        date_stop: Optional[pd.Timestamp],
        today: Optional[date] = None,
) -> str:
    """

    Derive TRUE campaign activity status.

    ACTIVE if:
        - effective_status == 'ACTIVE'
        - date_start <= today
        - AND (date_stop is null or date_stop >= today)

    Otherwise -> PASSIVE

    This overrides Meta / Supermetrics raw status.    
    """
    if today is None:
        today = date.today()

    status = str(effective_status).upper() if effective_status else ""

    # Missing or invalid start date -> not active
    if date_start is None or pd.isna(date_start):
        return "PASSIVE"
    
    start_date = pd.to_datetime(date_start).date()

    stop_date = None
    if date_stop is not None and not pd.isna(date_stop):
        stop_date = pd.to_datetime(date_stop).date()

    is_active = (
        status == "ACTIVE"
        and start_date <= today
        and (stop_date is None or stop_date >= today)
    )

    return "ACTIVE" if is_active else "PASSIVE"