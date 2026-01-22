# # notebook 2- 2-meta ingestion

from __future__ import annotations

import requests
import pandas as pd
from typing import Dict, List, Any, Optional
from tqdm import tqdm

# ----------------------------------
# Internal: Defensive Meta API request layer (retry, no sleep)
# ----------------------------------
def _fb_get(url: str, retries: int = 5) -> Dict[str, Any]:
    """
    Robust GET wrapper for Meta Graph API.
    Handles:
    - silent empty responses
    - HTML errors
    - non-JSON payloads
    - retry with backoff
    - no sleep
    """
    for _ in range(retries):
        try:
            r = requests.get(url, timeout=30)

            if not r.text:
                continue

            if not r.headers.get("Content-Type", "").startswith("application/json"):
                continue

            return r.json()

        except Exception:
            continue
    
    return {}

# =======================================================
# MARK: 5- Normalize Insight Structure (exact result type + value)
# =======================================================

# -----------------------------
# 4. Result parsing helpers
# -----------------------------
def _parse_result(block: Any) -> tuple[Optional[str], Optional[float]]:
    """
    Extract result type + value from Meta result blocks.
    
    :param block: Description
    :type block: Any
    :return: Description
    :rtype: tuple[str | None, float | None]
    """

    if not isinstance(block, list) or not block:
        return None, None
    
    item = block[0]
    rtype = item.get("indicator")
    values = item.get("values", [])

    rval = values[0].get("value") if values else None
    return rtype, rval

# ====================================================
# MARK:8.1- Normalize ad_result_type & campaign_result_type
# ====================================================

# ====================================================
# MARK: 8.1.1- Define CATEGORY_MAP + normalized() + categorize() function.
# The logic layer.
# ==================================================== 

# ---------------------------------
# Result type normalization (Meta semantic truth)
# ---------------------------------
CATEGORY_MAP = {
    # Engagement
    "post_engagement": "Engagement",
    "like": "Engagement",
    "profile_visit": "Engagement",
    "profile_visits": "Engagement",
    "page_visit": "Engagement",
    "video_thruplay": "Engagement",

    # Awareness
    "reach": "Awareness",
    "ad_recall_lift": "Awareness",

    # Traffic
    "link_click": "Traffic",
    "landing_page_view": "Traffic",

    # Messaging
    "messaging_conversation_started": "Messaging",

    # Leads
    "lead": "Leads",
    "conversion_lead": "Leads",
    "submit_application": "Leads",

    # Conversion
    "view_content": "Conversion",
    "initiate_checkout": "Conversion",
    "purchase": "Conversion",
    "onsite_purchase": "Conversion",
    "complete_registration": "Conversion",
    "customize_product": "Conversion",

    # Custom
    "custom_event": "Custom Event",

    # Unknown
    "mixed": "Unknown",
    "unknown": "Unknown",
}

def normalize_result_type(x: Optional[str]) -> str:
    if not x:
        return "unknown"
    
    low = str(x).lower()

    if "post_engagement" in low:
        return "post_engagement"
    if "actions:like" in low:
        return "like"
    if "profile_visit" in low:
        return "profile_visit"
    if "page_visit" in low:
        return "page_visit"
    if "video_thruplay" in low:
        return "video_thruplay"
    
    if "reach" in low:
        return "reach"
    if "ad_recall" in low:
        return "ad_recall_lift"
    
    if "link_click" in low:
        return "link_click"
    if "landing_page_view" in low:
        return "landing_page_view"
    
    if "messaging_conversation_started" in low:
        return "messaging_conversation_started"
    
    if "fb_pixel_lead" in low:
        return "lead"
    if "conversion_lead" in low:
        return "conversion_lead"
    if "submit_application" in low:
        return "submit_application"
    
    if "purchase" in low:
        return "purchase"
    if "initiate_checkout" in low:
        return "initiate_checkout"
    if "view_content" in low:
        return "view_content"
    
    if "custom" in low:
        return "custom_event"
    
    return "unknown"

def categorize_result_type(normalized: str) -> str:
    return CATEGORY_MAP.get(normalized, "Unknown")



# ------------------------------------------
# Meta ingestion - Daily Ad-level facts
#  // 6. Fact table builder
# ------------------------------------------
def fetch_meta_daily_fact_table(
        access_token: str,
        ad_account_id: str,
        date_since: str,
        date_until: str,
) -> pd.DataFrame:
    """
    Fetch DAILY ad-level semantic facts from Meta.

    Output grain:
        date x ad_id

    Contains:
        - campaigns / adset / ad identity
        - result_type (normalized + category)
        - result_value
        - cost_per_result
        - status + creative metadata
        - campaign date_start / date_stop
    """

    # -------------------------------------
    # 1. Fetch campaigns (semantic truth)
    # // 2. Hierarchy fetchers
    # -------------------------------------
    campaigns: List[Dict[str, Any]] = []
    url = (
        f"https://graph.facebook.com/v24.0/{ad_account_id}/campaigns"
        f"?fields=id,name,objective,effective_status,start_time,stop_time"
        f"daily_budget,lifetime_budget,budget_remaining,"
        f"insights.time_range({{'since':'{date_since}','until':'{date_until}'}})"
        #f"&effective_status=['ACTIVE','PAUSED','ARCHIVED']"
        f"{{date_start,date_stop,impressions,reach,spend,results,cost_per_result}}"
        f"&limit=200"
        f"&access_token={access_token}"
    )

    while url:
        data = _fb_get(url)
        campaigns.extend(data.get("data", []))
        url = data.get("paging", {}).get("next")
    
    campaign_map = {c["id"]: c for c in campaigns}

    # ======================================
    ## MARK: 2- Get All Adsets per Campaign 
    # ======================================
    
    # ----------------------------------
    # 2. Fetch ads (adset? identity?)
    # ----------------------------------

    adsets: List[Dict[str, Any]] = []

    #def get_adsets(access_token: str, campaign_id: str) -> List[Dict[str, Any]]:
    #    adsets = []

    #for c in campaigns:
    for c in tqdm(campaigns, desc="Campaigns", unit="campaign"):    
        url = (
            f"https://graph.facebook.com/v24.0/{c['id']}/adsets"
            f"?fields=id,name,status,effective_status,daily_budget,lifetime_budget,"
            f"optimization_goal,billing_event,start_time,end_time,"
            f"insights.time_range({{'since':'{date_since}','until':'{date_until}'}})"
            f"{{date_start,date_stop,impressions,reach,spend,results,cost_per_result}}"
            f"&limit=200"
            f"&access_token={access_token}"
        )

        while url:
            data = _fb_get(url)
            #adsets.extend(data.get("data", []))
            for a in data.get("data", []):
                a["campaign_id"] = c["id"]
                adsets.append(a)
            url = data.get("paging", {}).get("next")

    adset_map = {a["id"]: a for a in adsets}

    # -----------------------------
    # 3. Fetch Daily Ads , insights per ad?
    # ------------------------------

    # ======================================
    ## MARK: 3- Get All Ads per Adset
    # ======================================
    ads: List[Dict[str, Any]] = []

    #for a in adsets:
    for a in tqdm(adsets, desc="Adsets", unit="adset"):
        url = (
            f"https://graph.facebook.com/v24.0/{a['id']}/ads"
            f"?fields=id,name,status,effective_status,creative{{title}},"
            f"insights.time_range({{'since':'{date_since}','until':'{date_until}'}})"
            f"&time_increment=1"
            f"{{date_start,date_stop,impressions,reach,spend,results,cost_per_result}}"
            f"&limit=200"
            f"&access_token={access_token}"
        )

        while url:
            data = _fb_get(url)
            for ad in data.get("data", []):
                ad["adset_id"] = a["id"]
                ad["campaign_id"] = a["campaign_id"]
                ads.append(ad)
            url = data.get("paging", {}).get("next")

    # ==============================================================
    # MARK: 4- Get Insights (Data) for Each Object (Campaign, Adset, Ad)
    # ==============================================================


    
    # ==============================================================
    # MARK: 6- Build Final DataFrame (Campaign -> adset -> ad)
    # ==============================================================

    # ------------------------------------
    # 4. Build DAILY rows (aligned by date)
    # ------------------------------------
    rows: List[Dict[str, Any]] = []

    #for ad in ads:
    for ad in tqdm(ads, desc="Ads", unit="ad"):
        ad_insights = ad.get("insights", {}).get("data", [])

        aset = adset_map.get(ad["adset_id"])
        camp = campaign_map.get(ad["campaign_id"])

        aset_ins_map = {
            i["date_start"]: i for i in aset.get("insights", {}).get("data", [])
        } if aset else {}

        camp_ins_map = {
            i["date_start"]: i for i in camp.get("insights", {}).get("data", [])
        } if camp else {}

        for ad_ins in ad_insights:
            d = ad_ins.get("date_start")

            c_ins = camp_ins_map.get(d, {})
            a_ins = aset_ins_map.get(d, {})

            c_type, c_val = _parse_result(c_ins.get("results"))
            c_cpr_type, c_cpr_val = _parse_result(c_ins.get("cost_per_result"))

            as_type, as_val = _parse_result(a_ins.get("results"))
            as_cpr_type, as_cpr_val = _parse_result(a_ins.get("cost_per_result"))

            ad_type, ad_val = _parse_result(ad_ins.get("results"))
            ad_cpr_type, ad_cpr_val = _parse_result(ad_ins.get("cost_per_result"))

            rows.append({
                "date": ad_ins.get("date_stop"),

                # Campaign
                "campaign_id": camp.get("id"),
                "campaign_name": camp.get("name"),
                "campaign_objective": camp.get("objective"),
                "campaign_effective_status": camp.get("effective_status"),
                "campaign_date_start": camp.get("start_time"),
                "campaign_date_stop": camp.get("stop_time"),
                "campaign_result_type": c_type,
                "campaign_result_value": c_val,
                "campaign_cpr_type": c_cpr_type,
                "campaign_cpr_value": c_cpr_val,

                # Adset
                "adset_id": aset.get("id"),
                "adset_name": aset.get("name"),
                "adset_effective_status": aset.get("effective_status"),
                "adset_result_type": as_type,
                "adset_result_value": as_val,
                "adset_cpr_type": as_cpr_type,
                "adset_cpr_value": as_cpr_val,

                # Ad
                "ad_id": ad.get("id"),
                "ad_name": ad.get("name"),
                "ad_status": ad.get("status"),
                "creative_title": ad.get("creative", {}).get("title"),
                "ad_result_type": ad_type,
                "ad_result_value": ad_val,
                "ad_cpr_type": ad_cpr_type,
                "ad_cpr_value": ad_cpr_val,
            })

    df = pd.DataFrame(rows)

    # ------------------------------
    # 5. Normalize result types
    # ------------------------------
    for col in ["campaign_result_type", "adset_result_type", "ad_result_type"]:
        df[f"normalized_{col}"] = df[col].apply(normalize_result_type)
        df[f"{col}_category"] = df[f"normalized_{col}"].apply(categorize_result_type)

    # ------------------------------
    # 6. Type coercion (minimal)
    # ------------------------------
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["campaign_date_start"] = pd.to_datetime(df["campaign_date_start"], errors="coerce")
    df["campaign_date_stop"] = pd.to_datetime(df["campaign_date_stop"], errors="coerce")

    return df