from typing import Any, Dict, Optional

def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None

def json_user_to_row(discord_id: str, state: Dict[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    row["discord_id"] = int(discord_id)
    row["genshin_uid"] = safe_int(state.get("uid"))
    row["hsr_uid"] = safe_int(state.get("hsr_uid"))
    row["enabled"] = bool(state.get("enabled", True))
    row["notified_full"] = bool(state.get("notified_full", False))
    row["ltuid_v2"] = state.get("ltuid_v2")
    row["ltoken_v2"] = state.get("ltoken_v2")
    row["daily_spent"] = int(state.get("daily_spent", 0) or 0)
    row["last_resin"] = safe_int(state.get("last_resin"))
    return row

def row_to_json(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if row.get("genshin_uid") is not None:
        out["uid"] = str(row["genshin_uid"])
    if row.get("hsr_uid") is not None:
        out["hsr_uid"] = str(row["hsr_uid"])
    out["enabled"] = bool(row.get("enabled", True))
    out["notified_full"] = bool(row.get("notified_full", False))
    if row.get("ltuid_v2") is not None:
        out["ltuid_v2"] = row.get("ltuid_v2")
    if row.get("ltoken_v2") is not None:
        out["ltoken_v2"] = row.get("ltoken_v2")
    out["daily_spent"] = int(row.get("daily_spent", 0) or 0)
    if row.get("last_resin") is not None:
        out["last_resin"] = safe_int(row.get("last_resin"))
    return out