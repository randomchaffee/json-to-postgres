"""
Synchronous DB adapter exposing `load_subscriptions()` and `save_subscriptions(data)`.
This mirrors the shape of the file-based storage.
"""
import os
from typing import Dict, Any, Set
import psycopg

def _get_db_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return db_url

def _ensure_schema(cur) -> None:
    cur.execute(
    """
    CREATE TABLE IF NOT EXISTS users (
        discord_id BIGINT PRIMARY KEY,
        genshin_uid BIGINT,
        hsr_uid BIGINT,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        notified_full BOOLEAN NOT NULL DEFAULT FALSE,
        ltuid_v2 TEXT,
        ltoken_v2 TEXT,
        daily_spent INTEGER NOT NULL DEFAULT 0,
        last_resin INTEGER,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS guilds (guild_id BIGINT PRIMARY KEY, leaderboard_channel BIGINT);"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);"
    )
    
def _safe_int(value: Any) -> Any:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None
    
def load_subscriptions() -> Dict[str, Any]:
    db_url = _get_db_url()
    data: Dict[str, Any] = {"_meta": {}, "_guilds": {}}
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            _ensure_schema(cur)

            # users
            cur.execute(
                "SELECT discord_id, genshin_uid, hsr_uid, enabled, notified_full, ltuid_v2, ltoken_v2, daily_spent, last_resin FROM users"
            )
            for r in cur.fetchall():
                discord_id = str(r[0])
                state: Dict[str, Any] = {}
                if r[1] is not None:
                    state["uid"] = str(r[1])
                if r[2] is not None:
                    state["hsr_uid"] = str(r[2])
                state["enabled"] = bool(r[3])
                state["notified_full"] = bool(r[4])
                if r[5] is not None:
                    state["ltuid_v2"] = str(r[5])
                if r[6] is not None:
                    state["ltoken_v2"] = str(r[6])
                state["daily_spent"] = int(r[7] or 0)
                if r[8] is not None:
                    state["last_resin"] = int(r[8])
                data[discord_id] = state
                
            # guilds
            cur.execute("SELECT guild_id, leaderboard_channel FROM guilds")
            for gid, ch in cur.fetchall():
                data["_guilds"][str(gid)] = {
                    "leaderboard_channel": str(ch) if ch is not None else None
                }
            
            # meta
            cur.execute("SELECT key, value FROM meta")
            for k, v in cur.fetchall():
                data["_meta"][k] = v
            
    return data
    
def save_subscriptions(data: Dict[str, Any]) -> None:
    db_url = _get_db_url()
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            
            # existing users for delete detection
            cur.execute("SELECT discord_id FROM users")
            existing_user_ids: Set[int] = {r[0] for r in cur.fetchall()}
            
            incoming_user_ids: Set[int] = set()
            for key, state in data.items():
                if key in ("_meta", "_guilds"):
                    continue
                try:
                    did = int(key)
                except Exception:
                    continue
                
                incoming_user_ids.add(did)
                
                cur.execute(
                    """
                    INSERT INTO users (discord_id, genshin_uid, hsr_uid, enabled, notified_full, ltuid_v2, ltoken_v2, daily_spent, last_resin)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (discord_id) DO UPDATE SET
                        genshin_uid = EXCLUDED.genshin_uid,
                        hsr_uid = EXCLUDED.hsr_uid,
                        enabled = EXCLUDED.enabled,
                        notified_full = EXCLUDED.notified_full,
                        ltuid_v2 = EXCLUDED.ltuid_v2,
                        ltoken_v2 = EXCLUDED.ltoken_v2,
                        daily_spent = EXCLUDED.daily_spent,
                        last_resin = EXLUDED.last_resin,
                        updated_at = now()
                    """,
                    (
                        did,
                        _safe_int(state.get("uid")) if (state and state.get("uid")) else None,
                        _safe_int(state.get("hsr_uid")) if (state and state.get("hsr_uid")) else None,
                        bool(state.get("enabled", True)) if state is not None else True,
                        bool(state.get("notified_full", False)) if state is not None else False,
                        state.get("ltuid_v2") if state is not None else None,
                        state.get("ltoken_v2") if state is not None else None,
                        int(state.get("daily_spent", 0) or 0) if state is not None else 0,
                        _safe_int(state.get("last_resin")) if state is not None else None,
                    ),
                )
            
            # delete users missing from incoming data
            # this mirrors file-based save semantics
            to_delete = existing_user_ids - incoming_user_ids
            if to_delete:
                cur.execute("DELETE FROM users WHERE discord_id = ANY(%s)", (list(to_delete),))
                
            # guilds: upsert incoming, delete missing
            cur.execute("SELECT guild_id FROM guilds")
            existing_guilds = {r[0] for r in cur.fetchall()}
            incoming_guilds = set()
            for gid, cfg in (data.get("_guilds") or {}).items():
                try:
                    gid_i = int(gid)
                except Exception:
                    continue
                
                incoming_guilds.add(gid_i)
                cur.execute(
                    "INSERT INTO guilds (guild_id, leaderboard_channel) VALUES (%s,%s) ON CONFLICT (guild_id) DO UPDATE SET leaderboard_channel = EXCLUDED.leaderboard_channel;",
                    (gid_i, _safe_int(cfg.get("leaderboard_channel")) if cfg else None),
                )
            to_delete_g = existing_guilds - incoming_guilds
            if to_delete_g:
                cur.execute("DELETE FROM guilds WHERE guild_id = ANY(%s)", (list(to_delete_g),))
                
            # meta
            # replace values
            # also delete missing
            cur.execute("SELECT key FROM meta")
            existing_meta = {r[0] for r in cur.fetchall()}
            incoming_meta = set()
            for k, v in (data.get("_meta") or {}).items():
                incoming_meta.add(k)
                cur.execute(
                    "INSERT INTO meta (key, value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.values;",
                    (k, str(v)),
                )
            to_delete_m = existing_meta - incoming_meta
            if to_delete_m:
                cur.execute("DELETE FROM meta WHERE key = ANY(%s)", (list(to_delete_m),))
            
        conn.commit()