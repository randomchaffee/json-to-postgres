#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, os, sys
from pathlib import Path
import psycopg

# create user query
CREATE_USERS_SQL = """
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
CREATE_GUILDS_SQL = "CREATE TABLE IF NOT EXISTS guilds (guild_id BIGINT PRIMARY KEY, leaderboard_channel BIGINT);"
CREATE_META_SQL = "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);"

def parse_args():
    p = argparse.ArgumentParser(description="Migrate subscriptions.json -> Postgres")
    p.add_argument("--json", required=True, help="Path to subscriptions.json")
    p.add_argument("--database-url", default=os.getenv("DATABASE_URL"), help="Postgres DSN or DATABASE_URL env var")
    p.add_argument("--commit", action="store_true", help="Perform DB writes (default is dry-run)")
    return p.parse_args();

def safe_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None

def create_schema(cur):
    cur.execute(CREATE_USERS_SQL)
    cur.execute(CREATE_GUILDS_SQL)
    cur.execute(CREATE_META_SQL)
    
def upsert_user(cur, discord_id: int, state: dict):
    genshin_uid = safe_int(state.get("uid"))
    hsr_uid = safe_int(state.get("hsr_uid"))
    enabled = bool(state.get("enabled", True))
    notified_full = bool(state.get("notified_full", False))
    ltuid_v2 = state.get("ltuid_v2")
    ltoken_v2 = state.get("ltoken_v2")
    daily_spent = int(state.get("daily_spent", 0) or 0)
    last_resin = safe_int(state.get("last_resin"))
    cur.execute("""
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
            last_resin = EXCLUDED.last_resin,
            updated_at = now();
        """, (discord_id, genshin_uid, hsr_uid, enabled, notified_full, ltuid_v2, ltoken_v2, daily_spent, last_resin))
    
def upsert_guild(cur, guild_id: int, cfg: dict):
    channel = safe_int(cfg.get("leaderboard_channel"))
    cur.execute("INSERT INTO guilds (guild_id, leaderboard_channel) VALUES (%s, %s) ON CONFLICT (guild_id) DO UPDATE SET leaderboard_channel = EXCLUDED.leaderboard_channel;", (guild_id, channel))
    
def upsert_meta(cur, key: str, value: str):
    cur.execute("INSERT INTO meta (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;", (key, str(value)))
    
def main():
    args = parse_args()
    db_url = args.database_url
    if not db_url:
        print("ERROR: DATABASE_URL not provided")
        sys.exit(1)
        
    json_path = Path(args.json)
    if not json_path.exists():
        print("ERROR: JSON file not found:", json_path)
        sys.exit(1)
        
    with open(json_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            create_schema(cur)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT discord_id FROM users")
            existing_users = {r[0] for r in cur.fetchall()}
            inserted = updated = skipped = 0
            for discord_key, state in data.items():
                if discord_key in ("_meta", "_guilds"):
                    continue
                try:
                    discord_id = int(discord_key)
                except Exception:
                    skipped += 1
                    continue
                if discord_id in existing_users:
                    updated += 1
                else:
                    inserted += 1
                    existing_users.add(discord_id)
                upsert_user(cur, discord_id, state or {})
                
            guilds = data.get("_guilds", {}) or {}
            cur.execute("SELECT guild_id FROM guilds")
            existing_guilds = {r[0] for r in cur.fetchall()}
            g_inserted = g_updated = 0
            for gid, cfg in guilds.items():
                try:
                    gid_i = int(gid)
                except Exception:
                    continue
                if gid_i in existing_guilds:
                    g_updated += 1
                else:
                    g_inserted += 1
                    existing_guilds.add(gid_i)
                upsert_guild(cur, gid_i, cfg or {})
            meta = data.get("_meta", {}) or {}
            for k, v in meta.items():
                upsert_meta(cur, k, v)
            print("Summary: users inserted=%d updated=%d skipped=%d; guilds inserted=%d updated=%d" % (inserted, updated, skipped, g_inserted, g_updated))
            if args.commit:
                conn.commit()
                print("Commited.")
            else:
                conn.rollback()
                print("Dry-run (rolled back). Use --commit to write.")

if __name__ == "__main__":
    main()