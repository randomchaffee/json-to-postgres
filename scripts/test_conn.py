import os, psycopg
from dotenv import load_dotenv
load_dotenv()

db = os.getenv("DATABASE_URL")

if not db:
    raise ValueError("DATABASE_URL environment variable is not set")

print("DB:", db)
with psycopg.connect(db) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        result = cur.fetchone()
        if result:
            print(result[0])