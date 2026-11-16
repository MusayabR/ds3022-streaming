"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> marts -> one chart.

  python flow.py               # full data in data/raw
  python flow.py data/sample   # the committed sample (no Kafka needed)

Adapted from quakes_flow.py (Mon 02 Nov); settings, stage() and entities()
follow the Session 3 worksheet (Appendix A1 to A4, Coinbase).
"""
import os
import sys

import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\"))          # "raw" or "sample"
TAG = "" if NAME == "raw" else "_" + NAME
DB = f"coinbase{TAG}.duckdb"                         # coinbase_sample.duckdb for the sample
CHART = f"charts/bursts_by_coin{TAG}.png"            # the sample never overwrites the real chart
BURST_X = 2  # a burst: more than 2x the product's average over its previous 10 minutes


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of the database
        con.execute(f"CREATE OR REPLACE TABLE raw_trades AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_trades").fetchone()[0]


@task
def stage():
    """Kind 1: the same Kafka record landed twice. DISTINCT keeps one copy."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_trades
            WHERE action <> 'malformed'
              AND msg_type = 'match'               -- not subscriptions or last_match
              AND product_id IS NOT NULL AND trade_id IS NOT NULL
        """)
        q = lambda sql: con.execute(sql).fetchone()[0]
        return {"malformed": q("SELECT count(*) FROM raw_trades WHERE action = 'malformed'"),
                "records": q("SELECT count(*) FROM stg_records")}


@task
def entities():
    """Kind 2: the same trade sent twice. One row per (product_id, trade_id)."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE trades AS
            SELECT product_id, trade_id, side, price, size,
                   CAST(event_time AS TIMESTAMP) AS event_time,                 -- event time, UTC
                   CAST(raw::JSON ->> '$.data.sequence' AS BIGINT) AS sequence  -- parsed from raw
            FROM stg_records
            QUALIFY row_number() OVER (PARTITION BY product_id, trade_id
                                       ORDER BY kafka_offset DESC) = 1
        """)
        return con.execute("SELECT count(*) FROM trades").fetchone()[0]


@task
def marts():
    """Anomaly: a 1-minute window is a burst when its trade count is more than BURST_X times
    the average of that coin's previous 10 windows. Then per coin: how many bursts came
    alone, and how many in the same minute as another coin's burst?"""
    with duckdb.connect(DB) as con:
        con.execute(f"""
            CREATE OR REPLACE TABLE mart_minute_bursts AS
            WITH per_minute AS (
                SELECT product_id, time_bucket(INTERVAL '1 minute', event_time) AS minute,
                       count(*) AS trades
                FROM trades
                GROUP BY product_id, minute
            ), scored AS (
                SELECT *, avg(trades) OVER w AS usual, count(*) OVER w AS history
                FROM per_minute
                WINDOW w AS (PARTITION BY product_id ORDER BY minute
                             ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING)
            )
            SELECT product_id, minute, trades, round(usual, 1) AS usual_trades,
                   (history = 10 AND trades > {BURST_X} * usual) AS burst   -- needs 10 earlier windows
            FROM scored
            ORDER BY minute, product_id
        """)
        con.execute("""
            CREATE OR REPLACE TABLE mart_bursts_by_coin AS
            WITH b AS (SELECT product_id, minute FROM mart_minute_bursts WHERE burst),
                 m AS (SELECT minute, count(*) AS coins FROM b GROUP BY minute)
            SELECT p.product_id,
                   count(b.minute) FILTER (WHERE m.coins = 1) AS alone,
                   count(b.minute) FILTER (WHERE m.coins > 1) AS together
            FROM (SELECT DISTINCT product_id FROM mart_minute_bursts) p
            LEFT JOIN b USING (product_id)
            LEFT JOIN m USING (minute)
            GROUP BY p.product_id
            ORDER BY alone + together DESC, p.product_id
        """)
        return con.execute("SELECT count(*) FROM mart_minute_bursts WHERE burst").fetchone()[0]


@task
def chart():
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute("SELECT product_id, alone, together FROM mart_bursts_by_coin").fetchall()
        start, end = con.execute("SELECT min(minute), max(minute) FROM mart_minute_bursts").fetchone()
    coins = [r[0] for r in rows]
    alone = [r[1] for r in rows]
    together = [r[2] for r in rows]
    total = sum(alone) + sum(together)
    pct_together = 100.0 * sum(together) / total if total else 0.0
    btc = next((a + t for c, a, t in rows if c == "BTC-USD"), 0)
    top_coin, top_n = (coins[0], alone[0] + together[0]) if rows else ("none", 0)

    os.makedirs("charts", exist_ok=True)
    plt.figure(figsize=(8, 4.5))
    plt.bar(coins, alone, color="#2a7fb8", label="burst alone")
    plt.bar(coins, together, bottom=alone, color="#d9534f", label="same minute as another coin's burst")
    plt.title(f"{top_coin} bursts most ({top_n} minutes), BTC-USD the busiest coin only {btc}; "
              f"{pct_together:.0f}% of bursts are shared\n"
              f"(burst = 1-minute trade count over {BURST_X}x that coin's previous 10-minute average)",
              fontsize=10)
    plt.ylabel("burst minutes (count)")
    span = f"{start:%d %b %H:%M}-{end:%H:%M} UTC" if start else "no data"
    plt.xlabel(f"coin (Coinbase, event time {span})")
    plt.legend()
    plt.tight_layout()
    plt.savefig(CHART, dpi=120)
    plt.close()
    return CHART


@flow(name="coinbase-pipeline")
def pipeline():
    log = get_run_logger()
    raw = load_raw()
    n = stage()
    things = entities()
    bursts = marts()
    path = chart()
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", n["malformed"])
    log.info("Unique Kafka records:      %d", n["records"])
    log.info("Unique trades:             %d", things)
    log.info("Burst minutes (all coins): %d", bursts)
    log.info("Chart: %s", path)


if __name__ == "__main__":
    pipeline()