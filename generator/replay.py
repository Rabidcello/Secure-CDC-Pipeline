#!/usr/bin/env python3
"""Replay the Berka transaction file into PostgreSQL as live changes for CDC."""
import argparse
import time

import pandas as pd
import psycopg

p = argparse.ArgumentParser()
p.add_argument("--csv", default="data/trans.csv")
p.add_argument("--sep", default=";", help="Berka files are usually semicolon-separated")
p.add_argument("--cutoff", default="1993-09-01", help="accounts before this date are pre-existing")
p.add_argument("--seconds-per-day", type=float, default=2.0, help="real seconds per simulated day")
p.add_argument("--until", default=None, help="stop after this date, e.g. 1993-09-30")
p.add_argument("--seed-only", action="store_true", help="load pre-existing accounts, then exit")
p.add_argument("--dsn", default="host=localhost dbname=bank user=postgres password=admin-local-only")
args = p.parse_args()

SEED_SQL = ("INSERT INTO accounts (account_id, opened_on, balance) VALUES (%s, %s, %s) "
            "ON CONFLICT (account_id) DO NOTHING")
TXN_SQL = ("INSERT INTO transactions (transaction_id, account_id, transaction_date, amount, operation, balance_after) "
           "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING")
BALANCE_SQL = "UPDATE accounts SET balance = %s, updated_at = now() WHERE account_id = %s"


def load():
    df = pd.read_csv(args.csv, sep=args.sep,
                     usecols=["trans_id", "account_id", "date", "operation", "amount", "balance"])
    df["date"] = pd.to_datetime(df["date"].astype(str), format="%y%m%d")
    df = df.sort_values(["date", "trans_id"]).reset_index(drop=True)
    # signed amount = change in balance; the first row per account is the opening deposit
    df["signed"] = df.groupby("account_id")["balance"].diff().fillna(df["balance"]).round(2)
    df["operation"] = df["operation"].astype(object).where(df["operation"].notna(), None)
    return df


df = load()
bad = df[(df["signed"].abs() - df["amount"]).abs() > 0.01]
if len(bad):
    print(f"WARNING: {len(bad)} rows where order and balance disagree (same-day ordering). Sample:")
    print(bad.head())

cutoff = pd.Timestamp(args.cutoff)

with psycopg.connect(args.dsn, autocommit=True) as conn:
    # 1. Seed: accounts that existed before the cutoff, at their balance on the cutoff date
    seed = (df[df["date"] < cutoff]
            .groupby("account_id")
            .agg(opened_on=("date", "first"), balance=("balance", "last")))
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(SEED_SQL, [(int(a), r.opened_on.date(), float(r.balance))
                                   for a, r in seed.iterrows()])
    print(f"Seeded {len(seed)} pre-existing accounts at their {cutoff.date()} balances")
    if args.seed_only:
        raise SystemExit

    # 2. Work out where to resume from
    with conn.cursor() as cur:
        cur.execute("SELECT last_replayed_day FROM replay_state")
        last = cur.fetchone()[0]
        cur.execute("SELECT account_id FROM accounts")
        known = {row[0] for row in cur}
    start = pd.Timestamp(last) + pd.Timedelta(days=1) if last else cutoff

    replay = df[df["date"] >= start]
    if args.until:
        replay = replay[replay["date"] <= pd.Timestamp(args.until)]
    days = replay["date"].nunique()
    print(f"Replaying {len(replay):,} transactions over {days:,} days from {start.date()}")

    # 3. Replay one simulated day at a time, each day in a single database transaction
    for i, (day, batch) in enumerate(replay.groupby("date"), 1):
        t0 = time.monotonic()
        d = day.date()
        new = sorted({int(a) for a in batch["account_id"]} - known)

        with conn.transaction(), conn.cursor() as cur:
            if new:  # first appearance = new customer opening an account
                cur.executemany(SEED_SQL, [(a, d, 0) for a in new])
            cur.executemany(TXN_SQL, [
                (int(r.trans_id), int(r.account_id), d, float(r.signed), r.operation, float(r.balance))
                for r in batch.itertuples()])
            cur.executemany(BALANCE_SQL, [
                (float(r.balance), int(r.account_id)) for r in batch.itertuples()])
            cur.execute("UPDATE replay_state SET last_replayed_day = %s", (d,))

        known.update(new)
        elapsed = time.monotonic() - t0
        print(f"{d}  {len(batch):5,} txns  {len(new):3} new accounts  "
              f"day {i:,}/{days:,}  ({elapsed:.2f}s)")
        time.sleep(max(0.0, args.seconds_per_day - elapsed))

print("Replay finished")