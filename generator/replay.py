"""Replay Berka transactions into PostgreSQL so Debezium sees them as live changes.

  python replay.py seed         load the accounts that existed before the cutoff
  python replay.py run          replay the rest, one simulated day every 2 seconds
  python replay.py timelapse    replay one year in one minute, showing 10 accounts

Add --local to run or timelapse to keep everything in memory and never touch PostgreSQL.
"""
import argparse
import time

import pandas as pd
import psycopg
from rich.live import Live
from rich.table import Table

DSN = "host=localhost dbname=bank user=postgres password=admin-local-only connect_timeout=5"
CSV = "data/translist.csv"
CUTOFF = pd.Timestamp("1993-09-01")

INSERT_ACCOUNT = ("INSERT INTO accounts (account_id, opened_on, balance) VALUES (%s, %s, %s) "
                  "ON CONFLICT (account_id) DO NOTHING")
INSERT_TRANSACTION = ("INSERT INTO transactions (transaction_id, account_id, transaction_date, amount, "
                      "operation, balance_after) VALUES (%s, %s, %s, %s, %s, %s) "
                      "ON CONFLICT (transaction_id) DO NOTHING")
UPDATE_BALANCE = "UPDATE accounts SET balance = %s, updated_at = now() WHERE account_id = %s"


def load_transactions():
    df = pd.read_csv(CSV, sep=";", usecols=["trans_id", "account_id", "date", "operation", "amount", "balance"])
    df["date"] = pd.to_datetime(df["date"].astype(str), format="%y%m%d")
    df = put_in_order(df)
    df["amount"] = df.groupby("account_id")["balance"].diff().fillna(df["balance"]).round(2)
    df["operation"] = df["operation"].astype(object).where(df["operation"].notna(), None)
    return df


def put_in_order(df):
    """trans_id isn't chronological, so order each account's same-day rows by following the balance."""
    df = df.sort_values(["account_id", "date", "trans_id"]).reset_index(drop=True)
    acct, day, amt, bal = (df[c].to_numpy() for c in ["account_id", "date", "amount", "balance"])
    order, i, prev = [], 0, 0.0
    while i < len(df):
        j = i
        while j < len(df) and acct[j] == acct[i] and day[j] == day[i]:
            j += 1
        if i == 0 or acct[i] != acct[i - 1]:
            prev = 0.0
        pending = list(range(i, j))
        while pending:
            k = next((k for k in pending if abs(abs(bal[k] - prev) - amt[k]) < 0.01), pending[0])
            pending.remove(k)
            order.append(k)
            prev = bal[k]
        i = j
    return df.iloc[order].sort_values("date", kind="stable").reset_index(drop=True)


def accounts_at_cutoff(df):
    return (df[df["date"] < CUTOFF].groupby("account_id")
            .agg(opened_on=("date", "first"), balance=("balance", "last")))


def seed(conn, df):
    before = accounts_at_cutoff(df)
    rows = [(int(a), r.opened_on.date(), float(r.balance)) for a, r in before.iterrows()]
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(INSERT_ACCOUNT, rows)
    print(f"Seeded {len(rows):,} accounts with their {CUTOFF.date()} balances")


class PostgresBook:
    """Writes every change to PostgreSQL, where Debezium picks it up."""

    def __init__(self, conn):
        self.conn = conn
        with conn.cursor() as cur:
            last = cur.execute("SELECT last_replayed_day FROM replay_state").fetchone()[0]
            self.known = {a for (a,) in cur.execute("SELECT account_id FROM accounts")}
        self.start = pd.Timestamp(last) + pd.Timedelta(days=1) if last else CUTOFF

    def apply_day(self, day, rows):
        d = day.date()
        new = {int(a) for a in rows["account_id"]} - self.known
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.executemany(INSERT_ACCOUNT, [(a, d, 0) for a in new])
            cur.executemany(INSERT_TRANSACTION, [
                (int(r.trans_id), int(r.account_id), d, float(r.amount), r.operation, float(r.balance))
                for r in rows.itertuples()])
            cur.executemany(UPDATE_BALANCE, [(float(r.balance), int(r.account_id)) for r in rows.itertuples()])
            cur.execute("UPDATE replay_state SET last_replayed_day = %s", (d,))
        self.known.update(new)

    def balances(self, ids):
        with self.conn.cursor() as cur:
            return dict(cur.execute(
                "SELECT account_id, balance FROM accounts WHERE account_id = ANY(%s)", (ids,)).fetchall())

    def first_accounts(self, n):
        with self.conn.cursor() as cur:
            return [a for (a,) in cur.execute(
                "SELECT account_id FROM accounts ORDER BY opened_on, account_id LIMIT %s", (n,))]


class LocalBook:
    """Keeps balances in memory. Nothing is written anywhere."""

    def __init__(self, df):
        before = accounts_at_cutoff(df)
        self.opened = before["opened_on"].to_dict()
        self.balance = before["balance"].to_dict()
        self.start = CUTOFF

    def apply_day(self, day, rows):
        for r in rows.itertuples():
            self.opened.setdefault(r.account_id, day)
            self.balance[r.account_id] = r.balance

    def balances(self, ids):
        return {a: self.balance[a] for a in ids}

    def first_accounts(self, n):
        return sorted(self.opened, key=lambda a: (self.opened[a], a))[:n]


def run(book, df, seconds_per_day, until):
    todo = df[df["date"] >= book.start]
    if until:
        todo = todo[todo["date"] <= pd.Timestamp(until)]
    for day, rows in todo.groupby("date"):
        started = time.monotonic()
        book.apply_day(day, rows)
        print(f"{day.date()}  {len(rows):4} transactions")
        time.sleep(max(0, seconds_per_day - (time.monotonic() - started)))


def timelapse(book, df, days, seconds, watch):
    todo = df[df["date"] >= book.start]
    if todo.empty:
        print("Nothing left to replay.")
        return
    todo = todo[todo["date"] < todo["date"].min() + pd.Timedelta(days=days)]
    days_to_play = list(todo.groupby("date"))
    pace = seconds / len(days_to_play)

    ids = book.first_accounts(watch)
    previous = book.balances(ids)
    with Live() as live:
        for day, rows in days_to_play:
            started = time.monotonic()
            book.apply_day(day, rows)
            balances = book.balances(ids)
            live.update(balance_table(day, ids, balances, previous))
            previous = balances
            time.sleep(max(0, pace - (time.monotonic() - started)))


def balance_table(day, ids, balances, previous):
    table = Table(title=str(day.date()))
    table.add_column("Account")
    table.add_column("Balance", justify="right")
    table.add_column("Change", justify="right")
    for a in ids:
        change = balances[a] - previous[a]
        colour = "green" if change > 0 else "red"
        table.add_row(str(a), f"{balances[a]:,.2f}", f"[{colour}]{change:+,.2f}" if change else "")
    return table


def play(book, df, args):
    if args.mode == "run":
        run(book, df, args.seconds_per_day, args.until)
    else:
        timelapse(book, df, args.days, args.seconds, args.watch)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=["seed", "run", "timelapse"])
    parser.add_argument("--local", action="store_true", help="keep balances in memory; never touch PostgreSQL")
    parser.add_argument("--seconds-per-day", type=float, default=2, help="run: real seconds per simulated day")
    parser.add_argument("--until", help="run: stop after this date, e.g. 1993-09-30")
    parser.add_argument("--days", type=int, default=365, help="timelapse: simulated days to play")
    parser.add_argument("--seconds", type=float, default=60, help="timelapse: real seconds to play them in")
    parser.add_argument("--watch", type=int, default=10, help="timelapse: accounts to show")
    args = parser.parse_args()

    if args.local and args.mode == "seed":
        parser.error("seed only applies to PostgreSQL; --local seeds itself in memory")

    df = load_transactions()
    print(f"Loaded {len(df):,} transactions")

    if args.local:
        print("Local mode: nothing is written to PostgreSQL")
        play(LocalBook(df), df, args)
    else:
        with psycopg.connect(DSN, autocommit=True) as conn:
            if args.mode == "seed":
                seed(conn, df)
            else:
                play(PostgresBook(conn), df, args)