import datetime as dt
import os
import sqlite3


def default_db_path() -> str:
    """The one frontier file the spider, the sitemap poller and run_discovery.sh
    share: `$FRONTIER_DB`, else `frontier.db` in the current directory."""
    return os.environ.get("FRONTIER_DB", "frontier.db")


class Frontier:
    def __init__(self, db_path: str):
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS urls (
                url TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                first_seen DATE NOT NULL,
                last_fetch_ts TEXT,
                last_status INTEGER,
                content_hash TEXT,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                in_panel INTEGER NOT NULL DEFAULT 0,
                gone_date DATE
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_source_fetch ON urls(source, last_fetch_ts)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_panel ON urls(in_panel, last_fetch_ts)"
        )
        self._conn.commit()

    def add_urls(self, source: str, urls: list[str]) -> int:
        today = dt.date.today().isoformat()
        added = 0
        for url in urls:
            try:
                self._conn.execute(
                    "INSERT INTO urls (url, source, first_seen) VALUES (?, ?, ?)",
                    (url, source, today),
                )
                added += 1
            except sqlite3.IntegrityError:
                pass
        self._conn.commit()
        return added

    def next_batch(self, source: str, kind: str, limit: int) -> list[str]:
        if kind == "panel":
            cur = self._conn.execute(
                "SELECT url FROM urls WHERE source = ? AND in_panel = 1 "
                "ORDER BY last_fetch_ts ASC NULLS FIRST LIMIT ?",
                (source, limit),
            )
        else:
            cur = self._conn.execute(
                "SELECT url FROM urls WHERE source = ? AND last_fetch_ts IS NULL "
                "AND gone_date IS NULL "
                "ORDER BY first_seen ASC LIMIT ?",
                (source, limit),
            )
        return [row[0] for row in cur.fetchall()]

    def mark_fetched(self, url: str, status: int, content_hash: str) -> None:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        self._conn.execute(
            "UPDATE urls SET last_fetch_ts = ?, last_status = ?, content_hash = ?, "
            "consecutive_failures = 0 WHERE url = ?",
            (now, status, content_hash, url),
        )
        self._conn.commit()

    def mark_failed(self, url: str) -> None:
        self._conn.execute(
            "UPDATE urls SET consecutive_failures = consecutive_failures + 1 WHERE url = ?",
            (url,),
        )
        self._conn.execute(
            "UPDATE urls SET gone_date = ? WHERE url = ? AND consecutive_failures >= 2",
            (dt.date.today().isoformat(), url),
        )
        self._conn.commit()

    def is_gone(self, url: str) -> bool:
        cur = self._conn.execute(
            "SELECT gone_date FROM urls WHERE url = ?", (url,)
        )
        row = cur.fetchone()
        return row is not None and row[0] is not None

    def freeze_panel_cohort(self, size: int) -> int:
        cur = self._conn.execute(
            "SELECT url FROM urls WHERE last_fetch_ts IS NOT NULL "
            "AND gone_date IS NULL AND in_panel = 0 "
            "ORDER BY RANDOM() LIMIT ?",
            (size,),
        )
        urls = [row[0] for row in cur.fetchall()]
        if urls:
            placeholders = ",".join("?" for _ in urls)
            self._conn.execute(
                f"UPDATE urls SET in_panel = 1 WHERE url IN ({placeholders})",
                urls,
            )
            self._conn.commit()
        return len(urls)

    def panel_due(self, as_of: dt.date) -> list[str]:
        cur = self._conn.execute(
            "SELECT url FROM urls WHERE in_panel = 1 AND gone_date IS NULL"
        )
        return [row[0] for row in cur.fetchall()]
