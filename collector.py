#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import httpx
from dotenv import load_dotenv


DEFAULT_CONFIG_PATH = "config.json"
DEFAULT_INTERVAL_SECONDS = 300
DEFAULT_DATA_DIR = "data"
DEFAULT_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
DEFAULT_CLOB_BASE_URL = "https://clob.polymarket.com"
DEFAULT_TIMEOUT_SECONDS = 20.0


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: datetime | None = None) -> str:
    return (dt or utc_now()).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def utc_day(dt: datetime | None = None) -> str:
    return (dt or utc_now()).astimezone(timezone.utc).strftime("%Y-%m-%d")


def coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
        return [raw]
    return [value]


def coerce_float_list(value: Any) -> list[float]:
    out: list[float] = []
    for item in coerce_list(value):
        try:
            out.append(float(item))
        except (TypeError, ValueError):
            continue
    return out


def coerce_str_list(value: Any) -> list[str]:
    return [str(item).strip() for item in coerce_list(value) if str(item or "").strip()]


def parse_dt(value: Any) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        return text


def safe_name(value: str) -> str:
    text = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value).strip().lower())
    return "_".join(part for part in text.split("_") if part) or "subject"


def append_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    materialized = list(rows)
    if not materialized:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in materialized:
            f.write(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n")
    return len(materialized)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def order_book_side(order_book: Any, side: str) -> list[Any]:
    if isinstance(order_book, dict):
        data = order_book.get("data")
        if isinstance(data, dict) and side in data:
            return data.get(side) or []
        return order_book.get(side) or []
    return getattr(order_book, side, []) or []


def order_book_level_value(level: Any, field: str) -> Any:
    if isinstance(level, dict):
        return level.get(field)
    return getattr(level, field)


def extract_order_book_levels(order_book: Any) -> dict[str, list[list[float]]]:
    asks = sorted(
        [
            [float(order_book_level_value(level, "price")), float(order_book_level_value(level, "size"))]
            for level in order_book_side(order_book, "asks")
        ],
        key=lambda level: level[0],
    )
    bids = sorted(
        [
            [float(order_book_level_value(level, "price")), float(order_book_level_value(level, "size"))]
            for level in order_book_side(order_book, "bids")
        ],
        key=lambda level: level[0],
        reverse=True,
    )
    return {"asks": asks, "bids": bids}


@dataclass
class SubjectConfig:
    name: str
    query: str
    active: bool = True
    include_terms: tuple[str, ...] = ()
    exclude_terms: tuple[str, ...] = ()
    limit_per_type: int = 20
    collect_outcomes: tuple[str, ...] = ("yes",)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SubjectConfig":
        name = safe_name(str(data.get("name") or data.get("query") or "subject"))
        query = str(data.get("query") or "").strip()
        if not query:
            raise ValueError(f"subject {name!r} is missing query")
        outcomes = tuple(str(x).strip().lower() for x in coerce_str_list(data.get("collect_outcomes") or ["yes"]))
        return cls(
            name=name,
            query=query,
            active=coerce_bool(data.get("active"), True),
            include_terms=tuple(x.lower() for x in coerce_str_list(data.get("include_terms"))),
            exclude_terms=tuple(x.lower() for x in coerce_str_list(data.get("exclude_terms"))),
            limit_per_type=max(1, int(data.get("limit_per_type") or 20)),
            collect_outcomes=outcomes or ("yes",),
        )

    def matches_event(self, event: dict[str, Any]) -> bool:
        haystack = f"{event.get('title') or ''} {event.get('slug') or ''} {event.get('ticker') or ''}".lower()
        if self.include_terms and not all(term in haystack for term in self.include_terms):
            return False
        if self.exclude_terms and any(term in haystack for term in self.exclude_terms):
            return False
        if self.active:
            if event.get("closed") is True:
                return False
            if event.get("active") is False:
                return False
        return True


class PolymarketCollector:
    def __init__(self, config: dict[str, Any], *, data_dir_override: str | None = None):
        self.gamma_base_url = str(config.get("gamma_base_url") or DEFAULT_GAMMA_BASE_URL).rstrip("/")
        self.clob_base_url = str(config.get("clob_base_url") or DEFAULT_CLOB_BASE_URL).rstrip("/")
        self.interval_seconds = int(config.get("interval_seconds") or DEFAULT_INTERVAL_SECONDS)
        timeout_seconds = float(config.get("request_timeout_seconds") or DEFAULT_TIMEOUT_SECONDS)
        self.timeout = httpx.Timeout(timeout_seconds)
        self.max_concurrent_orderbook_requests = max(1, int(config.get("max_concurrent_orderbook_requests") or 8))
        self.skip_untradable_orderbooks = coerce_bool(config.get("skip_untradable_orderbooks"), True)
        self.data_dir = Path(data_dir_override or os.environ.get("POLY_COLLECTOR_DATA_DIR") or config.get("data_dir") or DEFAULT_DATA_DIR)
        self.state_dir = Path(config.get("state_dir") or "state")
        self.subjects = [SubjectConfig.from_dict(item) for item in list(config.get("subjects") or [])]
        if not self.subjects:
            raise ValueError("config must contain at least one subject")

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=self.timeout, trust_env=False)

    async def public_search_events(self, client: httpx.AsyncClient, subject: SubjectConfig) -> list[dict[str, Any]]:
        params: list[tuple[str, str]] = [
            ("q", subject.query),
            ("page", "1"),
            ("limit_per_type", str(subject.limit_per_type)),
            ("type", "events"),
            ("events_status", "active" if subject.active else "all"),
            ("presets", "EventsTitle"),
            ("presets", "Events"),
        ]
        resp = await client.get(f"{self.gamma_base_url}/public-search", params=params)
        resp.raise_for_status()
        data = resp.json()
        events = data.get("events", []) if isinstance(data, dict) else []
        return [event for event in events if isinstance(event, dict)]

    async def gamma_search_events(self, client: httpx.AsyncClient, subject: SubjectConfig) -> list[dict[str, Any]]:
        params = {"query": subject.query}
        if subject.active:
            params.update({"active": "true", "closed": "false"})
        resp = await client.get(f"{self.gamma_base_url}/events", params=params)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            return [] if data.get("error") else [data]
        return [event for event in data if isinstance(event, dict)]

    async def scan_subject_events(self, client: httpx.AsyncClient, subject: SubjectConfig) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        try:
            events = await self.public_search_events(client, subject)
        except Exception as e:
            print(f"[warn] {subject.name}: public-search failed: {e}", flush=True)
        if not events:
            try:
                events = await self.gamma_search_events(client, subject)
            except Exception as e:
                print(f"[warn] {subject.name}: gamma /events fallback failed: {e}", flush=True)
                events = []
        deduped: dict[str, dict[str, Any]] = {}
        for event in events:
            slug = str(event.get("slug") or event.get("ticker") or event.get("id") or "").strip()
            if not slug or not subject.matches_event(event):
                continue
            deduped[slug] = event
        return list(deduped.values())

    def normalize_market(self, market: dict[str, Any]) -> dict[str, Any]:
        token_ids = coerce_str_list(market.get("clobTokenIds"))
        prices = coerce_float_list(market.get("outcomePrices"))
        return {
            "id": str(market.get("id") or ""),
            "question": market.get("question") or market.get("title") or "",
            "slug": market.get("slug") or "",
            "group_item_title": market.get("groupItemTitle") or "",
            "outcomes": coerce_str_list(market.get("outcomes")),
            "outcome_prices": prices,
            "clob_token_ids": token_ids,
            "active": coerce_bool(market.get("active"), True),
            "closed": coerce_bool(market.get("closed"), False),
            "resolved": coerce_bool(market.get("resolved"), False),
            "accepting_orders": coerce_bool(market.get("acceptingOrders"), True),
            "enable_order_book": coerce_bool(market.get("enableOrderBook"), True),
            "archived": coerce_bool(market.get("archived"), False),
            "condition_id": market.get("conditionId") or "",
        }

    def normalize_event(self, subject: SubjectConfig, event: dict[str, Any]) -> dict[str, Any]:
        markets = [
            self.normalize_market(market)
            for market in list(event.get("markets") or [])
            if isinstance(market, dict)
        ]
        return {
            "subject": subject.name,
            "id": str(event.get("id") or ""),
            "ticker": event.get("ticker") or "",
            "slug": event.get("slug") or event.get("ticker") or "",
            "title": event.get("title") or "",
            "description": event.get("description") or "",
            "resolution_source": event.get("resolutionSource") or "",
            "start_date": parse_dt(event.get("startDate")),
            "end_date": parse_dt(event.get("endDate")),
            "active": coerce_bool(event.get("active"), True),
            "closed": coerce_bool(event.get("closed"), False),
            "markets": markets,
        }

    def selected_tokens(self, subject: SubjectConfig, market: dict[str, Any]) -> list[tuple[str, str]]:
        token_ids = list(market.get("clob_token_ids") or [])
        if not token_ids:
            return []
        modes = set(subject.collect_outcomes)
        if "all" in modes:
            return [(f"outcome_{idx}", token_id) for idx, token_id in enumerate(token_ids)]
        selected: list[tuple[str, str]] = []
        if "yes" in modes and len(token_ids) >= 1:
            selected.append(("yes", token_ids[0]))
        if "no" in modes and len(token_ids) >= 2:
            selected.append(("no", token_ids[1]))
        return selected

    def orderbook_skip_reason(self, market: dict[str, Any]) -> str:
        if not self.skip_untradable_orderbooks:
            return ""
        if market.get("closed"):
            return "market_closed"
        if market.get("resolved"):
            return "market_resolved"
        if market.get("archived"):
            return "market_archived"
        if market.get("accepting_orders") is False:
            return "not_accepting_orders"
        if market.get("enable_order_book") is False:
            return "orderbook_disabled"
        return ""

    async def fetch_orderbook(self, client: httpx.AsyncClient, token_id: str) -> tuple[dict[str, Any] | None, str]:
        try:
            resp = await client.get(f"{self.clob_base_url}/book", params={"token_id": token_id})
            if resp.status_code == 404:
                return None, "404_no_orderbook"
            resp.raise_for_status()
            return extract_order_book_levels(resp.json()), ""
        except Exception as e:
            return None, str(e)

    def market_rows(self, captured_at: str, event: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        base = {k: event.get(k) for k in ("subject", "id", "ticker", "slug", "title", "start_date", "end_date", "active", "closed")}
        for market in event.get("markets") or []:
            rows.append(
                {
                    "captured_at": captured_at,
                    "event": base,
                    "market": market,
                }
            )
        return rows

    async def collect_subject(self, client: httpx.AsyncClient, subject: SubjectConfig) -> dict[str, Any]:
        captured_at = utc_iso()
        raw_events = await self.scan_subject_events(client, subject)
        events = [self.normalize_event(subject, event) for event in raw_events]
        subject_dir = self.data_dir / subject.name
        day = utc_day()
        market_row_count = append_jsonl(subject_dir / "markets" / f"{day}.jsonl", [row for event in events for row in self.market_rows(captured_at, event)])
        write_json(subject_dir / "latest_markets.json", {"captured_at": captured_at, "subject": subject.name, "events": events})

        orderbook_jobs: list[dict[str, Any]] = []
        for event in events:
            event_ref = {
                "subject": subject.name,
                "event_id": event.get("id"),
                "event_slug": event.get("slug"),
                "event_title": event.get("title"),
                "event_start_date": event.get("start_date"),
                "event_end_date": event.get("end_date"),
            }
            for market in event.get("markets") or []:
                skip_reason = self.orderbook_skip_reason(market)
                for outcome_name, token_id in self.selected_tokens(subject, market):
                    orderbook_jobs.append(
                        {
                            **event_ref,
                            "market_id": market.get("id"),
                            "market_slug": market.get("slug"),
                            "bucket_label": market.get("group_item_title"),
                            "question": market.get("question"),
                            "outcome": outcome_name,
                            "token_id": token_id,
                            "gamma_price": (market.get("outcome_prices") or [None])[0] if outcome_name == "yes" else ((market.get("outcome_prices") or [None, None])[1] if len(market.get("outcome_prices") or []) > 1 else None),
                            "skip_reason": skip_reason,
                        }
                    )

        semaphore = asyncio.Semaphore(self.max_concurrent_orderbook_requests)

        async def collect_orderbook_row(job: dict[str, Any]) -> dict[str, Any]:
            if job.get("skip_reason"):
                return {
                    "captured_at": utc_iso(),
                    **job,
                    "order_book": None,
                    "source": "market_not_orderbook_eligible",
                    "error": "",
                }
            async with semaphore:
                book, error = await self.fetch_orderbook(client, str(job["token_id"]))
            return {
                "captured_at": utc_iso(),
                **job,
                "order_book": book,
                "source": "clob_public_book" if book is not None else "clob_fetch_error",
                "error": error,
            }

        orderbook_rows = await asyncio.gather(*(collect_orderbook_row(job) for job in orderbook_jobs))
        orderbook_row_count = append_jsonl(subject_dir / "orderbooks" / f"{day}.jsonl", orderbook_rows)
        return {
            "subject": subject.name,
            "captured_at": captured_at,
            "events": len(events),
            "markets": market_row_count,
            "orderbooks": orderbook_row_count,
            "orderbook_errors": sum(1 for row in orderbook_rows if row.get("error")),
            "orderbook_skips": sum(1 for row in orderbook_rows if row.get("skip_reason")),
        }

    async def run_once(self, selected_subjects: set[str] | None = None) -> list[dict[str, Any]]:
        started_at = utc_iso()
        subjects = [subject for subject in self.subjects if not selected_subjects or subject.name in selected_subjects]
        if not subjects:
            raise ValueError(f"no configured subjects match: {sorted(selected_subjects or [])}")
        async with self.client() as client:
            summaries = []
            for subject in subjects:
                print(f"[scan] {subject.name}: {subject.query}", flush=True)
                summaries.append(await self.collect_subject(client, subject))
                summary = summaries[-1]
                print(
                    f"[done] {subject.name}: events={summary['events']} markets={summary['markets']} "
                    f"orderbooks={summary['orderbooks']} skips={summary['orderbook_skips']} "
                    f"errors={summary['orderbook_errors']}",
                    flush=True,
                )
        state = {"started_at": started_at, "finished_at": utc_iso(), "summaries": summaries}
        write_json(self.state_dir / "run_state.json", state)
        return summaries

    async def run_forever(self, selected_subjects: set[str] | None = None) -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:
                pass
        print(f"[start] interval={self.interval_seconds}s data_dir={self.data_dir}", flush=True)
        while not stop.is_set():
            started = time.monotonic()
            try:
                await self.run_once(selected_subjects=selected_subjects)
            except Exception as e:
                print(f"[error] collection cycle failed: {e}", file=sys.stderr, flush=True)
            elapsed = time.monotonic() - started
            sleep_for = max(1.0, float(self.interval_seconds) - elapsed)
            try:
                await asyncio.wait_for(stop.wait(), timeout=sleep_for)
            except asyncio.TimeoutError:
                pass
        print("[stop] collector stopped", flush=True)


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only Polymarket market/orderbook collector")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to collector config JSON")
    parser.add_argument("--data-dir", default=None, help="Override output data directory")
    parser.add_argument("--subject", action="append", default=[], help="Run only this configured subject name; repeatable")
    parser.add_argument("--once", action="store_true", help="Run one collection cycle and exit")
    return parser.parse_args(argv)


async def async_main(argv: list[str] | None = None) -> None:
    load_dotenv()
    args = parse_args(argv)
    config = load_config(Path(args.config))
    collector = PolymarketCollector(config, data_dir_override=args.data_dir)
    selected_subjects = {safe_name(name) for name in args.subject} if args.subject else None
    if args.once:
        await collector.run_once(selected_subjects=selected_subjects)
    else:
        await collector.run_forever(selected_subjects=selected_subjects)


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
