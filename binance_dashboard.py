#!/usr/bin/env python3
"""Build and push a Binance perpetual 4h OI/price dashboard to TRMNL.

The calculation is restart-safe: every run asks Binance for recent 5-minute
history, so the computer does not need to stay on for four continuous hours.
Only Python's standard library is required.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

FAPI = "https://fapi.binance.com"
DAPI = "https://dapi.binance.com"
USER_AGENT = "TRMNL-Binance-Perpetual-Pulse/1.0"
TAIPEI = ZoneInfo("Asia/Taipei")
HISTORY_MS = 4 * 60 * 60 * 1000
FIVE_MIN_MS = 5 * 60 * 1000
TARGET_TOLERANCE_MS = 15 * 60 * 1000
DEFAULT_HISTORY_LIMIT = 55
MAX_TRMNL_BODY_BYTES = 1950


@dataclass(frozen=True)
class Instrument:
    symbol: str
    pair: str
    market: str  # UM, CM, TF
    api: str  # fapi, dapi
    underlying_type: str


@dataclass
class MarketPoint:
    symbol: str
    market: str
    oi_now: float
    oi_then: float
    price_now: float
    price_then: float
    oi_change: float
    price_change: float
    delta_oi_usd: float
    asof_ms: int
    active: bool
    cached: bool = False


class Progress:
    def __init__(self, total: int) -> None:
        self.total = total
        self.done = 0
        self.lock = threading.Lock()

    def tick(self) -> None:
        with self.lock:
            self.done += 1
            if self.done == self.total or self.done % 100 == 0:
                print(f"Fetched {self.done}/{self.total} symbols", flush=True)


def request_json(url: str, *, body: bytes | None = None, method: str | None = None,
                 attempts: int = 2, timeout: int = 12) -> Any:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, headers=headers, data=body, method=method)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                raw = response.read()
                return json.loads(raw) if raw.strip() else None
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:240]
            if exc.code in (418, 429):
                raise RuntimeError(f"Binance rate limited this run (HTTP {exc.code})") from exc
            last_error = RuntimeError(f"HTTP {exc.code}: {detail}")
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
        if attempt + 1 < attempts:
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"Request failed: {url}: {last_error}")


def url_with_query(base: str, path: str, params: dict[str, Any]) -> str:
    return f"{base}{path}?{urllib.parse.urlencode(params)}"


def load_universe() -> list[Instrument]:
    um_info = request_json(f"{FAPI}/fapi/v1/exchangeInfo")
    cm_info = request_json(f"{DAPI}/dapi/v1/exchangeInfo")
    result: list[Instrument] = []

    for row in um_info.get("symbols", []):
        contract_type = row.get("contractType")
        if row.get("status") != "TRADING":
            continue
        if contract_type not in ("PERPETUAL", "TRADIFI_PERPETUAL"):
            continue
        result.append(
            Instrument(
                symbol=row["symbol"],
                pair=row.get("pair") or row["symbol"],
                market="TF" if contract_type == "TRADIFI_PERPETUAL" else "UM",
                api="fapi",
                underlying_type=row.get("underlyingType") or "",
            )
        )

    for row in cm_info.get("symbols", []):
        if row.get("contractStatus") != "TRADING":
            continue
        if row.get("contractType") != "PERPETUAL":
            continue
        result.append(
            Instrument(
                symbol=row["symbol"],
                pair=row.get("pair") or row["symbol"],
                market="CM",
                api="dapi",
                underlying_type=row.get("underlyingType") or "COIN",
            )
        )

    unique = {x.symbol: x for x in result}
    return sorted(unique.values(), key=lambda x: (x.market, x.symbol))


def binance_time_ms() -> int:
    return int(request_json(f"{FAPI}/fapi/v1/time")["serverTime"])


def load_trading_schedule() -> dict[str, Any]:
    try:
        return request_json(f"{FAPI}/fapi/v1/tradingSchedule").get("marketSchedules", {})
    except Exception as exc:
        print(f"WARN trading schedule unavailable: {exc}", file=sys.stderr)
        return {}


def tradfi_is_active(underlying_type: str, now_ms: int, schedule: dict[str, Any],
                     latest_price_ms: int) -> bool:
    sessions = schedule.get(underlying_type, {}).get("sessions", [])
    for session in sessions:
        if int(session["startTime"]) <= now_ms < int(session["endTime"]):
            return session.get("type") != "NO_TRADING"
    # New or unmapped TradFi categories fall back to freshness rather than being dropped.
    return now_ms - latest_price_ms <= 20 * 60 * 1000


def closest_point(points: list[tuple[int, float]], target_ms: int,
                  tolerance_ms: int = TARGET_TOLERANCE_MS) -> tuple[int, float]:
    if not points:
        raise ValueError("empty time series")
    selected = min(points, key=lambda x: abs(x[0] - target_ms))
    if abs(selected[0] - target_ms) > tolerance_ms:
        raise ValueError("no data point close enough to T-4h")
    return selected


def percent_change(now_value: float, then_value: float) -> float:
    if not math.isfinite(now_value) or not math.isfinite(then_value) or then_value <= 0:
        raise ValueError("invalid comparison values")
    return (now_value / then_value - 1.0) * 100.0


def fetch_instrument(inst: Instrument, now_ms: int, schedule: dict[str, Any],
                     history_limit: int) -> MarketPoint:
    target_ms = now_ms - HISTORY_MS
    if inst.api == "fapi":
        oi_url = url_with_query(
            FAPI,
            "/futures/data/openInterestHist",
            {"symbol": inst.symbol, "period": "5m", "limit": history_limit},
        )
        price_url = url_with_query(
            FAPI,
            "/fapi/v1/markPriceKlines",
            {"symbol": inst.symbol, "interval": "5m", "limit": history_limit},
        )
    else:
        oi_url = url_with_query(
            DAPI,
            "/futures/data/openInterestHist",
            {"pair": inst.pair, "contractType": "PERPETUAL", "period": "5m", "limit": history_limit},
        )
        price_url = url_with_query(
            DAPI,
            "/dapi/v1/markPriceKlines",
            {"symbol": inst.symbol, "interval": "5m", "limit": history_limit},
        )

    oi_raw = request_json(oi_url)
    price_raw = request_json(price_url)
    oi_points = sorted(
        (int(x["timestamp"]), float(x["sumOpenInterestValue"])) for x in oi_raw
    )
    # Mark-price candle close time and close value.
    price_points = sorted((int(x[6]), float(x[4])) for x in price_raw)
    if len(oi_points) < 2 or len(price_points) < 2:
        raise ValueError("insufficient history")

    oi_latest_ts, oi_latest_raw = oi_points[-1]
    price_latest_ts, price_now = price_points[-1]
    _, oi_then_raw = closest_point(oi_points, target_ms)
    _, price_then = closest_point(price_points, target_ms)

    # COIN-M reports sumOpenInterestValue in the margin asset (e.g. BTC),
    # while UM/TradFi reports quote value. Convert CM to USD notional.
    if inst.market == "CM":
        oi_now = oi_latest_raw * price_now
        oi_then = oi_then_raw * price_then
    else:
        oi_now = oi_latest_raw
        oi_then = oi_then_raw

    asof_ms = min(oi_latest_ts, price_latest_ts)
    active = True
    if inst.market == "TF":
        active = tradfi_is_active(
            inst.underlying_type, now_ms, schedule, price_latest_ts
        )

    oi_change = percent_change(oi_now, oi_then)
    price_change_value = percent_change(price_now, price_then)
    return MarketPoint(
        symbol=inst.symbol,
        market=inst.market,
        oi_now=oi_now,
        oi_then=oi_then,
        price_now=price_now,
        price_then=price_then,
        oi_change=oi_change,
        price_change=price_change_value,
        delta_oi_usd=oi_now - oi_then,
        asof_ms=asof_ms,
        active=active,
    )


def load_cache(path: Path) -> dict[str, MarketPoint]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return {key: MarketPoint(**value) for key, value in raw.get("points", {}).items()}
    except (FileNotFoundError, json.JSONDecodeError, TypeError, ValueError):
        return {}


def save_cache(path: Path, points: list[MarketPoint], payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "saved_at_ms": int(time.time() * 1000),
        "points": {x.symbol: asdict(x) for x in points},
        "last_payload": payload,
    }
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(data, handle, ensure_ascii=False, separators=(",", ":"))
        temp_name = handle.name
    Path(temp_name).replace(path)


def fetch_all(instruments: list[Instrument], now_ms: int, schedule: dict[str, Any],
              workers: int, history_limit: int, cache_path: Path,
              cache_max_age_minutes: int) -> tuple[list[MarketPoint], list[str]]:
    cached = load_cache(cache_path)
    results: list[MarketPoint] = []
    errors: list[str] = []
    progress = Progress(len(instruments))

    def run(inst: Instrument) -> MarketPoint:
        try:
            return fetch_instrument(inst, now_ms, schedule, history_limit)
        finally:
            progress.tick()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        jobs = {pool.submit(run, inst): inst for inst in instruments}
        for future in concurrent.futures.as_completed(jobs):
            inst = jobs[future]
            try:
                results.append(future.result())
            except Exception as exc:
                old = cached.get(inst.symbol)
                age_ms = now_ms - old.asof_ms if old else 10**18
                if old and age_ms <= cache_max_age_minutes * 60 * 1000:
                    old.cached = True
                    results.append(old)
                else:
                    errors.append(f"{inst.symbol}: {exc}")
    return results, errors


def fmt_money(value: float) -> str:
    magnitude = abs(value)
    if magnitude >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if magnitude >= 1_000_000:
        return f"${value / 1_000_000:.0f}M"
    if magnitude >= 1_000:
        return f"${value / 1_000:.0f}K"
    return f"${value:.0f}"


def fmt_pct(value: float) -> str:
    return f"{value:+.2f}%"


def signal_name(oi_change: float, price_change_value: float) -> str:
    if oi_change >= 0 and price_change_value >= 0:
        return "LONG BUILD"
    if oi_change >= 0 and price_change_value < 0:
        return "SHORT BUILD"
    if oi_change < 0 and price_change_value >= 0:
        return "SHORT COVER"
    return "LONG UNWIND"


def side(value: float) -> str:
    return "up" if value >= 0 else "down"


def price_rows(rows: list[MarketPoint]) -> list[dict[str, Any]]:
    scale = max((abs(x.price_change) for x in rows), default=1.0) or 1.0
    return [
        {
            "s": x.symbol,
            "t": x.market,
            "p": fmt_pct(x.price_change),
            "d": side(x.price_change),
            "w": max(8, round(abs(x.price_change) / scale * 100)),
        }
        for x in rows
    ]


def build_payload(points: list[MarketPoint], universe: list[Instrument], now_ms: int,
                  min_oi_usd: float, oi_rows: int, price_row_count: int,
                  error_count: int) -> dict[str, Any]:
    eligible = [x for x in points if x.active and x.oi_now >= min_oi_usd]
    oi_ranked = sorted(eligible, key=lambda x: abs(x.delta_oi_usd), reverse=True)[:oi_rows]
    gainers = sorted(
        (x for x in eligible if x.price_change >= 0),
        key=lambda x: x.price_change,
        reverse=True,
    )[:price_row_count]
    losers = sorted(
        (x for x in eligible if x.price_change < 0),
        key=lambda x: x.price_change,
    )[:price_row_count]

    now_sum = sum(x.oi_now for x in points)
    then_sum = sum(x.oi_then for x in points)
    total_change = percent_change(now_sum, then_sum) if then_sum > 0 else 0.0
    counts = {key: sum(1 for x in universe if x.market == key) for key in ("UM", "CM", "TF")}

    payload = {
        "u": datetime.fromtimestamp(now_ms / 1000, TAIPEI).strftime("%m/%d %H:%M"),
        "um": counts["UM"],
        "cm": counts["CM"],
        "tf": counts["TF"],
        "to": fmt_money(now_sum),
        "no": fmt_pct(total_change),
        "ec": error_count,
        "oi": [
            {
                "s": x.symbol,
                "t": x.market,
                "v": fmt_money(x.oi_now),
                "o": fmt_pct(x.oi_change),
                "p": fmt_pct(x.price_change),
                "od": side(x.oi_change),
                "pd": side(x.price_change),
                "g": signal_name(x.oi_change, x.price_change),
                "c": 1 if x.cached else 0,
            }
            for x in oi_ranked
        ],
        "ga": price_rows(gainers),
        "lo": price_rows(losers),
    }
    return payload


def trmnl_body(payload: dict[str, Any]) -> bytes:
    compact = json.loads(json.dumps(payload))
    while True:
        body = json.dumps(
            {"merge_variables": compact}, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        if len(body) <= MAX_TRMNL_BODY_BYTES:
            return body
        if len(compact["ga"]) > 3 or len(compact["lo"]) > 3:
            target = "ga" if len(compact["ga"]) >= len(compact["lo"]) else "lo"
            compact[target].pop()
            continue
        if len(compact["oi"]) > 7:
            compact["oi"].pop()
            continue
        raise RuntimeError(f"TRMNL payload cannot fit below {MAX_TRMNL_BODY_BYTES} bytes")


def push_to_trmnl(webhook_url: str, payload: dict[str, Any]) -> int:
    if not webhook_url.startswith("https://trmnl.com/api/custom_plugins/"):
        raise RuntimeError("TRMNL webhook URL does not look valid")
    body = trmnl_body(payload)
    request_json(webhook_url, body=body, method="POST", attempts=3, timeout=25)
    return len(body)


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--webhook", default=os.getenv("TRMNL_WEBHOOK_URL", ""))
    parser.add_argument("--dry-run", action="store_true", help="build data without posting")
    parser.add_argument("--output", help="write the compact payload JSON to this path")
    parser.add_argument("--max-symbols", type=int, default=0, help="test only; 0 means all")
    parser.add_argument("--workers", type=int, default=0)
    parser.add_argument("--state-dir", default=".state")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_config(Path(args.config))
    webhook = args.webhook or config.get("trmnl_webhook_url", "")
    workers = args.workers or int(config.get("workers", 64))
    min_oi_usd = float(config.get("min_oi_usd", 5_000_000))
    oi_rows = int(config.get("oi_rows", 10))
    price_rows_count = int(config.get("price_rows", 5))
    min_success_ratio = float(config.get("min_success_ratio", 0.60))
    cache_max_age = int(config.get("cache_max_age_minutes", 30))
    history_limit = int(config.get("history_limit", DEFAULT_HISTORY_LIMIT))
    if history_limit < 51:
        raise RuntimeError("history_limit must be at least 51 for a 4h comparison")

    now_ms = binance_time_ms()
    universe = load_universe()
    if args.max_symbols:
        # Keep examples from every market during smoke tests.
        selected: list[Instrument] = []
        per_market = max(1, args.max_symbols // 3)
        for market in ("UM", "CM", "TF"):
            selected.extend([x for x in universe if x.market == market][:per_market])
        universe = selected[: args.max_symbols]
    if not universe:
        raise RuntimeError("Binance returned an empty perpetual universe")

    schedule = load_trading_schedule()
    cache_path = Path(args.state_dir) / "market_cache.json"
    points, errors = fetch_all(
        universe,
        now_ms,
        schedule,
        workers,
        history_limit,
        cache_path,
        cache_max_age,
    )
    success_ratio = len(points) / len(universe)
    if success_ratio < min_success_ratio:
        sample = "; ".join(errors[:5])
        raise RuntimeError(
            f"Only {len(points)}/{len(universe)} symbols succeeded; "
            f"last TRMNL screen retained. Examples: {sample}"
        )

    payload = build_payload(
        points,
        universe,
        now_ms,
        min_oi_usd,
        oi_rows,
        price_rows_count,
        len(errors),
    )
    if len(payload["oi"]) < 5 or len(payload["ga"]) < 2 or len(payload["lo"]) < 2:
        raise RuntimeError("Not enough valid ranked rows; last TRMNL screen retained")

    body_size = len(trmnl_body(payload))
    if args.output:
        Path(args.output).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    if args.dry_run:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        if not webhook:
            raise RuntimeError("Set trmnl_webhook_url in config.json or TRMNL_WEBHOOK_URL")
        body_size = push_to_trmnl(webhook, payload)

    save_cache(cache_path, points, payload)
    print(
        f"OK Binance Perpetuals | {len(points)}/{len(universe)} symbols | "
        f"UM {payload['um']} CM {payload['cm']} TF {payload['tf']} | "
        f"{body_size} bytes | {payload['u']} Taipei",
        flush=True,
    )
    if errors:
        print(f"WARN {len(errors)} symbols omitted; first: {errors[0]}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Cancelled", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
