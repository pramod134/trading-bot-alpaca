import asyncio
import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx

from config import settings
from logger import log
import supabase_client
import market_client



OCC_UNDERLYING_RE = re.compile(r"^([A-Za-z]+)")


def extract_underlier(occ: str) -> str:
    """
    Extract underlier for OCC symbols like:
        SPY251126P00672000 → SPY
        QQQ251231C00644000 → QQQ
        AMD260102P00180000 → AMD

    If it's already an equity ticker (only letters), return unchanged.
    """
    if not occ:
        return ""

    # If no digits → already an equity
    if occ.isalpha():
        return occ.upper()

    m = OCC_UNDERLYING_RE.match(occ)
    if m:
        return m.group(1).upper()

    return occ.upper()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(v: Any) -> Any:
    try:
        if v is None:
            return None
        f = float(v)
        if not math.isfinite(f):
            return None
        return f
    except Exception:
        return None


async def _fetch_alpaca_positions(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """
    Fetch all open positions from Alpaca paper trading.

    Uses:
        - settings.alpaca_paper_base
        - settings.alpaca_api_key
        - settings.alpaca_api_secret
    """
    url = f"{settings.alpaca_paper_base}/v2/positions"
    headers = {
        "APCA-API-KEY-ID": settings.alpaca_api_key,
        "APCA-API-SECRET-KEY": settings.alpaca_api_secret,
        "Accept": "application/json",
    }
    r = await client.get(url, headers=headers, timeout=15)
    r.raise_for_status()
    js = r.json()
    if not js:
        return []
    if isinstance(js, dict):
        # In theory Alpaca always returns a list, but be defensive
        return [js]
    return js


async def run_positions_loop() -> None:
    """
    Periodically sync positions from Alpaca PAPER trading into public.positions.

    This loop also fetches LIVE quotes (via Tradier) before inserting,
    so each row in positions is born with mark / prev_close / underlier_spot
    already filled as best as possible.
    """
    interval = max(3, settings.poll_positions_sec)
    log(
        "info",
        "positions_loop_start",
        interval=interval,
        broker="alpaca",
    )

    while True:
        start = datetime.now(timezone.utc)
        try:
            async with httpx.AsyncClient() as client:
                # 1) Fetch all positions from Alpaca paper
                raw_positions: List[Dict[str, Any]] = await _fetch_alpaca_positions(client)
                log(
                    "info",
                    "alpaca_positions_fetched",
                    count=len(raw_positions),
                )

                # If broker reports no open positions, immediately clear any
                # broker-origin positions so we don't leave stale rows when
                # positions are closed manually at the broker.
                if not raw_positions:
                    log(
                        "info",
                        "alpaca_positions_empty",
                        msg="No open positions from Alpaca; clearing broker-origin positions",
                    )
                    # Passing an empty list means "there are no current broker
                    # positions", so delete_missing_positions should remove all
                    # positions tied to this broker.
                    supabase_client.delete_missing_positions([])
                    await asyncio.sleep(interval)
                    continue

                # 2) Build symbol list for quotes (live) and precompute metadata
                symbols_to_quote: List[str] = []
                enriched: List[Dict[str, Any]] = []

                for p in raw_positions:
                    # Alpaca position schema:
                    # - symbol
                    # - qty
                    # - avg_entry_price
                    # - asset_class (equity, option, etc.)
                    sym_raw = str(p.get("symbol", "")).upper()
                    if not sym_raw:
                        continue

                    # qty can be string; allow floats but cast to int
                    qty_raw = p.get("qty") or p.get("quantity") or 0
                    try:
                        qty = int(float(qty_raw))
                    except Exception:
                        qty = 0

                    if qty == 0:
                        continue

                    avg_entry_raw = (
                        p.get("avg_entry_price")
                        or p.get("average_entry_price")
                        or p.get("avg_cost")
                        or p.get("cost_basis")
                    )
                    try:
                        avg_cost = float(avg_entry_raw) if avg_entry_raw is not None else None
                    except Exception:
                        avg_cost = None

                    asset_class = str(p.get("asset_class") or p.get("asset_type") or "").lower()
                    # Treat as option if Alpaca says "option" or if it's a long OCC-like symbol
                    is_option = asset_class == "option" or len(sym_raw) > 15

                    asset_type = "option" if is_option else "equity"
                    contract_multiplier = 100 if is_option else 1

                    if is_option:
                        # OCC string from Alpaca (e.g. SPY251126P00672000)
                        occ = sym_raw

                        # Underlier extracted from OCC (SPY251126P00672000 → SPY)
                        underlier_symbol = extract_underlier(sym_raw)

                        # The “symbol” we store in DB is always the simple underlier
                        symbol = underlier_symbol
                    else:
                        # Equity case — symbol is already correct
                        occ = None
                        underlier_symbol = ""
                        symbol = sym_raw

                    # Collect for quotes:
                    if asset_type == "equity":
                        # Equities: quote the symbol itself
                        symbols_to_quote.append(symbol)
                    else:
                        # Options: quote the OCC for option price,
                        # and the underlier for spot.
                        if occ:
                            symbols_to_quote.append(occ)
                        if underlier_symbol:
                            symbols_to_quote.append(underlier_symbol)

                    enriched.append(
                        {
                            "account_id": "alpaca",  # kept for compatibility, not used in id
                            "symbol": symbol,
                            "occ": occ,
                            "asset_type": asset_type,
                            "qty": qty,
                            "avg_cost": avg_cost,
                            "contract_multiplier": contract_multiplier,
                            "underlier_symbol": underlier_symbol,
                        }
                    )

                if not enriched:
                    await asyncio.sleep(interval)
                    continue

                # 3) Fetch LIVE quotes for all collected symbols
                async with httpx.AsyncClient() as live_client:
                    quotes = await market_client.fetch_quotes(live_client, symbols_to_quote)

                # 4) Upsert into Supabase
                current_ids: List[str] = []
                for pos in enriched:
                    symbol = pos["symbol"]
                    occ = pos["occ"]
                    asset_type = pos["asset_type"]
                    qty = pos["qty"]
                    avg_cost = pos["avg_cost"]
                    contract_multiplier = pos["contract_multiplier"]
                    underlier_symbol = pos["underlier_symbol"]

                    mark = None
                    prev_close = None
                    underlier_spot = None

                    if asset_type == "option":
                        # Option mark from OCC symbol (fallback to symbol)
                        oq_key = occ or symbol
                        oq = quotes.get(oq_key)
                        if oq:
                            mark = oq.get("last") or oq.get("close")
                            prev_close = oq.get("prevclose")

                        # Underlier spot from underlier symbol
                        if underlier_symbol:
                            uq = quotes.get(underlier_symbol)
                            if uq:
                                underlier_spot = uq.get("last") or uq.get("close")
                    else:
                        # Equity: mark and spot from same symbol
                        sq = quotes.get(symbol)
                        if sq:
                            mark = sq.get("last") or sq.get("close")
                            prev_close = sq.get("prevclose")
                            underlier_spot = mark

                    # Build primary key id
                    pid_symbol = occ if (asset_type == "option" and occ) else symbol
                    pid = supabase_client.build_position_id(pid_symbol)

                    row: Dict[str, Any] = {
                        "id": pid,
                        "symbol": symbol,
                        "asset_type": asset_type,
                        "occ": occ,
                        "qty": qty,
                        "avg_cost": _safe_float(avg_cost),
                        "mark": _safe_float(mark),
                        "prev_close": _safe_float(prev_close),
                        "contract_multiplier": contract_multiplier,
                        "underlier_spot": _safe_float(underlier_spot),
                        "last_updated": _now_iso(),
                    }

                    current_ids.append(pid)
                    status = supabase_client.upsert_position_row(row)
                    log(
                        "info",
                        "position_upsert",
                        id=pid,
                        asset_type=asset_type,
                        qty=qty,
                        status=status,
                        env="alpaca+tradier_live",
                    )

                # 5) Delete broker-origin positions that no longer exist
                supabase_client.delete_missing_positions(current_ids)

        except Exception as e:
            log("error", "positions_loop_error", error=str(e))

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        sleep_for = max(0, interval - elapsed)
        await asyncio.sleep(sleep_for)


async def run_quotes_loop() -> None:
    """
    Periodically refresh quote fields for active positions using LIVE quotes:
    - mark
    - prev_close
    - underlier_spot

    This acts as a refresher: positions are initially born with quotes
    in the positions loop, and this loop keeps them up to date.
    """
    interval = max(2, settings.poll_quotes_sec)
    log("info", "quotes_loop_start", interval=interval)

    while True:
        start = datetime.now(timezone.utc)
        try:
            active = supabase_client.fetch_active_positions()
            if not active:
                await asyncio.sleep(interval)
                continue

            symbols_to_quote: List[str] = []
            for r in active:
                symbol = str(r.get("symbol", "")).upper()
                underlier = str(r.get("underlier") or "").upper()
                occ = str(r.get("occ") or "").upper()
                asset_type = r.get("asset_type")

                if asset_type == "option":
                    # For options, quote OCC for option price and underlier for spot
                    if occ:
                        symbols_to_quote.append(occ)
                    if underlier:
                        symbols_to_quote.append(underlier)
                else:
                    # For equities, just quote the symbol
                    if symbol:
                        symbols_to_quote.append(symbol)

            if not symbols_to_quote:
                await asyncio.sleep(interval)
                continue

            async with httpx.AsyncClient() as client:
                quotes = await market_client.fetch_quotes(client, symbols_to_quote)

            for r in active:
                pid = r.get("id")
                symbol = str(r.get("symbol", "")).upper()
                underlier = str(r.get("underlier") or "").upper()
                occ = str(r.get("occ") or "").upper()
                asset_type = r.get("asset_type")

                mark = None
                prev_close = None
                underlier_spot = None

                if asset_type == "option":
                    oq_key = occ or symbol
                    oq = quotes.get(oq_key)
                    if oq:
                        mark = oq.get("last") or oq.get("close")
                        prev_close = oq.get("prevclose")

                    # Underlier spot from underlier
                    if underlier:
                        uq = quotes.get(underlier)
                        if uq:
                            underlier_spot = uq.get("last") or uq.get("close")
                else:
                    sq = quotes.get(symbol)
                    if sq:
                        mark = sq.get("last") or sq.get("close")
                        prev_close = sq.get("prevclose")
                        underlier_spot = mark

                fields: Dict[str, Any] = {
                    "mark": _safe_float(mark),
                    "prev_close": _safe_float(prev_close),
                    "underlier_spot": _safe_float(underlier_spot),
                    "last_updated": _now_iso(),
                }
                supabase_client.update_quote_fields(pid, fields)

            log("info", "quotes_updated", count=len(active), env="live")

        except Exception as e:
            log("error", "quotes_loop_error", error=str(e))

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        sleep_for = max(0, interval - elapsed)
        await asyncio.sleep(sleep_for)
