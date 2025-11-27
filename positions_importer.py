import os
import asyncio
import math
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx

from .config import settings
from .logger import log
from . import tradier_client
from . import supabase_client
from . import market_data
from . import spot_indicators

symbol_index_for_indicators = 0

import re

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


async def _fetch_alpaca_positions(
    client: httpx.AsyncClient,
) -> List[Dict[str, Any]]:
    """
    Fetch open positions from Alpaca PAPER account.

    Uses:
      - ALPACA_API_KEY
      - ALPACA_API_SECRET
      - ALPACA_PAPER_BASE_URL (optional, defaults to https://paper-api.alpaca.markets/v2)
    """
    api_key = os.environ.get("ALPACA_API_KEY")
    api_secret = os.environ.get("ALPACA_API_SECRET")
    if not api_key or not api_secret:
        raise RuntimeError(
            "Missing ALPACA_API_KEY / ALPACA_API_SECRET environment variables"
        )

    base_url = os.environ.get(
        "ALPACA_PAPER_BASE_URL", "https://paper-api.alpaca.markets/v2"
    ).rstrip("/")

    url = f"{base_url}/positions"
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
    }

    log("debug", "alpaca_positions_request", url=url)

    resp = await client.get(url, headers=headers, timeout=10.0)
    resp.raise_for_status()
    data = resp.json()

    # Alpaca returns a list; just be defensive
    if isinstance(data, list):
        log("debug", "alpaca_positions_response_list", count=len(data))
        return data
    if isinstance(data, dict) and "positions" in data and isinstance(
        data["positions"], list
    ):
        log("debug", "alpaca_positions_response_dict", count=len(data["positions"]))
        return data["positions"]

    log(
        "warning",
        "alpaca_positions_unexpected_response_shape",
        shape=type(data).__name__,
    )
    return []


async def run_positions_loop() -> None:
    """
    Periodically sync positions from Alpaca PAPER into public.positions.

    Flow:
      1) Fetch open positions from Alpaca PAPER.
      2) Normalize them into a unified format:
         - symbol
         - occ (for options)
         - asset_type (equity / option)
         - qty
         - avg_cost
         - underlier_symbol
         - contract_multiplier
      3) Fetch LIVE quotes from Tradier for both:
         - equity symbols (for mark / prev_close)
         - OCC symbols and underliers (for options and their spot).
      4) Upsert each into Supabase public.positions
      5) Delete positions that no longer exist at the broker
    """
    interval = max(3, settings.poll_positions_sec)
    log(
        "info",
        "positions_loop_start",
        interval=interval,
        source_broker="alpaca_paper",
        quote_broker="tradier_live",
    )

    while True:
        start = datetime.now(timezone.utc)
        try:
            async with httpx.AsyncClient() as client:
                # 1) Fetch all positions from Alpaca PAPER
                raw_positions: List[Dict[str, Any]] = await _fetch_alpaca_positions(
                    client
                )
                log(
                    "info",
                    "alpaca_positions_fetched",
                    count=len(raw_positions),
                )

                # If nothing, skip this cycle
                if not raw_positions:
                    log("info", "alpaca_positions_empty", reason="no_open_positions")
                    await asyncio.sleep(interval)
                    continue

                # 2) Normalize positions and build symbol list for LIVE quotes
                symbols_to_quote: List[str] = []
                enriched: List[Dict[str, Any]] = []

                # Fallback account_id if Alpaca doesn’t send one
                default_account_id = os.environ.get(
                    "ALPACA_PAPER_ACCOUNT_ID", "alpaca-paper"
                )

                for p in raw_positions:
                    account_id = str(p.get("account_id") or default_account_id)

                    sym_raw = str(p.get("symbol", "")).upper().strip()
                    if not sym_raw:
                        log("warning", "alpaca_position_missing_symbol", raw=p)
                        continue

                    # Alpaca sends qty as string
                    qty_raw = p.get("qty") or p.get("quantity") or "0"
                    try:
                        qty = int(float(qty_raw))
                    except Exception:
                        log("warning", "alpaca_position_bad_qty", qty_raw=qty_raw, raw=p)
                        qty = 0

                    # Use Alpaca cost_basis / avg_entry_price if available
                    cost_basis_total = float(p.get("cost_basis") or 0.0)
                    avg_cost = (
                        float(p.get("avg_entry_price"))
                        if p.get("avg_entry_price") is not None
                        else (cost_basis_total / qty if qty not in (0, 0.0) else None)
                    )

                    asset_class = str(p.get("asset_class", "")).lower()
                    # Treat as option if asset_class says "option" or if it's a long OCC-like symbol
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

                    # Collect for quotes (LIVE via Tradier):
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
                            "account_id": account_id,
                            "symbol": symbol,
                            "occ": occ,
                            "asset_type": asset_type,
                            "qty": qty,
                            "avg_cost": avg_cost,
                            "contract_multiplier": contract_multiplier,
                            "underlier_symbol": underlier_symbol,
                        }
                    )

                log(
                    "debug",
                    "positions_normalized",
                    count=len(enriched),
                    symbols_to_quote_count=len(symbols_to_quote),
                )

                # 3) Fetch LIVE quotes for all collected symbols (Tradier LIVE)
                async with httpx.AsyncClient() as live_client:
                    log(
                        "info",
                        "quotes_request_start",
                        symbols_count=len(symbols_to_quote),
                    )
                    quotes = await tradier_client.fetch_quotes(
                        live_client, symbols_to_quote
                    )
                    log(
                        "info",
                        "quotes_response_received",
                        quotes_keys=len(quotes.keys()) if isinstance(quotes, dict) else 0,
                    )

                # 4) Build fully-populated rows and upsert into positions
                current_ids: List[str] = []

                for pos in enriched:
                    account_id = pos["account_id"]
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
                        # Option mark from OCC symbol (fallback to underlier if needed)
                        oq_key = occ or symbol
                        oq = quotes.get(oq_key) if isinstance(quotes, dict) else None
                        if oq:
                            mark = oq.get("last") or oq.get("close")
                            prev_close = oq.get("prevclose")

                        # Underlier spot from underlying symbol, if we have it
                        if underlier_symbol:
                            uq = (
                                quotes.get(underlier_symbol)
                                if isinstance(quotes, dict)
                                else None
                            )
                            if uq:
                                underlier_spot = uq.get("last") or uq.get("close")
                    else:
                        # Equity: mark and spot from same symbol
                        sq = quotes.get(symbol) if isinstance(quotes, dict) else None
                        if sq:
                            mark = sq.get("last") or sq.get("close")
                            prev_close = sq.get("prevclose")
                            underlier_spot = mark

                    # Build primary key id
                    pid_symbol = occ if (asset_type == "option" and occ) else symbol
                    pid = supabase_client.build_tradier_id(account_id, pid_symbol)

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

                    log(
                        "debug",
                        "position_row_built",
                        id=pid,
                        symbol=symbol,
                        asset_type=asset_type,
                        qty=qty,
                    )

                    current_ids.append(pid)
                    status = supabase_client.upsert_position_row(row)
                    log(
                        "info",
                        "position_upsert",
                        id=pid,
                        asset_type=asset_type,
                        qty=qty,
                        status=status,
                        env="alpaca-paper+tradier-live",
                    )

                # 5) Delete positions that no longer exist at the broker
                supabase_client.delete_missing_tradier_positions(current_ids)
                log(
                    "info",
                    "positions_cleanup_done",
                    kept_count=len(current_ids),
                )

        except Exception as e:
            log("error", "positions_loop_error", error=str(e))

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        sleep_for = max(0, interval - elapsed)
        log("debug", "positions_loop_sleep", sleep_for=sleep_for)
        await asyncio.sleep(sleep_for)


async def run_quotes_loop() -> None:
    """
    Periodically refresh quote fields for active positions using LIVE quotes:
    - mark
    - prev_close
    - underlier_spot

    This now acts as a refresher: positions are initially born with quotes
    in the positions loop, and this loop keeps them up to date.
    """
    interval = max(2, settings.poll_quotes_sec)
    log("info", "quotes_loop_start", interval=interval)

    while True:
        start = datetime.now(timezone.utc)
        try:
            active = supabase_client.fetch_active_tradier_positions()
            if not active:
                log("info", "quotes_loop_no_active_positions")
                await asyncio.sleep(interval)
                continue

            log("debug", "quotes_loop_active_positions", count=len(active))

            symbols_to_quote: List[str] = []
            for r in active:
                symbol = str(r.get("symbol", "")).upper()
                underlier = str(r.get("underlier") or "").upper()
                occ = str(r.get("occ") or "").upper()
                asset_type = r.get("asset_type")

                if asset_type == "option":
                    if occ:
                        symbols_to_quote.append(occ)
                    if underlier:
                        symbols_to_quote.append(underlier)
                else:
                    if symbol:
                        symbols_to_quote.append(symbol)

            log(
                "debug",
                "quotes_loop_symbols_to_quote",
                symbols_count=len(symbols_to_quote),
            )

            async with httpx.AsyncClient() as client:
                quotes = await tradier_client.fetch_quotes(client, symbols_to_quote)
                log(
                    "info",
                    "quotes_loop_quotes_received",
                    quotes_keys=len(quotes.keys()) if isinstance(quotes, dict) else 0,
                )

            for r in active:
                pid = r["id"]
                symbol = str(r.get("symbol", "")).upper()
                underlier = str(r.get("underlier") or "").upper()
                occ = str(r.get("occ") or "").upper()
                asset_type = r.get("asset_type")

                mark = None
                prev_close = None
                underlier_spot = None

                if asset_type == "option":
                    oq_key = occ or symbol
                    oq = quotes.get(oq_key) if isinstance(quotes, dict) else None
                    if oq:
                        mark = oq.get("last") or oq.get("close")
                        prev_close = oq.get("prevclose")

                    if underlier:
                        uq = (
                            quotes.get(underlier)
                            if isinstance(quotes, dict)
                            else None
                        )
                        if uq:
                            underlier_spot = uq.get("last") or uq.get("close")
                else:
                    sq = quotes.get(symbol) if isinstance(quotes, dict) else None
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

                log(
                    "debug",
                    "quotes_loop_update_fields",
                    id=pid,
                    mark=fields["mark"],
                    prev_close=fields["prev_close"],
                    underlier_spot=fields["underlier_spot"],
                )

                supabase_client.update_quote_fields(pid, fields)

            log("info", "quotes_updated", count=len(active), env="live")

        except Exception as e:
            log("error", "quotes_loop_error", error=str(e))

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        sleep_for = max(0, interval - elapsed)
        log("debug", "quotes_loop_sleep", sleep_for=sleep_for)
        await asyncio.sleep(sleep_for)


async def run_spot_indicators_loop() -> None:
    """
    Polygon Basic–friendly mode:

    - Run every 30 seconds.
    - Each cycle:
        - Pick ONE timeframe from a rotating list:
            5m  (scalp)
            15m (day)
            1h  (day)
            1d  (swing)
        - Fetch candles for ONE symbol for that timeframe.
        - Compute indicators and upsert into spot_tf.
    - Result:
        - ~2 aggregate calls per minute from the bot.
        - Each timeframe refreshed about every 2 minutes.
        - Plenty of buffer under a 5 calls/min rate limit.
    """
    # 30 seconds between cycles → ~2 calls/min
    interval = 30

    tf_cycle = [
        ("5m", "scalp"),
        ("15m", "day"),
        ("1h", "day"),
        ("1d", "swing"),
    ]
    tf_index = 0

    PER_REQUEST_DELAY_SEC = 0.0  # only 1 request per cycle

    log("info", "spot_indicators_loop_start", interval=interval)

    global symbol_index_for_indicators

    while True:
        start = datetime.now(timezone.utc)

        # Rotate timeframe each cycle
        tf, use_case = tf_cycle[tf_index]
        tf_index = (tf_index + 1) % len(tf_cycle)

        try:
            # Only one symbol per cycle to keep load tiny
            symbols = supabase_client.fetch_spot_symbols_for_indicators()

            if not symbols:
                log("info", "spot_indicators_no_symbols")
            else:
                # Rotate index safely
                symbol_count = len(symbols)
                symbol_index_for_indicators = symbol_index_for_indicators % symbol_count
                symbol = symbols[symbol_index_for_indicators]
                symbol_index_for_indicators += 1  # next cycle will use the next symbol

                log(
                    "info",
                    "spot_indicators_symbol_cycle",
                    symbol=symbol,
                    timeframe=tf,
                    use_case=use_case,
                )

                async with httpx.AsyncClient() as client:
                    try:
                        candles = await market_data.fetch_candles(
                            client,
                            symbol=symbol,
                            interval=tf,
                            limit=1000,
                        )

                        log(
                            "debug",
                            "spot_indicators_candles_fetched",
                            symbol=symbol,
                            timeframe=tf,
                            count=len(candles),
                        )

                        if len(candles) < 30:
                            log(
                                "info",
                                "spot_indicators_not_enough_candles",
                                symbol=symbol,
                                timeframe=tf,
                                count=len(candles),
                            )
                        else:
                            snapshot = spot_indicators.compute_spot_snapshot(
                                candles,
                                timeframe=tf,
                                use_case=use_case,
                                fractal=2,
                            )
                            log(
                                "debug",
                                "spot_indicators_snapshot_computed",
                                symbol=symbol,
                                timeframe=tf,
                            )

                            supabase_client.upsert_spot_tf_row(symbol, snapshot)

                            log(
                                "info",
                                "spot_indicators_upserted",
                                symbol=symbol,
                                timeframe=tf,
                                use_case=use_case,
                            )

                        if PER_REQUEST_DELAY_SEC > 0:
                            await asyncio.sleep(PER_REQUEST_DELAY_SEC)

                    except httpx.HTTPStatusError as exc:
                        status = exc.response.status_code
                        log(
                            "error",
                            "spot_indicators_http_error",
                            symbol=symbol,
                            timeframe=tf,
                            status=status,
                            detail=str(exc),
                        )
                        if PER_REQUEST_DELAY_SEC > 0:
                            await asyncio.sleep(PER_REQUEST_DELAY_SEC)

                    except Exception as inner_e:
                        log(
                            "error",
                            "spot_indicators_symbol_tf_error",
                            symbol=symbol,
                            timeframe=tf,
                            error=str(inner_e),
                        )
                        if PER_REQUEST_DELAY_SEC > 0:
                            await asyncio.sleep(PER_REQUEST_DELAY_SEC)

        except Exception as e:
            log("error", "spot_indicators_loop_error", error=str(e))

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        sleep_for = max(0, interval - elapsed)
        log("debug", "spot_indicators_loop_sleep", sleep_for=sleep_for)
        await asyncio.sleep(sleep_for)
