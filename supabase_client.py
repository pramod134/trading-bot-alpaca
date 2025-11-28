import math
from datetime import datetime, date
from typing import Any, Dict, List

from supabase import Client, create_client

from .config import settings
from .logger import log


# ---------- JSON sanitization helpers ----------

def _sanitize_value(v: Any) -> Any:
    """
    Make sure a value is safe to send through Supabase's JSON client:

    - NaN / +/-inf -> None
    - datetimes/dates -> ISO strings
    - dicts/lists -> sanitized recursively
    - everything else -> unchanged
    """
    if isinstance(v, (datetime, date)):
        return v.isoformat()

    if isinstance(v, float):
        if not math.isfinite(v):
            return None
        return v

    if isinstance(v, dict):
        return {k: _sanitize_value(x) for k, x in v.items()}

    if isinstance(v, list):
        return [_sanitize_value(x) for x in v]

    return v


def _sanitize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply _sanitize_value to all values in the row.
    """
    return {k: _sanitize_value(v) for k, v in row.items()}


# ---------- Supabase client ----------

log("debug", "supabase_client_init", url=settings.supabase_url)
sb: Client = create_client(settings.supabase_url, settings.supabase_key)


# ---------- Helpers for indicators (spot_tf) ----------

def upsert_spot_tf_row(symbol: str, snapshot: Dict[str, Any]) -> None:
    """
    Upsert one row into spot_tf for given symbol + timeframe + use_case.

    snapshot must include:
      - timeframe
      - use_case (optional, defaults to 'generic')
      - structure_state
      - swings
      - fvgs
      - liquidity
      - volume_profile
      - trend
      - extras
    """
    timeframe = snapshot["timeframe"]
    use_case = snapshot.get("use_case", "generic")

    row: Dict[str, Any] = {
        "symbol": symbol,
        "timeframe": timeframe,
        "use_case": use_case,
        "structure_state": snapshot.get("structure_state", "unknown"),
        "swings": snapshot.get("swings", {}),
        "fvgs": snapshot.get("fvgs", []),
        "liquidity": snapshot.get("liquidity", {}),
        "volume_profile": snapshot.get("volume_profile", {}),
        "trend": snapshot.get("trend", {}),
        "extras": snapshot.get("extras", {}),
        # let DB default now() if caller didn't set last_updated
        "last_updated": snapshot.get("last_updated"),
    }

    # Strip Nones so Supabase can use defaults
    row = {k: v for k, v in row.items() if v is not None}

    try:
        # 1) Fetch existing row (if any) so we can skip if nothing changed
        resp = (
            sb.table("spot_tf")
            .select(
                "structure_state,swings,fvgs,liquidity,volume_profile,trend,extras"
            )
            .eq("symbol", symbol)
            .eq("timeframe", timeframe)
            .eq("use_case", use_case)
            .execute()
        )
        existing_rows = getattr(resp, "data", None) or []
        existing = existing_rows[0] if existing_rows else None

        if existing:
            keys = [
                "structure_state",
                "swings",
                "fvgs",
                "liquidity",
                "volume_profile",
                "trend",
                "extras",
            ]
            unchanged = all(existing.get(k) == row.get(k) for k in keys)
            if unchanged:
                log(
                    "info",
                    "spot_tf_upsert_skipped_no_change",
                    symbol=symbol,
                    timeframe=timeframe,
                    use_case=use_case,
                )
                return

        # 2) Actually upsert when there is a change or no existing row
        sb.table("spot_tf").upsert(row, on_conflict="symbol,timeframe,use_case").execute()
        log(
            "info",
            "spot_tf_upsert_success",
            symbol=symbol,
            timeframe=timeframe,
            use_case=use_case,
        )
    except Exception as e:
        log(
            "error",
            "supabase_spot_tf_upsert_error",
            symbol=symbol,
            snapshot=row,
            error=str(e),
        )
        raise


# ---------- Helpers for positions ----------

def build_broker_position_id(broker: str, account_id: str, symbol: str) -> str:
    """
    Build a stable primary key for a broker position.

    Example:
      broker='alpaca', account_id='XYZ', symbol='SPY'
      -> 'alpaca:XYZ:SPY'
    """
    sid = f"{broker}:{account_id}:{symbol.upper()}"
    log(
        "debug",
        "supabase_build_position_id",
        broker=broker,
        account_id=account_id,
        symbol=symbol,
        id=sid,
    )
    return sid


def build_tradier_id(account_id: str, symbol: str) -> str:
    """
    Backwards-compatible helper for old Tradier-based code.

    New code should use build_broker_position_id(...) or a broker-specific helper.
    """
    return build_broker_position_id("tradier", account_id, symbol)


def build_alpaca_id(account_id: str, symbol: str) -> str:
    """
    Convenience helper for Alpaca-based positions.
    """
    return build_broker_position_id("alpaca", account_id, symbol)


def upsert_position_row(row: Dict[str, Any]) -> str:
    """
    Upsert a single row into public.positions.

    Expects row to already contain:
      - id (broker:account:symbol or similar)
      - symbol
      - asset_type
      - qty
      - occ (for options, if any)
      - avg_cost
      - contract_multiplier
      - prev_close / mark / underlier_spot (optional)
    """
    clean = _sanitize_row(row)
    log("debug", "positions_upsert_start", row=clean)
    try:
        sb.table("positions").upsert(clean, on_conflict="id").execute()
        log("info", "positions_upsert_success", id=clean.get("id"))
    except Exception as e:
        log("error", "positions_upsert_error", row=clean, error=str(e))
        raise
    return "upserted"


def delete_missing_positions(broker: str, current_ids: List[str]) -> None:
    """
    Delete positions in DB for a given broker that are no longer present
    in the broker's latest position list.

    current_ids should contain the full 'broker:account:symbol' ids
    that are still alive.
    """
    current_set = set(current_ids)
    log(
        "debug",
        "delete_missing_positions_start",
        broker=broker,
        current_count=len(current_ids),
    )

    res = sb.table("positions").select("id").like("id", f"{broker}:%").execute()
    rows = res.data or []

    for r in rows:
        pid = r["id"]
        if pid not in current_set:
            sb.table("positions").delete().eq("id", pid).execute()
            log("info", "deleted_stale_position", broker=broker, id=pid)


def delete_missing_tradier_positions(current_ids: List[str]) -> None:
    """
    Backwards-compatible wrapper that deletes stale Tradier positions.

    New code should call delete_missing_positions('alpaca', current_ids)
    or pass whichever broker is active.
    """
    delete_missing_positions("tradier", current_ids)


def fetch_spot_symbols_for_indicators(max_symbols: int = 50) -> List[str]:
    """
    Fetch a list of underlying symbols from the spot table for which we
    should compute indicators.

    We only include equity/stock/ETF/underlier rows, and we try to ignore
    OCC-style option symbols.
    """
    log("debug", "fetch_spot_symbols_for_indicators_start", max_symbols=max_symbols)

    try:
        resp = (
            sb.table("spot")
            .select("instrument_id,asset_type")
            .not_.is_("instrument_id", "null")
            .neq("instrument_id", "")
            .limit(max_symbols)
            .execute()
        )
    except Exception as e:
        log("error", "fetch_spot_symbols_for_indicators_error", error=str(e))
        return []

    data = resp.data or []
    symbols: List[str] = []
    seen: set[str] = set()

    for row in data:
        raw_sym = row.get("instrument_id")
        asset_type = (row.get("asset_type") or "").lower()

        if not raw_sym:
            continue

        # Only treat these as underliers for indicators
        if asset_type not in ("equity", "stock", "etf", "underlier"):
            continue

        sym = str(raw_sym).upper().strip()
        if not sym:
            continue

        # Filter out obvious OCC-style option symbols (long, contains digits)
        if len(sym) > 6 and any(c.isdigit() for c in sym):
            continue

        if sym not in seen:
            seen.add(sym)
            symbols.append(sym)

    log("info", "fetch_spot_symbols_for_indicators_done", count=len(symbols))
    return symbols


def fetch_active_positions(broker: str = "alpaca") -> List[Dict[str, Any]]:
    """
    Fetch active (qty != 0) positions for a given broker from public.positions.
    """
    log("debug", "fetch_active_positions_start", broker=broker)

    res = (
        sb.table("positions")
        .select("id,symbol,occ,asset_type,contract_multiplier,qty,avg_cost,underlier")
        .neq("qty", 0)
        .like("id", f"{broker}:%")
        .execute()
    )

    data = res.data or []
    log("info", "fetch_active_positions_done", broker=broker, count=len(data))
    return data


def fetch_active_tradier_positions() -> List[Dict[str, Any]]:
    """
    Backwards-compatible wrapper for old Tradier-based code.

    New code should call fetch_active_positions('alpaca') or pass
    the desired broker explicitly.
    """
    return fetch_active_positions("tradier")


def update_quote_fields(pid: str, fields: Dict[str, Any]) -> None:
    """
    Update mark / prev_close / underlier_spot / last_updated for a given position id.
    """
    clean = _sanitize_row(fields)
    log("debug", "update_quote_fields_start", pid=pid, fields=clean)
    try:
        sb.table("positions").update(clean).eq("id", pid).execute()
        log("info", "update_quote_fields_success", pid=pid)
    except Exception as e:
        # This will dump the exact payload that could not be JSON-encoded
        log("error", "supabase_update_error", id=pid, fields=clean, error=str(e))
        raise
