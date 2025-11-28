import math
from datetime import datetime, date
from typing import Any, Dict, List

from supabase import Client, create_client

from config import settings
from logger import log


# ---------- JSON sanitization helpers ----------

def _sanitize_value(v: Any) -> Any:
    """
    Make sure a value is safe to send through Supabase's JSON client:
    - datetimes/dates -> ISO strings
    - NaN / +/-inf -> None
    - dicts/lists -> sanitized recursively
    - everything else -> unchanged
    """
    if isinstance(v, (datetime, date)):
        return v.isoformat()

    if isinstance(v, float):
        if not math.isfinite(v):
            return None

    if isinstance(v, dict):
        return {k: _sanitize_value(val) for k, val in v.items()}

    if isinstance(v, list):
        return [_sanitize_value(x) for x in v]

    return v


def _sanitize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _sanitize_value(v) for k, v in row.items()}


# ---------- Supabase client singleton ----------

_sb: Client | None = None


def get_client() -> Client:
    global _sb
    if _sb is None:
        _sb = create_client(settings.supabase_url, settings.supabase_key)
    return _sb


sb = get_client()


# ---------- spot_tf helpers (kept for archived indicators) ----------

def upsert_spot_tf_row(symbol: str, snapshot: Dict[str, Any]) -> None:
    """
    Upsert one row into spot_tf (symbol+timeframe+use_case unique).
    """
    sb = get_client()

    timeframe = snapshot.get("timeframe")
    use_case = snapshot.get("use_case")

    row = {
        "symbol": symbol,
        "timeframe": timeframe,
        "use_case": use_case,
        "payload": _sanitize_value(snapshot),
    }

    try:
        existing = (
            sb.table("spot_tf")
            .select("symbol,timeframe,use_case,payload")
            .eq("symbol", symbol)
            .eq("timeframe", timeframe)
            .eq("use_case", use_case)
            .execute()
        )

        existing_rows = existing.data or []
        if existing_rows:
            current_payload = existing_rows[0].get("payload")
            if current_payload == row["payload"]:
                log(
                    "info",
                    "supabase_spot_tf_no_change",
                    symbol=symbol,
                    timeframe=timeframe,
                    use_case=use_case,
                )
                return

        # 2) Actually upsert when there is a change or no existing row
        sb.table("spot_tf").upsert(row, on_conflict="symbol,timeframe,use_case").execute()

    except Exception as e:
        log(
            "error",
            "supabase_spot_tf_upsert_error",
            symbol=symbol,
            snapshot=row,
            error=str(e),
        )
        raise


def build_position_id(symbol: str) -> str:
    """
    Build a stable primary key for Alpaca-imported positions.
    Example: alpaca:SPY250919C00450000
    """
    return f"alpaca:{symbol.upper()}"


def upsert_position_row(row: Dict[str, Any]) -> str:
    """
    Upsert a position row into public.positions.
    Row MUST contain 'id' and any base fields (symbol, asset_type, occ, qty, avg_cost, etc.)

    Uses Supabase 'upsert' on conflict id.
    """
    clean = _sanitize_row(row)
    try:
        sb.table("positions").upsert(clean, on_conflict="id").execute()
    except Exception as e:
        log("error", "supabase_upsert_error", row=clean, error=str(e))
        raise
    return "upserted"


def delete_missing_positions(current_ids: List[str]) -> None:
    """
    Delete positions whose id starts with 'alpaca:' but are not in current_ids.
    This prevents touching rows from other brokers.
    """
    current_set = set(current_ids)

    res = sb.table("positions").select("id").like("id", "alpaca:%").execute()
    rows = res.data or []

    for r in rows:
        pid = r["id"]
        if pid not in current_set:
            sb.table("positions").delete().eq("id", pid).execute()
            log("info", "deleted_stale_position", id=pid)


def fetch_spot_symbols_for_indicators(max_symbols: int = 500) -> List[str]:
    """
    Return a list of symbols eligible for spot indicators, based on the 'spot' table.

    'spot' is expected to expose:
      - instrument_id as the raw symbol/underlier
      - asset_type to filter out options

    Only rows where asset_type looks like equity/stock/ETF/underlier
    will be included. OCC-style option codes are also filtered out
    using a length+digit heuristic for extra safety.
    """
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
        log("error", "supabase_fetch_spot_symbols_error", error=str(e))
        return []

    data = resp.data or []
    symbols: List[str] = []
    seen: set[str] = set()

    for row in data:
        raw_sym = row.get("instrument_id")
        asset_type = (row.get("asset_type") or "").lower()

        if not raw_sym:
            continue

        # Only accept real underliers
        if asset_type not in ("equity", "stock", "etf", "underlier"):
            continue

        sym = str(raw_sym).upper().strip()
        if not sym:
            continue

        # EXTRA SAFETY:
        # Option OCC symbols are long and contain digits
        # e.g., AMD240118C00100000
        if len(sym) > 6 and any(c.isdigit() for c in sym):
            continue

        if sym not in seen:
            seen.add(sym)
            symbols.append(sym)

    return symbols


def fetch_active_positions() -> List[Dict[str, Any]]:
    """
    Get all non-zero qty positions for Alpaca-imported rows (id like 'alpaca:%').
    Also select generated 'underlier' for options so quotes loop can fetch underlier spot.
    """
    res = (
        sb.table("positions")
        .select("id,symbol,occ,asset_type,contract_multiplier,qty,avg_cost,underlier")
        .neq("qty", 0)
        .like("id", "alpaca:%")
        .execute()
    )
    return res.data or []


def update_quote_fields(pid: str, fields: Dict[str, Any]) -> None:
    """
    Update mark / prev_close / underlier_spot / last_updated for a given position id.
    """
    clean = _sanitize_row(fields)
    try:
        sb.table("positions").update(clean).eq("id", pid).execute()
    except Exception as e:
        log("error", "supabase_update_quote_fields_error", id=pid, fields=clean, error=str(e))
        raise
