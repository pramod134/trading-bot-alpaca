# bot/spot_updater.py

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import httpx

from .config import settings
from .logger import log
from . import tradier_client
from . import supabase_client


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(v: Any) -> Any:
    try:
        if v is None:
            return None
        f = float(v)
        if f != f:  # NaN check
            return None
        return f
    except Exception:
        return None


def _map_instrument_to_tradier_symbol(instrument_id: str, asset_type: str) -> str:
    """
    Map internal instrument_id + asset_type -> Tradier symbol.

    No change to logic; asset_type='option' keeps OCC style.
    """
    atype = (asset_type or "").lower()
    if atype == "option":
        if instrument_id.startswith("O:"):
            return instrument_id[2:]
        return instrument_id

    return instrument_id


def _fetch_spot_rows() -> List[Dict[str, Any]]:
    """
    Read rows from public.spot.
    """
    log("debug", "spot_updater_fetch_spot_rows_start")
    res = supabase_client.sb.table("spot").select("instrument_id, asset_type").execute()

    if res.error:
        log("error", "spot_updater_fetch_spot_rows_error", error=str(res.error))
        raise RuntimeError(f"Error fetching spot rows: {res.error}")

    rows = res.data or []
    log("info", "spot_updater_fetch_spot_rows_done", count=len(rows))
    return rows


def _build_tradier_symbol_map(
    spot_rows: List[Dict[str, Any]]
) -> Tuple[List[str], Dict[str, str]]:
    """
    Build:
      tradier_symbols
      tradier_to_instrument
    """
    tradier_symbols: List[str] = []
    tradier_to_instrument: Dict[str, str] = {}

    log("debug", "spot_updater_build_symbol_map_start", rows=len(spot_rows))

    for row in spot_rows:
        instrument_id = row["instrument_id"]
        asset_type = row.get("asset_type", "equity")

        tsym = _map_instrument_to_tradier_symbol(instrument_id, asset_type)
        tsym_u = tsym.upper()

        if tsym_u not in tradier_to_instrument:
            tradier_to_instrument[tsym_u] = instrument_id
            tradier_symbols.append(tsym_u)

    log(
        "info",
        "spot_updater_build_symbol_map_done",
        tradier_symbols=len(tradier_symbols),
        mappings=len(tradier_to_instrument),
    )

    return tradier_symbols, tradier_to_instrument


def _update_spot_prices(price_map: Dict[str, float], tradier_to_instrument: Dict[str, str]) -> None:
    """
    Write last_price + last_updated to Supabase spot table.
    """
    if not price_map:
        log("info", "spot_updater_no_prices_to_update")
        return

    now_iso = _now_iso()
    log("debug", "spot_updater_update_prices_start", count=len(price_map))

    for tsym_u, last_price in price_map.items():
        instrument_id = tradier_to_instrument.get(tsym_u)
        if not instrument_id:
            log("warning", "spot_updater_missing_map", symbol=tsym_u)
            continue

        fields = {"last_price": last_price, "last_updated": now_iso}

        try:
            supabase_client.sb.table("spot").update(fields).eq(
                "instrument_id", instrument_id
            ).execute()
            log(
                "debug",
                "spot_updater_update_one",
                instrument_id=instrument_id,
                symbol=tsym_u,
                last_price=last_price,
            )
        except Exception as e:
            log(
                "error",
                "spot_update_error",
                instrument_id=instrument_id,
                symbol=tsym_u,
                error=str(e),
            )

    log("info", "spot_updater_update_prices_done", count=len(price_map))


async def run_spot_updater_loop() -> None:
    """
    Pull live prices from Tradier (real quotes) every 2s.
    """
    interval = 2
    log("info", "spot_updater_loop_start", interval=interval, tradier_base=settings.tradier_live_base)

    async with httpx.AsyncClient() as client:
        while True:
            cycle_start = datetime.now(timezone.utc)
            try:
                spot_rows = _fetch_spot_rows()
                if not spot_rows:
                    log("info", "spot_updater_no_spot_rows")
                    await asyncio.sleep(interval)
                    continue

                tradier_symbols, tradier_to_instrument = _build_tradier_symbol_map(spot_rows)
                if not tradier_symbols:
                    log("info", "spot_updater_no_symbols_to_query")
                    await asyncio.sleep(interval)
                    continue

                log(
                    "debug",
                    "spot_updater_query_tradier",
                    symbols=tradier_symbols,
                )

                quotes: Dict[str, Dict[str, Any]] = await tradier_client.fetch_quotes(
                    client, tradier_symbols
                )

                price_map: Dict[str, float] = {}
                for tsym_u, q in quotes.items():
                    last = _safe_float(q.get("last"))
                    if last is None:
                        bid = _safe_float(q.get("bid"))
                        ask = _safe_float(q.get("ask"))
                        if bid is not None and ask is not None:
                            last = (bid + ask) / 2.0

                    if last is None:
                        log(
                            "warning",
                            "spot_updater_no_price_for_symbol",
                            symbol=tsym_u,
                            quote=q,
                        )
                        continue

                    price_map[tsym_u.upper()] = last

                _update_spot_prices(price_map, tradier_to_instrument)

                log(
                    "info",
                    "spot_updater_cycle_done",
                    updated=len(price_map),
                    total=len(tradier_symbols),
                )

            except Exception as e:
                log("error", "spot_updater_loop_error", error=str(e))

            elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()
            sleep_for = max(0.0, interval - elapsed)
            log("debug", "spot_updater_sleep", sleep_for=sleep_for)
            await asyncio.sleep(sleep_for)


async def main() -> None:
    await run_spot_updater_loop()


if __name__ == "__main__":
    asyncio.run(main())
