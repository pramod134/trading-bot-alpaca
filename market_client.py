import httpx
from typing import Any, Dict, List

from config import settings

# Live auth (quotes) - Tradier live is kept for market data only
QUOTE_HEADERS = {
    "Authorization": f"Bearer {settings.tradier_live_token}",
    "Accept": "application/json",
}


async def fetch_quotes(client: httpx.AsyncClient, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Live quotes from Tradier:
    - Uses live base URL + live token.
    - Returns a dict keyed by uppercased symbol -> raw quote payload.
    """
    if not symbols:
        return {}

    # Deduplicate and normalize symbols
    unique: List[str] = []
    seen: set[str] = set()
    for s in symbols:
        u = str(s or "").upper()
        if not u or u in seen:
            continue
        seen.add(u)
        unique.append(u)

    if not unique:
        return {}

    out: Dict[str, Dict[str, Any]] = {}

    # Tradier supports a reasonable batch size; keep it conservative
    for i in range(0, len(unique), 70):
        batch = unique[i : i + 70]
        url = f"{settings.tradier_live_base}/markets/quotes?symbols={','.join(batch)}"
        r = await client.get(url, headers=QUOTE_HEADERS, timeout=15)
        r.raise_for_status()
        qs = r.json().get("quotes", {}).get("quote")
        if not qs:
            continue
        if isinstance(qs, dict):
            qs = [qs]
        for q in qs:
            sym = q.get("symbol", "").upper()
            if sym:
                out[sym] = q

    return out
