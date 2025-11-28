# bot/main.py

import asyncio

from .position_importer import (
    run_positions_loop,
    run_quotes_loop,
)
from .new_trade_importer import run_new_trades_import_loop


async def main() -> None:
    await asyncio.gather(
        run_positions_loop(),
        run_quotes_loop(),
        run_new_trades_import_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
