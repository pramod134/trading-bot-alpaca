import os
from dataclasses import dataclass


@dataclass
class Settings:
    # Which broker to use for what
    trading_broker: str          # e.g. "alpaca"
    market_data_broker: str      # e.g. "tradier", later maybe "public"

    # Tradier (live quotes)
    tradier_live_token: str | None
    tradier_live_base: str | None

    # Alpaca (paper trading)
    alpaca_key: str | None
    alpaca_secret: str | None
    alpaca_paper_base: str | None

    # Public (future: live quotes or trading)
    public_api_key: str | None
    public_base_url: str | None

    # Supabase
    supabase_url: str
    supabase_key: str

    # Timers
    poll_positions_sec: int
    poll_quotes_sec: int
    poll_spot_tf_sec: int = int(os.getenv("POLL_SPOT_TF_SEC", "900"))  # 15 minutes default

    @classmethod
    def load(cls) -> "Settings":
        # Broker selectors: change these envs later instead of changing code
        trading_broker = os.environ.get("TRADING_BROKER", "alpaca")
        market_data_broker = os.environ.get("MARKET_DATA_BROKER", "tradier")

        return cls(
            trading_broker=trading_broker,
            market_data_broker=market_data_broker,

            # Tradier live (used now for quotes; optional if you switch later)
            tradier_live_token=os.environ.get("TRADIER_LIVE_TOKEN"),
            tradier_live_base=os.environ.get(
                "TRADIER_LIVE_BASE_URL", "https://api.tradier.com/v1"
            ) if market_data_broker == "tradier" else None,

            # Alpaca paper (current trading engine)
            alpaca_key=os.environ.get("ALPACA_API_KEY"),
            alpaca_secret=os.environ.get("ALPACA_API_SECRET"),
            alpaca_paper_base=os.environ.get(
                "ALPACA_PAPER_BASE_URL", "https://paper-api.alpaca.markets"
            ) if trading_broker == "alpaca" else None,

            # Public.com (future use)
            public_api_key=os.environ.get("PUBLIC_API_KEY"),
            public_base_url=os.environ.get("PUBLIC_BASE_URL"),

            # Supabase
            supabase_url=os.environ["SUPABASE_URL"],
            supabase_key=os.environ["SUPABASE_SERVICE_KEY"],

            # Timers
            poll_positions_sec=int(os.environ.get("POLL_POSITIONS_SEC", 10)),
            poll_quotes_sec=int(os.environ.get("POLL_QUOTES_SEC", 5)),
        )


settings = Settings.load()
