import os
from dataclasses import dataclass


@dataclass
class Settings:
    # Sandbox (positions) - no longer used for trading, kept for compatibility
    tradier_sandbox_token: str
    tradier_sandbox_accounts: list[str]
    tradier_sandbox_base: str

    # Live (quotes)
    tradier_live_token: str
    tradier_live_base: str

    # Alpaca paper trading (positions)
    alpaca_api_key: str
    alpaca_api_secret: str
    alpaca_paper_base: str

    # Supabase
    supabase_url: str
    supabase_key: str

    # Timers
    poll_positions_sec: int
    poll_quotes_sec: int

    # Spot TF polling (kept for compatibility, even though indicators loop is archived)
    poll_spot_tf_sec: int = int(os.getenv("POLL_SPOT_TF_SEC", "900"))  # 15 minutes default

    @classmethod
    def load(cls) -> "Settings":
        return cls(
            # Sandbox (legacy - not used for trading anymore)
            tradier_sandbox_token=os.environ.get("TRADIER_SANDBOX_TOKEN", ""),
            tradier_sandbox_accounts=[
                a.strip()
                for a in os.environ.get("TRADIER_SANDBOX_ACCOUNT_IDS", "").split(",")
                if a.strip()
            ],
            tradier_sandbox_base=os.environ.get(
                "TRADIER_SANDBOX_BASE_URL", "https://sandbox.tradier.com/v1"
            ),

            # Live (quotes)
            tradier_live_token=os.environ["TRADIER_LIVE_TOKEN"],
            tradier_live_base=os.environ.get(
                "TRADIER_LIVE_BASE_URL", "https://api.tradier.com/v1"
            ),

            # Alpaca paper (positions)
            alpaca_api_key=os.environ["ALPACA_API_KEY"],
            alpaca_api_secret=os.environ["ALPACA_API_SECRET"],
            alpaca_paper_base=os.environ.get(
                "ALPACA_PAPER_BASE_URL", "https://paper-api.alpaca.markets"
            ),

            # Supabase
            supabase_url=os.environ["SUPABASE_URL"],
            supabase_key=os.environ["SUPABASE_SERVICE_KEY"],

            # Timers
            poll_positions_sec=int(os.environ.get("POLL_POSITIONS_SEC", 10)),
            poll_quotes_sec=int(os.environ.get("POLL_QUOTES_SEC", 5)),
        )


settings = Settings.load()
