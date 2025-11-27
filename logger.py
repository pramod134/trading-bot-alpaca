import json
import sys
import time
from typing import Any


def log(level: str, msg: str, **kv: Any) -> None:
    rec = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": level,
        "msg": msg,
    }
    rec.update(kv)
    sys.stdout.write(json.dumps(rec) + "\n")
    sys.stdout.flush()
