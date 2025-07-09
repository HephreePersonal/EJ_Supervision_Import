import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

class ProgressTracker:
    """Helper to manage ETL progress files."""

    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> dict[str, Any]:
        """Return contents of the progress file or an empty dict."""
        if not self.path or not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:  # pragma: no cover - unexpected
            logger.error("Failed to read progress file %s: %s", self.path, exc)
            return {}

    def get(self, key: str, default: int = 0) -> int:
        """Get a numeric progress value for ``key``."""
        data = self.load()
        try:
            return int(data.get(key, default))
        except Exception:
            return default

    def update(self, key: str, value: int) -> None:
        """Update the progress ``key`` with ``value``."""
        if not self.path:
            return
        data = self.load()
        data[key] = value
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception as exc:  # pragma: no cover - unlikely
            logger.error("Failed to write progress file %s: %s", self.path, exc)

    def delete(self) -> None:
        """Delete the progress file if it exists."""
        if self.path and os.path.exists(self.path):
            try:
                os.remove(self.path)
            except OSError:  # pragma: no cover - best effort
                pass
