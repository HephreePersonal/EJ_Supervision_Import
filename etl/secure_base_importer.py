"""Simplified secure importer base class."""

from __future__ import annotations

import logging
from typing import Any

from etl.base_importer import BaseDBImporter


logger = logging.getLogger(__name__)


class SecureBaseDBImporter(BaseDBImporter):
    """Base importer that always enables extra validation."""

    def __init__(self) -> None:
        super().__init__()
        self.extra_validation = True
