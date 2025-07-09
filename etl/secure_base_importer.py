"""Simplified secure importer base class."""

from __future__ import annotations
import logging
import os
from etl.base_importer import BaseDBImporter


logger = logging.getLogger(__name__)


class SecureBaseDBImporter(BaseDBImporter):
    """Base importer that always enables extra validation."""

    def __init__(self) -> None:
        super().__init__()
        self.extra_validation = True

    def clear_migration_history(self) -> None:
        """Clear all migration history to force fresh execution."""
        from db.connections import get_target_connection
        from db.migrations import VERSION_TABLE

        logger.info("Clearing migration history to force fresh execution")

        try:
            with get_target_connection() as conn:
                # Clear the migration history table
                clear_sql = f"DELETE FROM dbo.{VERSION_TABLE}"
                if hasattr(conn, "execute") and not hasattr(conn, "cursor"):
                    # SQLAlchemy connection
                    import sqlalchemy

                    conn.execute(sqlalchemy.text(clear_sql))
                    conn.commit()
                else:
                    # DB-API connection
                    with conn.cursor() as cursor:
                        cursor.execute(clear_sql)
                    conn.commit()

                logger.info("Migration history cleared successfully")
        except Exception as e:
            logger.warning(f"Failed to clear migration history: {e}")

    def run(self) -> bool:
        """Override run to clear migration history first."""
        # Clear migration history to force fresh execution
        self.clear_migration_history()

        # Force fresh start by clearing progress tracking
        os.environ["RESUME"] = "0"

        # Call parent run method
        return super().run()
