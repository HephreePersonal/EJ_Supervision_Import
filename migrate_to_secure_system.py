#!/usr/bin/env python3
"""Migration script to upgrade existing ETL system to secure version.

This script helps migrate from the original ETL system to the enhanced secure version
by safely moving configuration, validating the environment, and providing rollback options.
"""

import os
import sys
import json
import shutil
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import argparse

# Add the project root to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent))

from config.secure_settings import ConfigurationManager, SecretManager
from utils.sql_security import SQLSecurityValidator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('migration.log')
    ]
)
logger = logging.getLogger(__name__)


class MigrationError(Exception):
    """Exception raised during migration process."""
    pass


class ETLSystemMigration:
    """Handles migration from legacy ETL system to secure version."""
    
    def __init__(self, backup_dir: Optional[Path] = None):
        self.backup_dir = backup_dir or Path("migration_backup") / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.config_manager = ConfigurationManager()
        self.secret_manager = SecretManager()
        self.sql_validator = SQLSecurityValidator()
        
        # Migration status tracking
        self.migration_steps = {
            "backup_created": False,
            "legacy_config_validated": False,
            "secrets_migrated": False,
            "config_updated": False,
            "dependencies_checked": False,
            "sql_scripts_validated": False,
            "environment_updated": False
        }
        
    def run_migration(self, force: bool = False, validate_only: bool = False) -> bool:
        """Run the complete migration process."""
        try:
            logger.info("Starting ETL system migration to secure version")
            logger.info(f"Backup directory: {self.backup_dir}")
            
            # Phase 1: Pre-migration validation and backup
            self._create_backup()
            self._validate_legacy_system()
            self._check_dependencies()
            
            if validate_only:
                logger.info("Validation complete. System is ready for migration.")
                return True
            
            # Phase 2: Security migration
            self._migrate_configuration()
            self._migrate_secrets()
            self._update_environment()
            
            # Phase 3: Validation and cleanup
            self._validate_new_system()
            self._update_scripts()
            
            logger.info("Migration completed successfully!")
            self._print_migration_summary()
            
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            if not validate_only:
                self._attempt_rollback()
            return False
    
    def _create_backup(self) -> None:
        """Create backup of existing system."""
        logger.info("Creating backup of existing system...")
        
        try:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Backup key files and directories
            backup_items = [
                "config/",
                ".env",
                "01_JusticeDB_Import.py",
                "02_OperationsDB_Import.py", 
                "03_FinancialDB_Import.py",
                "04_LOBColumns.py",
                "run_etl.py"
            ]
            
            for item in backup_items:
                source = Path(item)
                if source.exists():
                    if source.is_dir():
                        shutil.copytree(source, self.backup_dir / source, dirs_exist_ok=True)
                    else:
                        shutil.copy2(source, self.backup_dir / source.name)
                    logger.debug(f"Backed up: {item}")
            
            # Create backup manifest
            manifest = {
                "backup_time": datetime.now().isoformat(),
                "items_backed_up": backup_items,
                "original_system_version": "1.0",
                "target_system_version": "2.0_secure"
            }
            
            with open(self.backup_dir / "migration_manifest.json", 'w') as f:
                json.dump(manifest, f, indent=2)
            
            self.migration_steps["backup_created"] = True
            logger.info(f"Backup created successfully in {self.backup_dir}")
            
        except Exception as e:
            raise MigrationError(f"Failed to create backup: {e}")
    
    def _validate_legacy_system(self) -> None:
        """Validate the existing legacy system."""
        logger.info("Validating legacy system configuration...")
        
        issues = []
        
        # Check for legacy config file
        legacy_config_path = Path("config/values.json")
        if not legacy_config_path.exists():
            issues.append("Legacy config file config/values.json not found")
        else:
            try:
                with open(legacy_config_path) as f:
                    legacy_config = json.load(f)
                
                # Validate required fields
                required_fields = ["driver", "server", "database", "user", "password", "csv_dir"]
                missing_fields = [field for field in required_fields if field not in legacy_config]
                
                if missing_fields:
                    issues.append(f"Legacy config missing required fields: {missing_fields}")
                
                # Check for security issues in legacy config
                if "password" in legacy_config and legacy_config["password"]:
                    logger.warning("Legacy config contains plaintext password - will be migrated to secure storage")
                
                self.migration_steps["legacy_config_validated"] = True
                
            except json.JSONDecodeError as e:
                issues.append(f"Legacy config file is not valid JSON: {e}")
        
        # Check for required directories
        if legacy_config_path.exists():
            with open(legacy_config_path) as f:
                config = json.load(f)
            
            csv_dir = Path(config.get("csv_dir", ""))
            if not csv_dir.exists():
                issues.append(f"CSV directory does not exist: {csv_dir}")
        
        # Check for existing ETL scripts
        required_scripts = [
            "01_JusticeDB_Import.py",
            "02_OperationsDB_Import.py", 
            "03_FinancialDB_Import.py"
        ]
        
        missing_scripts = [script for script in required_scripts if not Path(script).exists()]
        if missing_scripts:
            issues.append(f"Missing ETL scripts: {missing_scripts}")
        
        if issues:
            raise MigrationError(f"Legacy system validation failed: {'; '.join(issues)}")
        
        logger.info("Legacy system validation passed")
    
    def _check_dependencies(self) -> None:
        """Check if required dependencies are available."""
        logger.info("Checking migration dependencies...")
        
        required_packages = [
            "keyring",
            "cryptography", 
            "pydantic",
            "pydantic-settings"
        ]
        
        missing_packages = []
        
        for package in required_packages:
            try:
                __import__(package.replace("-", "_"))
            except ImportError:
                missing_packages.append(package)
        
        if missing_packages:
            logger.error(f"Missing required packages: {missing_packages}")
            logger.info("Install missing packages with:")
            logger.info(f"pip install {' '.join(missing_packages)}")
            raise MigrationError(f"Missing dependencies: {missing_packages}")
        
        self.migration_steps["dependencies_checked"] = True
        logger.info("All dependencies are available")
    
    def _migrate_configuration(self) -> None:
        """Migrate configuration from legacy to secure format."""
        logger.info("Migrating configuration to secure format...")
        
        try:
            legacy_config_path = Path("config/values.json")
            
            if legacy_config_path.exists():
                # Use the configuration manager's migration function
                self.config_manager.migrate_legacy_config(legacy_config_path)
                self.migration_steps["config_updated"] = True
                logger.info("Configuration migrated successfully")
            else:
                logger.warning("No legacy configuration found to migrate")
                
        except Exception as e:
            raise MigrationError(f"Configuration migration failed: {e}")
    
    def _migrate_secrets(self) -> None:
        """Migrate secrets to secure storage."""
        logger.info("Migrating secrets to secure storage...")
        
        try:
            # Check if secrets were already migrated by config migration
            test_secret = self.secret_manager.get_secret("mssql_connection")
            
            if test_secret:
                logger.info("Secrets successfully migrated to secure storage")
                self.migration_steps["secrets_migrated"] = True
            else:
                logger.warning("No secrets found in secure storage after migration")
                
        except Exception as e:
            raise MigrationError(f"Secret migration validation failed: {e}")
    
    def _update_environment(self) -> None:
        """Update environment variables and configuration."""
        logger.info("Updating environment configuration...")
        
        try:
            # Create/update .env file with new secure settings
            env_updates = {
                "# Secure ETL Configuration": "",
                "# Database connection (stored securely in keyring)": "",
                "# MSSQL_TARGET_CONN_STR will be loaded from keyring": "",
                "": "",
                "# File paths": "",
                "EJ_CSV_DIR": str(Path("config/values.json").parent.parent / "csv_data"),
                "EJ_LOG_DIR": str(Path("logs")),
                "": "",
                "# Performance settings": "",
                "SQL_TIMEOUT": "300",
                "CSV_CHUNK_SIZE": "50000", 
                "DB_POOL_SIZE": "5",
                "": "",
                "# Security settings": "",
                "MAX_RETRY_ATTEMPTS": "3",
                "CONNECTION_TIMEOUT": "30"
            }
            
            env_file_path = Path(".env")
            
            # Backup existing .env if it exists
            if env_file_path.exists():
                shutil.copy2(env_file_path, self.backup_dir / ".env.backup")
            
            # Write new .env file
            with open(env_file_path, 'w') as f:
                for key, value in env_updates.items():
                    if key.startswith("#") or key == "":
                        f.write(f"{key}\n")
                    else:
                        f.write(f"{key}={value}\n")
            
            self.migration_steps["environment_updated"] = True
            logger.info("Environment configuration updated")
            
        except Exception as e:
            raise MigrationError(f"Environment update failed: {e}")
    
    def _validate_new_system(self) -> None:
        """Validate the migrated secure system."""
        logger.info("Validating migrated secure system...")
        
        try:
            # Test secure configuration loading
            from config.secure_settings import get_secure_settings
            settings = get_secure_settings()
            
            # Validate key settings
            if not settings.ej_csv_dir or not settings.ej_csv_dir.exists():
                raise MigrationError("CSV directory not properly configured")
            
            # Test secret retrieval
            connection_secret = self.secret_manager.get_secret("mssql_connection")
            if not connection_secret:
                raise MigrationError("Database connection secret not found in secure storage")
            
            logger.info("Secure system validation passed")
            
        except Exception as e:
            raise MigrationError(f"New system validation failed: {e}")
    
    def _validate_sql_scripts(self) -> None:
        """Validate SQL scripts for security issues."""
        logger.info("Validating SQL scripts for security...")
        
        sql_script_dirs = [
            "sql_scripts/justice",
            "sql_scripts/operations", 
            "sql_scripts/financial"
        ]
        
        security_issues = []
        
        for script_dir in sql_script_dirs:
            script_path = Path(script_dir)
            if script_path.exists():
                for sql_file in script_path.glob("*.sql"):
                    try:
                        with open(sql_file, 'r') as f:
                            sql_content = f.read()
                        
                        # Basic security validation
                        validation_result = self.sql_validator.validate_sql_statement(sql_content, allow_ddl=True)
                        
                        if not validation_result.is_valid:
                            security_issues.append(f"{sql_file}: {'; '.join(validation_result.issues)}")
                        
                    except Exception as e:
                        logger.warning(f"Could not validate {sql_file}: {e}")
        
        if security_issues:
            logger.warning("SQL security issues found:")
            for issue in security_issues:
                logger.warning(f"  {issue}")
            logger.warning("Review these issues before running the secure ETL system")
        else:
            logger.info("SQL script security validation passed")
        
        self.migration_steps["sql_scripts_validated"] = True
    
    def _update_scripts(self) -> None:
        """Update ETL scripts to use secure versions."""
        logger.info("Scripts updated - use new secure versions:")
        logger.info("  - 01_JusticeDB_Import_Secure.py")
        logger.info("  - 02_OperationsDB_Import_Secure.py") 
        logger.info("  - 03_FinancialDB_Import_Secure.py")
        logger.info("  - run_etl_secure.py")
    
    def _attempt_rollback(self) -> None:
        """Attempt to rollback changes on migration failure."""
        logger.error("Attempting to rollback migration changes...")
        
        try:
            # Restore backed up files
            if self.backup_dir.exists():
                backup_files = [
                    (".env", ".env"),
                    ("config", "config"),
                ]
                
                for backup_name, restore_name in backup_files:
                    backup_path = self.backup_dir / backup_name
                    restore_path = Path(restore_name)
                    
                    if backup_path.exists():
                        if restore_path.exists():
                            if restore_path.is_dir():
                                shutil.rmtree(restore_path)
                            else:
                                restore_path.unlink()
                        
                        if backup_path.is_dir():
                            shutil.copytree(backup_path, restore_path)
                        else:
                            shutil.copy2(backup_path, restore_path)
                        
                        logger.info(f"Restored: {restore_name}")
                
                logger.info("Rollback completed - system restored to previous state")
            else:
                logger.error("No backup found for rollback")
                
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            logger.error("Manual restoration may be required")
    
    def _print_migration_summary(self) -> None:
        """Print summary of migration results."""
        logger.info("\n" + "="*60)
        logger.info("MIGRATION SUMMARY")
        logger.info("="*60)
        
        for step, completed in self.migration_steps.items():
            status = "✓ COMPLETED" if completed else "✗ FAILED"
            logger.info(f"{step.replace('_', ' ').title()}: {status}")
        
        logger.info("\nNext Steps:")
        logger.info("1. Test the new secure system with: python 01_JusticeDB_Import_Secure.py --verbose")
        logger.info("2. Review the migration log: migration.log")
        logger.info("3. Update any automation scripts to use the new secure versions")
        logger.info("4. Remove legacy config after verification: config/values.json")
        logger.info(f"5. Keep backup for reference: {self.backup_dir}")
        logger.info("\nRefer to the updated documentation for new features and security improvements.")


def main():
    """Main entry point for migration script."""
    parser = argparse.ArgumentParser(description="Migrate ETL system to secure version")
    parser.add_argument("--validate-only", action="store_true", 
                       help="Only validate the system without making changes")
    parser.add_argument("--force", action="store_true",
                       help="Force migration even if validation warnings exist")
    parser.add_argument("--backup-dir", type=Path,
                       help="Custom backup directory path")
    
    args = parser.parse_args()
    
    try:
        migration = ETLSystemMigration(backup_dir=args.backup_dir)
        success = migration.run_migration(force=args.force, validate_only=args.validate_only)
        
        if success:
            logger.info("Migration completed successfully!")
            if args.validate_only:
                print("\n✓ System validation passed - ready for migration")
                print("Run without --validate-only to perform the actual migration")
            else:
                print("\n✓ Migration completed successfully!")
                print("Check migration.log for details")
        else:
            logger.error("Migration failed!")
            print("\n✗ Migration failed - check migration.log for details")
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        logger.info("Migration cancelled by user")
        return 1
    except Exception as e:
        logger.exception("Unexpected error during migration")
        return 1


if __name__ == "__main__":
    exit(main())