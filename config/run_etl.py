def _load_config(self):
    """Load configuration using the simplified system"""
    try:
        # Load directly from the JSON file
        file_config = load_config_from_file()
        
        # Start with defaults
        config = {
            "driver": "{ODBC Driver 17 for SQL Server}",
            "server": "",
            "database": "",
            "user": "",
            "password": "",
            "csv_dir": "",
            "log_dir": "",
            "include_empty_tables": False,
            "always_include_tables": []
        }
        
        # Update with file config values
        if file_config:
            config.update(file_config)
            # Handle both old and new CSV directory keys
            if "ej_csv_dir" in file_config:
                config["csv_dir"] = file_config["ej_csv_dir"]
    
        # Store always_include_tables as instance attribute
        self.always_include_tables = config.get("always_include_tables", [])
        
        return config
        
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        self.always_include_tables = []
        return {
            "driver": "{ODBC Driver 17 for SQL Server}",
            "server": "",
            "database": "",
            "user": "",
            "password": "",
            "csv_dir": "",
            "log_dir": "",
            "include_empty_tables": False,
            "always_include_tables": []
        }