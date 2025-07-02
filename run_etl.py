
"""Graphical wrapper to run the ETL scripts sequentially.

This module provides a small Tk based application that builds a SQL Server
connection string and launches each of the ETL scripts.  It can also be used in
headless mode by calling :func:`run_sequential_etl` with an environment
dictionary.  The UI exposes the most common configuration options such as the
CSV directory and whether empty tables should be processed.
"""

import os
import sys
import json
from importlib import resources
from pathlib import Path
import logging
from utils.logging_helper import setup_logging, operation_counts
logger = logging.getLogger(__name__)
import tkinter as tk
from tkinter import messagebox, scrolledtext, filedialog
import pyodbc
import queue
from datetime import datetime

from etl.runner import (
    SCRIPTS,
    run_sequential_etl,
    run_script,
)

# Use importlib.resources so the config file can be bundled inside an executable
CONFIG_FILE = str(resources.files("config").joinpath("values.json"))
# Add this code to run_etl.py to make it work with our new modular structure


class App(tk.Tk):
    def __init__(self):
        """Initialize the main application window and start queue processing."""
        super().__init__()
        self.title("EJ Supervision Importer")
        self.resizable(True, True)
        self.minsize(900, 700)  # Increased height for better visibility
        self.conn_str = None
        self.csv_dir = ""
        self.config_values = self._load_config()
        self._create_connection_widgets()
        self.status_labels = {}
        self.current_runner = None
        self.update_queue = queue.Queue()
        self.status_queue = queue.Queue()
        
        # Start the queue processing
        self._process_queues()
        
        # Schedule automatic output clearing (5 minutes = 300,000 ms)
        self._schedule_auto_clear()
    
    def _schedule_auto_clear(self):
        """Schedule automatic clearing of output every 5 minutes"""
        self.after(300000, self._auto_clear)
    
    def _auto_clear(self):
        """Automatically clear the output and reschedule"""
        if hasattr(self, "output_text"):
            self.clear_output()
            self.output_text.insert(tk.END, "[AUTO] Output automatically cleared (5-minute interval)\n\n")
        # Reschedule for next time
        self._schedule_auto_clear()
    
    def _load_config(self):
        """Load configuration from JSON file if it exists"""
        try:
            config_path = Path(CONFIG_FILE)
            if config_path.is_file():
                with config_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    data.setdefault("password", "")
                    data.setdefault("always_include_tables", [])
                    return data
        except Exception as e:
            logger.error(f"Error loading config: {e}")
        return {
            "driver": "",
            "server": "",
            "database": "",
            "user": "",
            "password": "",
            "csv_dir": "",
            "include_empty_tables": False,
            "always_include_tables": []
        }
    
    def _save_config(self):
        """Save current configuration to JSON file"""
        config = App._load_config(self)
        config.update({
            "driver": self.entries["driver"].get(),
            "server": self.entries["server"].get(),
            "database": self.entries["database"].get(),
            "user": self.entries["user"].get(),
            "csv_dir": self.csv_dir_var.get(),
            "include_empty_tables": self.include_empty_var.get(),
        })
        
        try:
            config_path = Path(CONFIG_FILE)
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with config_path.open("w", encoding="utf-8") as f:
                json.dump(config, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving config: {e}")
    
    def _create_connection_widgets(self):
        """Create entry fields for connection parameters and CSV directory."""
        fields = ["Driver", "Server", "Database", "User", "Password"]
        self.entries = {}
        for i, field in enumerate(fields):
            lbl = tk.Label(self, text=field+":")
            lbl.grid(row=i, column=0, sticky="e", padx=5, pady=2)
            ent = tk.Entry(self, width=60)
            if field.lower() == "password":
                ent.config(show="*")
            # Pre-populate with config values if available
            field_key = field.lower()
            if field_key in self.config_values and self.config_values[field_key]:
                ent.insert(0, self.config_values[field_key])
            ent.grid(row=i, column=1, padx=5, pady=2)
            self.entries[field.lower()] = ent

        row = len(fields)
        lbl = tk.Label(self, text="CSV Directory:")
        lbl.grid(row=row, column=0, sticky="e", padx=5, pady=2)
        self.csv_dir_var = tk.StringVar()
        if "csv_dir" in self.config_values:
            self.csv_dir_var.set(self.config_values["csv_dir"])
        ent = tk.Entry(self, textvariable=self.csv_dir_var, width=40)
        ent.grid(row=row, column=1, padx=5, pady=2)
        browse_btn = tk.Button(self, text="Browse", command=self._browse_csv_dir)
        browse_btn.grid(row=row, column=2, padx=5, pady=2)

        # checkbox to include empty tables
        self.include_empty_var = tk.BooleanVar(value=self.config_values.get("include_empty_tables", False))
        chk = tk.Checkbutton(self, text="Include empty tables", variable=self.include_empty_var)
        chk.grid(row=row+1, column=0, columnspan=2, pady=(5, 0))

        test_btn = tk.Button(self, text="Test Connection", command=self.test_connection)
        test_btn.grid(row=row+2, column=0, columnspan=2, pady=10)
    
    def _browse_csv_dir(self):
        """Open a directory chooser dialog and store the selected path."""
        directory = filedialog.askdirectory()
        if directory:
            self.csv_dir_var.set(directory)
    
    def _show_script_widgets(self):
        """Create buttons and status labels for each ETL script."""
        if hasattr(self, "script_frame"):
            return

        self.script_frame = tk.Frame(self)
        start_row = len(self.entries) + 3
        self.script_frame.grid(row=start_row, column=0, columnspan=3, sticky="nsew")
        
        # Configure row and column weights to allow expansion
        self.grid_rowconfigure(start_row, weight=1)
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)
        self.grid_columnconfigure(2, weight=1)

        # Add column headers
        tk.Label(self.script_frame, text="Script", font=("Arial", 10, "bold")).grid(row=0, column=0, sticky="w", padx=5, pady=2)
        tk.Label(self.script_frame, text="Action", font=("Arial", 10, "bold")).grid(row=0, column=1, sticky="w", padx=5, pady=2)
        tk.Label(self.script_frame, text="Current Status", font=("Arial", 10, "bold")).grid(row=0, column=2, sticky="w", padx=5, pady=2)

        self.run_buttons = {}
        for idx, (label, path) in enumerate(SCRIPTS, 1):
            tk.Label(self.script_frame, text=path).grid(row=idx, column=0, sticky="w", padx=5, pady=2)
            
            # Store button reference so we can disable/enable it
            btn = tk.Button(
                self.script_frame,
                text="Run",
                command=lambda p=path: self.run_script(p)
            )
            btn.grid(row=idx, column=1, padx=5, pady=2)
            self.run_buttons[path] = btn
            
            # Add status label for current status
            status_var = tk.StringVar(value="Not started")
            status_lbl = tk.Label(self.script_frame, textvariable=status_var, 
                                 width=50, anchor="w", bg="#f0f0f0")
            status_lbl.grid(row=idx, column=2, sticky="w", padx=5, pady=2)
            self.status_labels[path] = status_var
            
        # Configure grid for output text to expand
        self.script_frame.grid_rowconfigure(len(SCRIPTS)+1, weight=1)
        self.script_frame.grid_columnconfigure(0, weight=1)
        self.script_frame.grid_columnconfigure(1, weight=1)
        self.script_frame.grid_columnconfigure(2, weight=1)

        # Create output text area with auto-scroll checkbox
        output_frame = tk.Frame(self.script_frame)
        output_frame.grid(row=len(SCRIPTS)+1, column=0, columnspan=3, sticky="nsew", pady=(10, 0))
        output_frame.grid_rowconfigure(0, weight=1)
        output_frame.grid_columnconfigure(0, weight=1)
        
        # Add auto-scroll checkbox
        control_frame = tk.Frame(output_frame)
        control_frame.grid(row=0, column=0, sticky="ew")
        
        self.auto_scroll_var = tk.BooleanVar(value=True)
        tk.Checkbutton(control_frame, text="Auto-scroll output", variable=self.auto_scroll_var).pack(side=tk.LEFT, padx=5)
        
        # Add clear button
        tk.Button(control_frame, text="Clear Output", command=self.clear_output).pack(side=tk.LEFT, padx=5)
        
        # Create scrolled text widget
        self.output_text = scrolledtext.ScrolledText(output_frame, width=120, height=30, wrap=tk.WORD)
        self.output_text.grid(row=1, column=0, sticky="nsew")
        
        # Add timestamp to output
        self.output_text.insert(tk.END, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Ready to run scripts.\n\n")
    
    def _build_conn_str(self):
        """Assemble a SQL Server ODBC connection string from the entry values."""
        driver = self.entries["driver"].get() or "{ODBC Driver 17 for SQL Server}"
        server = self.entries["server"].get()
        database = self.entries["database"].get()
        user = self.entries["user"].get()
        password = self.entries["password"].get()

        parts = [f"DRIVER={driver}", f"SERVER={server}"]
        if database:
            parts.append(f"DATABASE={database}")
        if user:
            parts.append(f"UID={user}")
        if password:
            parts.append(f"PWD={password}")
        return ";".join(parts)
    
    def test_connection(self):
        """Validate the connection details entered by the user."""
        conn_str = self._build_conn_str()
        if not conn_str:
            messagebox.showerror("Error", "Please provide connection details")
            return
        try:
            pyodbc.connect(conn_str, timeout=5)
        except Exception as exc:
            messagebox.showerror("Connection Failed", str(exc))
            return

        messagebox.showinfo("Success", "Connection successful!")
        self.conn_str = conn_str
        os.environ["MSSQL_TARGET_CONN_STR"] = conn_str
        db_name = self.entries["database"].get()
        if db_name:
            os.environ["MSSQL_TARGET_DB_NAME"] = db_name
        self.csv_dir = self.csv_dir_var.get()
        if self.csv_dir:
            os.environ["EJ_CSV_DIR"] = self.csv_dir
        
        # Save current configuration
        self._save_config()
        
        self._show_script_widgets()
    
    def clear_output(self):
        """Clear the output text area."""
        self.output_text.delete(1.0, tk.END)
        self.output_text.insert(tk.END, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Output cleared.\n\n")
    
    def run_script(self, path):
        """Launch the selected ETL script in a background thread."""
        if not self.conn_str:
            messagebox.showerror("Error", "Please test the connection first")
            return
        
        # Disable all run buttons while a script is running
        for btn in self.run_buttons.values():
            btn.config(state=tk.DISABLED)
        
        # Reset status
        self.status_labels[path].set("Starting...")
        self.output_text.insert(tk.END, f"\n[{datetime.now().strftime('%H:%M:%S')}] Starting {path}...\n")
        if self.auto_scroll_var.get():
            self.output_text.see(tk.END)
        
        # Set up environment
        os.environ["INCLUDE_EMPTY_TABLES"] = "1" if self.include_empty_var.get() else "0"
        
        my_env = os.environ.copy()
        my_env["PYTHONUNBUFFERED"] = "1"
        my_env["PYTHONIOENCODING"] = "utf-8"

        progress_file = os.path.join(
            os.environ.get("EJ_LOG_DIR", ""),
            os.path.splitext(os.path.basename(path))[0] + ".progress.json",
        )

        resume = False
        if os.path.exists(progress_file):
            resume = messagebox.askyesno(
                "Resume Migration",
                "A previous migration was interrupted. Resume from last progress?",
            )
            if not resume:
                try:
                    os.remove(progress_file)
                except OSError:
                    pass

        my_env["PROGRESS_FILE"] = progress_file
        my_env["RESUME"] = "1" if resume else "0"
        
        # Stop any existing runner
        if self.current_runner and self.current_runner.is_alive():
            self.current_runner.stop()
            self.current_runner.join(timeout=5)

        # Create and start new runner thread via orchestration module
        self.current_runner = run_script(
            path,
            my_env,
            self.update_queue,
            self.status_queue,
        )
        
    
    def _process_queues(self):
        """Process updates from the runner threads."""
        try:
            # Process output queue
            while True:
                try:
                    msg_type, content = self.update_queue.get_nowait()
                    
                    if msg_type == "output":
                        self.output_text.insert(tk.END, content)
                        if self.auto_scroll_var.get():
                            self.output_text.see(tk.END)
                    elif msg_type == "done":
                        # Re-enable all buttons
                        for btn in self.run_buttons.values():
                            btn.config(state=tk.NORMAL)
                        break
                        
                except queue.Empty:
                    break
            
            # Process status queue
            while True:
                try:
                    path, status = self.status_queue.get_nowait()
                    if path in self.status_labels:
                        self.status_labels[path].set(status)
                except queue.Empty:
                    break
                    
        except Exception as e:
            logger.error(f"Error processing queues: {e}")
        
        # Schedule next check
        self.after(50, self._process_queues)  # Check every 50ms for responsive UI
    
    def destroy(self):
        """Clean up when closing the application."""
        # Stop any running threads
        if self.current_runner and self.current_runner.is_alive():
            self.current_runner.stop()
            self.current_runner.join(timeout=2)
        super().destroy()

if __name__ == "__main__":
    setup_logging()
    app = App()
    app.mainloop()
