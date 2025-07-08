# PyInstaller hook for pyodbc
# Save this file in a 'hooks' directory in your project root

from PyInstaller.utils.hooks import collect_dynamic_libs

# Collect the pyodbc dynamic libraries
binaries = collect_dynamic_libs('pyodbc')
