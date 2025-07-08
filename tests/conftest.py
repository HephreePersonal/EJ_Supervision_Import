import sys
import types

# Install lightweight stand-in modules before tests import application code.

def _install_stubs():
    modules = {}
    if "tqdm" not in sys.modules:
        dummy = types.ModuleType("tqdm")
        dummy.tqdm = lambda it, **kw: it
        modules["tqdm"] = dummy
    if "dotenv" not in sys.modules:
        mod = types.ModuleType("dotenv")
        mod.load_dotenv = lambda *a, **k: None
        modules["dotenv"] = mod
    if "sqlalchemy" not in sys.modules:
        sa_mod = types.ModuleType("sqlalchemy")
        sa_mod.create_engine = lambda *a, **k: None
        sa_mod.MetaData = lambda *a, **k: None
        pool_mod = types.ModuleType("pool")
        pool_mod.NullPool = object
        sa_mod.pool = pool_mod
        engine_mod = types.ModuleType("engine")
        engine_mod.Engine = object
        engine_mod.Connection = object
        engine_mod.URL = types.SimpleNamespace(create=lambda *a, **k: None)
        sa_mod.engine = engine_mod
        modules.update({
            "sqlalchemy": sa_mod,
            "sqlalchemy.pool": pool_mod,
            "sqlalchemy.engine": engine_mod,
            "sqlalchemy.types": types.SimpleNamespace(Text=lambda *a, **k: None),
        })
    if "pydantic" not in sys.modules:
        pd_mod = types.ModuleType("pydantic")
        class _BaseSettings:
            def __init__(self, **values):
                for k, v in values.items():
                    setattr(self, k, v)
        pd_mod.BaseSettings = _BaseSettings
        pd_mod.DirectoryPath = str
        pd_mod.Field = lambda *a, **k: None
        class _SecretStr(str):
            def get_secret_value(self):
                return str(self)
        pd_mod.SecretStr = _SecretStr
        pd_mod.validator = lambda *a, **k: (lambda f: f)
        modules["pydantic"] = pd_mod
        ps_mod = types.ModuleType("pydantic_settings")
        ps_mod.BaseSettings = _BaseSettings
        modules["pydantic_settings"] = ps_mod
    if "pyodbc" not in sys.modules:
        class _DummyError(Exception):
            pass
        modules["pyodbc"] = types.SimpleNamespace(Error=_DummyError, connect=lambda *a, **k: None)
    if "mysql" not in sys.modules:
        dummy_mysql = types.ModuleType("mysql")
        dummy_mysql.connector = types.SimpleNamespace(connect=lambda **k: None)
        modules["mysql"] = dummy_mysql
        modules["mysql.connector"] = dummy_mysql.connector
    if "keyring" not in sys.modules:
        modules["keyring"] = types.ModuleType("keyring")
    if "pandas" not in sys.modules:
        modules["pandas"] = types.ModuleType("pandas")
    if "tkinter" not in sys.modules:
        tk = types.ModuleType("tkinter")
        tk.Tk = object
        tk.Label = object
        tk.Entry = object
        tk.Button = object
        tk.Checkbutton = object
        tk.Frame = object
        tk.BooleanVar = object
        tk.StringVar = object
        tk.scrolledtext = types.SimpleNamespace(ScrolledText=object)
        tk.messagebox = types.SimpleNamespace(
            showerror=lambda *a, **k: None,
            showinfo=lambda *a, **k: None,
            askyesno=lambda *a, **k: True,
        )
        tk.filedialog = types.SimpleNamespace(askdirectory=lambda *a, **k: "")
        tk.END = None
        tk.WORD = None
        tk.LEFT = None
        tk.DISABLED = "disabled"
        modules.update({
            "tkinter": tk,
            "tkinter.messagebox": tk.messagebox,
            "tkinter.scrolledtext": tk.scrolledtext,
            "tkinter.filedialog": tk.filedialog,
        })
    for name, mod in modules.items():
        sys.modules.setdefault(name, mod)


def pytest_configure(config):
    _install_stubs()
