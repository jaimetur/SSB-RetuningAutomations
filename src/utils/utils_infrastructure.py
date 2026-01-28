import os, sys
import platform
import zipfile
from colorama import Fore
from datetime import datetime
from pathlib import Path
import re


# TAG and TAGS Colored for messages output (in console and log)
MSG_TAGS = {
    'VERBOSE'                   : "VERBOSE : ",
    'DEBUG'                     : "DEBUG   : ",
    'INFO'                      : "INFO    : ",
    'WARNING'                   : "WARNING : ",
    'ERROR'                     : "ERROR   : ",
    'CRITICAL'                  : "CRITICAL: ",
}
MSG_TAGS_COLORED = {
    'VERBOSE'                   : f"{Fore.CYAN}{MSG_TAGS['VERBOSE']}",
    'DEBUG'                     : f"{Fore.LIGHTCYAN_EX}{MSG_TAGS['DEBUG']}",
    'INFO'                      : f"{Fore.LIGHTWHITE_EX}{MSG_TAGS['INFO']}",
    'WARNING'                   : f"{Fore.YELLOW}{MSG_TAGS['WARNING']}",
    'ERROR'                     : f"{Fore.RED}{MSG_TAGS['ERROR']}",
    'CRITICAL'                  : f"{Fore.MAGENTA}{MSG_TAGS['CRITICAL']}",
}

def clear_screen():
    os.system('clear' if os.name == 'posix' else 'cls')

def get_os(step_name=""):
    """Return normalized operating system name (linux, macos, windows)"""
    current_os = platform.system()
    if current_os in ["Linux", "linux"]:
        os_label = "linux"
    elif current_os in ["Darwin", "macOS", "macos"]:
        os_label = "macos"
    elif current_os in ["Windows", "windows", "Win"]:
        os_label = "windows"
    else:
        print(f"{MSG_TAGS['ERROR']}{step_name}Unsupported Operating System: {current_os}")
        os_label = "unknown"
    print(f"{MSG_TAGS['INFO']}{step_name}Detected OS: {os_label}")
    return os_label


def get_arch(step_name=""):
    """Return normalized system architecture (e.g., x64, arm64)"""
    current_arch = platform.machine()
    if current_arch in ["x86_64", "amd64", "AMD64", "X64", "x64"]:
        arch_label = "x64"
    elif current_arch in ["aarch64", "arm64", "ARM64"]:
        arch_label = "arm64"
    else:
        print(f"{MSG_TAGS['ERROR']}{step_name}Unsupported Architecture: {current_arch}")
        arch_label = "unknown"
    print(f"{MSG_TAGS['INFO']}{step_name}Detected architecture: {arch_label}")
    return arch_label

def print_arguments_pretty(arguments, title="Arguments", step_name="", use_custom_print=True):
    """
    Prints a list of command-line arguments in a structured and readable one-line-per-arg format.

    Args:
        :param arguments:
        :param step_name:
        :param title:
        :param use_custom_print:
        :param use_logger:
    """
    print("")
    indent = "    "
    i = 0

    if use_custom_print:
        from utils_infrastructure.StandaloneUtils import custom_print
        custom_print(f"{title}:")
        while i < len(arguments):
            arg = arguments[i]
            if arg.startswith('--') and i + 1 < len(arguments) and not arguments[i + 1].startswith('--'):
                custom_print(f"{step_name}{indent}{arg}={arguments[i + 1]}")
                i += 2
            else:
                custom_print(f"{step_name}{indent}{arg}")
                i += 1
    else:
        pass
        print(f"{MSG_TAGS['INFO']}{title}:")
        while i < len(arguments):
            arg = arguments[i]
            if arg.startswith('--') and i + 1 < len(arguments) and not arguments[i + 1].startswith('--'):
                print(f"{MSG_TAGS['INFO']}{step_name}{indent}{arg}={arguments[i + 1]}")
                i += 2
            else:
                print(f"{MSG_TAGS['INFO']}{step_name}{indent}{arg}")
                i += 1
    print("")

def zip_folder(temp_dir, output_file):
    print(f"Creating packed file: {output_file}...")

    # Convertir output_file a un objeto Path
    output_path = Path(output_file)

    # Crear los directorios padres si no existen
    if not output_path.parent.exists():
        print(f"Creating needed folder for: {output_path.parent}")
        output_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(output_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                file_path = Path(root) / file
                # Añade al zip respetando la estructura de carpetas
                zipf.write(file_path, file_path.relative_to(temp_dir))
            for dir in dirs:
                dir_path = Path(root) / dir
                # Añade directorios vacíos al zip
                if not os.listdir(dir_path):
                    zipf.write(dir_path, dir_path.relative_to(temp_dir))
    print(f"File successfully packed: {output_file}")

def get_resource_path(relative_path: str) -> str:
    """
    Return absolute path to resource, working for:
    - Source execution (normal Python)
    - PyInstaller / Nuitka executables

    When running from source:
        this file is src/utils/utils_infrastructure.py
        project 'src' folder   = parent of this file's directory
        resources are addressed relative to 'src'.

    When frozen:
        base path is the temp extraction folder (PyInstaller onefile)
        or the executable folder (Nuitka / PyInstaller onefolder).
    """
    if getattr(sys, 'frozen', False):
        # First, try next to the executable (your external ppt_templates folder)
        exe_dir = os.path.dirname(sys.executable)
        external_candidate = os.path.join(exe_dir, relative_path)
        if os.path.exists(external_candidate):
            return external_candidate

        # If not found there, fall back to internal bundled resources (PyInstaller _MEIPASS)
        base_path = getattr(sys, '_MEIPASS', exe_dir)
    else:
        # Source: go one level up from src/utils -> src
        utils_dir = os.path.dirname(os.path.abspath(__file__))    # .../src/utils
        base_path = os.path.dirname(utils_dir)                    # .../src

    return os.path.join(base_path, relative_path)

# ============================== LOGGING SYSTEM ============================== #

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[mK]")

def strip_ansi(text: str) -> str:
    """Remove ANSI escape sequences (colors) from a string."""
    if not isinstance(text, str) or not text:
        return text
    return ANSI_ESCAPE_RE.sub("", text)

class LoggerDual:
    """
    Simple dual logger that mirrors stdout prints to both console and a log file.
    Replaces sys.stdout so every print() goes to both outputs automatically.

    Enhancements:
      - Optional extra mirror log files (e.g., also write the same log inside the output folder).
      - Optional auto-flush so the log is updated during execution and not only at program end.
    """

    def __init__(self, log_file_path: str, timestamp_format: str = "%Y-%m-%d %H:%M:%S", tee_to_console: bool = True, enable_color: bool = True, auto_flush: bool = True, mirror_file_paths: list[str] | None = None):
        self.terminal = sys.stdout
        self.log_path = log_file_path
        self.log = open(log_file_path, "a", encoding="utf-8") if log_file_path else None
        self.timestamp_format = timestamp_format
        self.tee_to_console = tee_to_console
        self.enable_color = enable_color
        self.auto_flush = auto_flush
        self._at_line_start = True  # True when next write begins a new log line

        self._mirror_logs: list[tuple[str, object]] = []  # list[(path, file_handle)]
        for p in (mirror_file_paths or []):
            self.add_mirror_file(p)

    def add_mirror_file(self, mirror_path: str) -> bool:
        """Add an additional log file to mirror output into. Returns True if added."""
        if not mirror_path:
            return False

        norm = str(mirror_path)
        for existing_path, _fh in self._mirror_logs:
            if existing_path == norm:
                return False

        try:
            os.makedirs(os.path.dirname(norm), exist_ok=True)
        except Exception:
            pass

        try:
            fh = open(norm, "a", encoding="utf-8")
            self._mirror_logs.append((norm, fh))
            return True
        except Exception:
            return False

    def clear_mirror_files(self) -> None:
        """Close and remove all mirror log files (best-effort)."""
        try:
            for _p, fh in self._mirror_logs:
                try:
                    fh.close()
                except Exception:
                    pass
        except Exception:
            pass
        self._mirror_logs = []

    def _now_prefix(self) -> str:
        """Build a timestamp prefix for the log file."""
        return f"[{datetime.now().strftime(self.timestamp_format)}] "

    def _strip_ansi(self, s: str) -> str:
        # Keep behavior consistent: log files should not contain ANSI color codes.
        return strip_ansi(s)

    def write(self, message: str):
        # Always write raw message to terminal
        if self.tee_to_console and self.terminal is not None:
            self.terminal.write(message)

        if not message:
            return

        clean = self._strip_ansi(message) if self.enable_color else message
        parts = clean.splitlines(keepends=True)

        for part in parts:
            if self._at_line_start:
                prefix = self._now_prefix()
                if self.log is not None:
                    self.log.write(prefix)
                for _p, fh in self._mirror_logs:
                    fh.write(prefix)
                self._at_line_start = False

            if self.log is not None:
                self.log.write(part)
            for _p, fh in self._mirror_logs:
                fh.write(part)

            # If this chunk ends with a newline, next write starts a new line
            if part.endswith("\n"):
                self._at_line_start = True

        if self.auto_flush:
            self.flush()

    def flush(self):
        """Required for compatibility with Python's stdout flush behavior."""
        try:
            if self.terminal is not None:
                self.terminal.flush()
        except Exception:
            pass

        try:
            if self.log is not None:
                self.log.flush()
        except Exception:
            pass

        for _p, fh in self._mirror_logs:
            try:
                fh.flush()
            except Exception:
                pass

    def close(self):
        """Close file handles (best-effort)."""
        try:
            if self.log is not None:
                self.log.close()
        except Exception:
            pass

        for _p, fh in self._mirror_logs:
            try:
                fh.close()
            except Exception:
                pass

