import os
import platform
from colorama import Fore

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