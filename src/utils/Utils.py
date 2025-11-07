import os
import platform
import zipfile
from pathlib import Path
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
        from Utils.StandaloneUtils import custom_print
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