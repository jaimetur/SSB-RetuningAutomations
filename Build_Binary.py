# ------------------------------------------------------------
# Add 'src/' folder to sys.path to import any module from 'src/'.
import os, sys
current_dir = os.path.dirname(__file__)
src_path = os.path.abspath(os.path.join(current_dir, "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ------------------------------------------------------------

import glob
import platform
import shutil
import subprocess
import tempfile
import zipfile
from colorama import Fore
from pathlib import Path

from SSB_RetuningAutomations import TOOL_NAME, TOOL_VERSION, COPYRIGHT_TEXT

global OPERATING_SYSTEM
global ARCHITECTURE
global TOOL_SOURCE_NAME
global TOOL_VERSION_WITHOUT_V
global TOOL_NAME_VERSION
global root_dir
global tool_name_with_version_os_arch
global script_zip_file
global archive_path_relative

# ---------------------------- GLOBAL VARIABLES ----------------------------
COMPILE_IN_ONE_FILE = True

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
# --------------------------------------------------------------------------


# ---------------------------------- HELPERS -------------------------------
def _clear_screen():
    os.system('clear' if os.name == 'posix' else 'cls')


def _get_os(step_name=""):
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


def _get_arch(step_name=""):
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


def _print_arguments_pretty(arguments, title="Arguments", step_name="", use_custom_print=True):
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


def _zip_folder(temp_dir, output_file):
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

def _include_extrafiles_and_zip(input_file, output_file):
    extra_files_to_subdir = [
        {
            'subdir': 'assets/logos',  # These files go into the 'assets/logos' subdirectory
            # 'files': ["./assets/logos/logo.png"]
            'files': ["./assets/logos/logo_02*.png"]
        },
        {
            'subdir': 'docs',  # These files go into the 'docs' subdirectory
            'files': ["./README.md", "./CHANGELOG.md", "./ROADMAP.md", "./DOWNLOAD.md", "./CONTRIBUTING.md", "./CODE_OF_CONDUCT.md", "./LICENSE"]
        },
        {
            'subdir': 'help',  # These files go into the 'help' subdirectory
            'files': ["./help/*.pptx"]
        },
    ]
    if not input_file or not output_file:
        print("Usage: _include_extrafiles_and_zip(input_file, output_file)")
        sys.exit(1)
    if not Path(input_file).is_file():
        print(f"ERROR   : The input file '{input_file}' does not exists.")
        sys.exit(1)
    temp_dir = Path(tempfile.mkdtemp())
    tool_version_dir = os.path.join(temp_dir, TOOL_NAME_VERSION)
    print(tool_version_dir)
    os.makedirs(tool_version_dir, exist_ok=True)
    shutil.copy(input_file, tool_version_dir)

    # Now copy the extra files
    for subdirs_dic in extra_files_to_subdir:
        subdir = subdirs_dic.get('subdir', '')  # If 'subdir' is empty, it will copy into the root directory
        files = subdirs_dic.get('files', [])  # Ensure we always have a list
        subdir_path = os.path.join(tool_version_dir, subdir) if subdir else tool_version_dir
        os.makedirs(subdir_path, exist_ok=True)  # Create the folder if it does not exist
        for file_pattern in files:
            # Convert the relative path pattern into an absolute path
            absolute_pattern = os.path.abspath(file_pattern)
            # Find files matching the pattern
            matched_files = glob.glob(absolute_pattern)
            # If no files were found and the path is a valid file, treat it as such
            if not matched_files and os.path.isfile(absolute_pattern):
                matched_files = [absolute_pattern]
            # Copy matched files into the destination directory
            for file in matched_files:
                shutil.copy(file, subdir_path)
    # Zip the temporary directory and then remove it
    _zip_folder(temp_dir, output_file)
    shutil.rmtree(temp_dir)


def _get_tool_version(file):
    if not Path(file).is_file():
        print(f"ERROR   : The file {file} does not exists.")
        return None
    with open(file, 'r') as f:
        for line in f:
            if line.startswith("TOOL_VERSION"):
                return line.split('"')[1]
    print("ERROR   : Not found any value between colons after TOOL_VERSION.")
    return None


def _get_clean_version(version: str):
    # Remove the leading 'v' if present
    clean_version = version.lstrip('v')
    return clean_version


def _extract_release_body(input_file, output_file, download_file):
    """Extracts two specific sections from the changelog file, modifies a header, and appends them along with additional content from another file."""
    # Open the file and read its content into a list
    with open(input_file, 'r', encoding='utf-8') as infile:
        lines = infile.readlines()
    # Initialize key indices and counter
    changelog_index = None
    second_release_index = None
    release_count = 0
    # Loop through lines to find the start of the "Changelog" section and locate the second occurrence of "## Release"
    for i, line in enumerate(lines):
        if line.strip() == "# 🗓️ CHANGELOG":
            changelog_index = i
            # lines[i] = lines[i].replace("# 🗓️ CHANGELOG", "# 🗓️ Changelog")
        if "## Release:" in line:
            release_count += 1
            if release_count == 2:
                second_release_index = i
                break
    # Validate that the changelog section exists
    if changelog_index is None:
        print("Required sections not found in the file.")
        return
    # Extract content from "## Changelog:" to the second "## Release"
    if second_release_index is not None:
        release_section = lines[changelog_index:second_release_index]
    else:
        release_section = lines[changelog_index:]
    # Read content of download_file
    with open(download_file, 'r', encoding='utf-8') as df:
        download_content = df.readlines()
    # Append both the download file content and the release section to the output file
    # If the output file already exists, remove it
    if os.path.exists(output_file):
        os.remove(output_file)
    with open(output_file, 'a', encoding='utf-8') as outfile:
        outfile.writelines(release_section)
        outfile.writelines(download_content)
# ------------------------ END OF HELPERS ---------------------------------


def main(compiler='pyinstaller', compile_in_one_file=COMPILE_IN_ONE_FILE):
    # =======================
    # Create global variables
    # =======================
    global OPERATING_SYSTEM
    global ARCHITECTURE
    global TOOL_SOURCE_NAME
    global TOOL_VERSION_WITHOUT_V
    global TOOL_NAME_VERSION
    global root_dir
    global tool_name_with_version_os_arch
    global script_zip_file
    global archive_path_relative

    # Detect operating system and architecture
    OPERATING_SYSTEM = _get_os()
    ARCHITECTURE = _get_arch()

    # Script Names
    TOOL_SOURCE_NAME = f"{TOOL_NAME}.py"
    TOOL_VERSION_WITHOUT_V = _get_clean_version(TOOL_VERSION)
    TOOL_NAME_VERSION = f"{TOOL_NAME}_v{TOOL_VERSION}"

    # Get working directory
    root_dir = os.getcwd()
    # Get the root directory one level above the working directory
    # root_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

    # Compute relative path
    tool_name_with_version_os_arch = f"{TOOL_NAME_VERSION}_{OPERATING_SYSTEM}_{ARCHITECTURE}"
    script_zip_file = Path(f"./SSB_RetuningAutomations-builds/{TOOL_VERSION_WITHOUT_V}/{tool_name_with_version_os_arch}.zip").resolve()
    archive_path_relative = os.path.relpath(script_zip_file, root_dir)
    # ========================
    # End of global variables
    # ========================

    _clear_screen()
    print("")
    print("=================================================================================================")
    print(f"INFO:    Running Main Module - main(compiler={compiler}, compile_in_one_file={compile_in_one_file})...")
    print("=================================================================================================")
    print("")

    # print("Adding neccesary packets to Python environment before to compile...")
    # subprocess.run([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'])
    # subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', './requirements.txt'])
    # if OPERATING_SYSTEM == 'windows' and ARCHITECTURE == 'x64)':
    #     subprocess.run([sys.executable, '-m', 'pip', 'install', 'windows-curses'])
    # print("")

    if TOOL_VERSION:
        print(f"TOOL_VERSION found: {TOOL_VERSION_WITHOUT_V}")
    else:
        print("Caanot find TOOL_VERSION.")

    # Extract the body of RELEASE-NOTES
    print("Extracting body of RELEASE-NOTES...")

    # Paths for CHANGELOG.md, RELEASE-NOTES.md and DOWNLOAD.md
    download_filepath = os.path.join(root_dir, 'DOWNLOAD.md')
    changelog_filepath = os.path.join(root_dir, 'CHANGELOG.md')
    current_release_filepath = os.path.join(root_dir, 'RELEASE-NOTES.md')

    # Extract the body of the current release from CHANGELOG.md
    _extract_release_body(input_file=changelog_filepath, output_file=current_release_filepath, download_file=download_filepath)
    print(f"File '{current_release_filepath}' created successfully!.")

    # Save build_info.txt
    with open(os.path.join(root_dir, 'build_info.txt'), 'w') as file:
        file.write('OPERATING_SYSTEM=' + OPERATING_SYSTEM + '\n')
        file.write('ARCHITECTURE=' + ARCHITECTURE + '\n')
        file.write('TOOL_NAME=' + TOOL_NAME + '\n')
        file.write('TOOL_VERSION=' + TOOL_VERSION_WITHOUT_V + '\n')
        file.write('ROOT_PATH=' + root_dir + '\n')
        file.write('ARCHIVE_PATH=' + archive_path_relative + '\n')
        print('')
        print(f'OPERATING_SYSTEM: {OPERATING_SYSTEM}')
        print(f'ARCHITECTURE: {ARCHITECTURE}')
        print(f'TOOL_NAME: {TOOL_NAME}')
        print(f'TOOL_VERSION: {TOOL_VERSION_WITHOUT_V}')
        print(f'ROOT_PATH: {root_dir}')
        print(f'ARCHIVE_PATH: {archive_path_relative}')

    ok = True
    # Run compilation
    if compiler:
        ok = compile(compiler=compiler, compile_in_one_file=compile_in_one_file)
    return ok


def compile(compiler='pyinstaller', compile_in_one_file=COMPILE_IN_ONE_FILE):
    global OPERATING_SYSTEM
    global ARCHITECTURE
    global TOOL_SOURCE_NAME
    global TOOL_VERSION_WITHOUT_V
    global TOOL_NAME_VERSION
    global root_dir
    global tool_name_with_version_os_arch
    global script_zip_file
    global archive_path_relative

    # Initialize variables
    TOOL_NAME_WITH_VERSION_OS_ARCH = f"{TOOL_NAME_VERSION}_{OPERATING_SYSTEM}_{ARCHITECTURE}"
    splash_image = "assets/logos/logo_02.png"  # Splash image for Windows

    if OPERATING_SYSTEM == 'windows':
        script_compiled = f'{TOOL_NAME}.exe'
        script_compiled_with_version_os_arch_extension = f"{TOOL_NAME_WITH_VERSION_OS_ARCH}.exe"
    else:
        if compiler == 'pyinstaller':
            script_compiled = f'{TOOL_NAME}'
        else:
            script_compiled = f'{TOOL_NAME}.bin'
        script_compiled_with_version_os_arch_extension = f"{TOOL_NAME_WITH_VERSION_OS_ARCH}.run"

    # Append additional info into build_info.txt
    with open(os.path.join(root_dir, 'build_info.txt'), 'a') as file:
        file.write('COMPILER=' + str(compiler) + '\n')
        file.write('SCRIPT_COMPILED=' + os.path.abspath(script_compiled_with_version_os_arch_extension) + '\n')
        print('')
        print(f'COMPILER: {compiler}')
        print(f'COMPILE_IN_ONE_FILE: {compile_in_one_file}')
        print(f'SCRIPT_COMPILED: {script_compiled}')

    print("")
    print("=================================================================================================")
    print(f"INFO:    Compiling with '{compiler}' for OS: '{OPERATING_SYSTEM}' and architecture: '{ARCHITECTURE}'...")
    print("=================================================================================================")
    print("")

    success = False

    # ===============================================================================================================================================
    # COMPILE WITH PYINSTALLER...
    # ===============================================================================================================================================
    if compiler == 'pyinstaller':
        print("Compiling with Pyinstaller...")
        import PyInstaller.__main__

        # Build and Dist folders for PyInstaller
        build_path = "./pyinstaller_build"
        dist_path = "./pyinstaller_dist"

        # Remove temporary files and directories from previous compilations
        print("Removing temporary files from previous compilations...")
        Path(f"{TOOL_NAME}.spec").unlink(missing_ok=True)
        shutil.rmtree(build_path, ignore_errors=True)
        shutil.rmtree(dist_path, ignore_errors=True)
        print("")

        # Prepare PyInstaller command
        pyinstaller_command = ['./src/' + TOOL_SOURCE_NAME]

        # Mode onefile or standalone
        if compile_in_one_file:
            pyinstaller_command.extend(["--onefile"])
        else:
            pyinstaller_command.extend(['--onedir'])

        # Add splash image to .exe file (only supported on Windows)
        if OPERATING_SYSTEM == 'windows':
            pyinstaller_command.extend(("--splash", splash_image))

        # Add generic arguments to PyInstaller
        pyinstaller_command.extend(["--noconfirm"])
        pyinstaller_command.extend(("--distpath", dist_path))
        pyinstaller_command.extend(("--workpath", build_path))

        # On Linux set runtime tmp dir to /var/tmp for Synology compatibility (/tmp may not have permissions on Synology NAS)
        if OPERATING_SYSTEM == 'linux':
            pyinstaller_command.extend(("--runtime-tmpdir", '/var/tmp'))

        # Now run PyInstaller with previous settings
        _print_arguments_pretty(pyinstaller_command, title="Pyinstaller Arguments", use_custom_print=False)

        try:
            PyInstaller.__main__.run(pyinstaller_command)
            print("[OK] PyInstaller finished successfully.")
            success = True
        except SystemExit as e:
            if e.code == 0:
                print("[OK] PyInstaller finished successfully.")
                success = True
            else:
                print(f"[ERROR] PyInstaller failed with error code: {e.code}")

    # ===============================================================================================================================================
    # COMPILE WITH NUITKA...
    # ===============================================================================================================================================
    elif compiler == 'nuitka':
        print("Compiling with Nuitka...")

        # Build and Dist folders for Nuitka
        dist_path = "./nuitka_dist"
        build_path = f"{dist_path}/{TOOL_NAME}.build"

        # Remove temporary files and directories from previous compilations
        print("Removing temporary files from previous compilations...")
        Path(f"{TOOL_NAME}.spec").unlink(missing_ok=True)
        shutil.rmtree(build_path, ignore_errors=True)
        shutil.rmtree(dist_path, ignore_errors=True)
        print("")

        # Prepare Nuitka command
        nuitka_command = [sys.executable, '-m', 'nuitka', './src/' + TOOL_SOURCE_NAME]

        # Mode onefile or standalone
        if compile_in_one_file:
            nuitka_command.extend(['--onefile'])
            nuitka_command.append('--onefile-no-compression')
            if OPERATING_SYSTEM == 'windows':
                nuitka_command.extend([f'--onefile-windows-splash-screen-image={splash_image}'])
        else:
            nuitka_command.extend(['--standalone'])

        # Add generic arguments to Nuitka
        nuitka_command.extend([
            '--jobs=4',
            '--assume-yes-for-downloads',
            '--enable-plugin=tk-inter',
            '--disable-cache=ccache',
            '--lto=yes',
            '--nofollow-imports',
            '--nofollow-import-to=unused_module',

            # '--remove-output',
            f'--output-dir={dist_path}',

            # f'--windows-icon-from-ico=./assets/ico/SSB_RetuningAutomations.ico',
            f'--copyright={COPYRIGHT_TEXT}',
            f"--company-name={TOOL_NAME}",
            f"--product-name={TOOL_NAME}",
            f"--file-description={TOOL_NAME_VERSION} by Jaime Tur",
            f"--file-version={TOOL_VERSION_WITHOUT_V.split('-')[0]}",
            f"--product-version={TOOL_VERSION_WITHOUT_V.split('-')[0]}",

        ])

        # Force-include the tableauhyperapi/bin directory when available (helps with hyperd and native libs)
        try:
            import tableauhyperapi as _tha
            tha_pkg_dir = Path(_tha.__file__).resolve().parent
            bin_dir = tha_pkg_dir / "bin"
            if bin_dir.exists():
                # Nuitka: --include-data-dir=SOURCE=DESTINATION
                nuitka_command.extend([f'--include-data-dir={str(bin_dir)}=tableauhyperapi/bin'])
        except Exception as e:
            print(f"[WARN] Could not auto-detect tableauhyperapi paths to include binaries for Nuitka: {e}")

        # Set runtime tmp dir to a specific folder within /var/tmp or %TEMP% to reduce antivirus detection probability
        if OPERATING_SYSTEM != 'windows':
            # On Linux set runtime tmp dir to /var/tmp for Synology compatibility (/tmp may not have permissions on Synology NAS)
            nuitka_command.extend([f'--onefile-tempdir-spec=/var/tmp/{TOOL_NAME_WITH_VERSION_OS_ARCH}'])
        else:
            nuitka_command.extend([rf'--onefile-tempdir-spec=%TEMP%\{TOOL_NAME_WITH_VERSION_OS_ARCH}'])

        # Now run Nuitka with previous settings
        _print_arguments_pretty(nuitka_command, title="Nuitka Arguments", use_custom_print=False)
        result = subprocess.run(nuitka_command)
        success = (result.returncode == 0)
        if not success:
            print(f"[ERROR] Nuitka failed with code: {result.returncode}")

    else:
        print(f"Compiler '{compiler}' not supported. Valid options are 'pyinstaller' or 'nuitka'. Compilation skipped.")
        return success

    # ===============================================================================================================================================
    # PACKAGING AND CLEANING ACTIONS...
    # ===============================================================================================================================================
    # Check if compilation finished successfully, otherwise exit
    if success:
        print("[OK] Compilation process finished successfully.")
    else:
        print("[ERROR] There was some error during compilation process.")
        return success

    # Compiled script absolute path
    script_compiled_abs_path = ''
    if compiler == 'pyinstaller':
        script_compiled_abs_path = os.path.abspath(f"{dist_path}/{script_compiled}")
    elif compiler == 'nuitka':
        script_compiled_abs_path = os.path.abspath(f"{dist_path}/{TOOL_NAME}.dist/{script_compiled}")

    # Move the compiled script to the parent folder
    if compile_in_one_file:
        print('')
        print(f"Moving compiled script '{script_compiled_with_version_os_arch_extension}'...")
        shutil.move(f'{dist_path}/{script_compiled}', f'./{script_compiled_with_version_os_arch_extension}')
        # Zip the compiled script together with the extra files/directories
        _include_extrafiles_and_zip(f'./{script_compiled_with_version_os_arch_extension}', script_zip_file)
        script_compiled_abs_path = os.path.abspath(script_compiled_with_version_os_arch_extension)

    # Delete temporary files and folders created during compilation
    print('')
    print("Deleting temporary compilation files...")
    Path(f"{TOOL_NAME}.spec").unlink(missing_ok=True)
    Path(f"nuitka-crash-report.xml").unlink(missing_ok=True)
    shutil.rmtree(build_path, ignore_errors=True)
    if compile_in_one_file:
        shutil.rmtree(dist_path, ignore_errors=True)
    print("Temporary compilation files successfully deleted!")

    print('')
    print("=================================================================================================")
    print(f"Compilation for OS: '{OPERATING_SYSTEM}' and architecture: '{ARCHITECTURE}' completed successfully!!!")
    print('')
    print(f"SCRIPT_COMPILED: {script_compiled_abs_path}")
    print(f"SCRIPT_ZIPPED  : {script_zip_file}")
    print('')
    print("All compilations have finished successfully.")
    print("=================================================================================================")
    print('')
    return success


if __name__ == "__main__":
    # Read CLI arguments (if any)
    arg1 = sys.argv[1] if len(sys.argv) > 1 else None
    arg2 = sys.argv[2] if len(sys.argv) > 2 else None

    # Parse compiler argument
    if arg1 is not None:
        arg_lower = arg1.lower()
        if arg_lower in ['false', '-false', '--false', '0', 'no', 'n', 'none', '-none', '--none',
                         'no-compile', '-no-compile', '--no-compile', 'no-compiler', '-no-compiler', '--no-compiler']:
            compiler = None
        elif arg_lower in ['pyinstaller', '-pyinstaller', '--pyinstaller']:
            compiler = 'pyinstaller'
        elif arg_lower in ['nuitka', '-nuitka', '--nuitka']:
            compiler = 'nuitka'
        else:
            print(f"Unrecognized compiler: '{arg1}'. Using 'PyInstaller' by default...")
            compiler = 'pyinstaller'
    else:
        compiler = False  # Default value

    # Parse onefile/onedir argument
    if arg2 is not None:
        arg_lower = arg2.lower()
        if arg_lower in ['false', '-false', '--false', '0', 'no', 'n', 'none', '-none', '--none',
                         'onedir', '-onedir', '--onedir', 'standalone', '-standalone', '--standalone',
                         'no-onefile', '-no-onefile', '--no-onefile']:
            onefile = False
        else:
            onefile = True
    else:
        onefile = True  # Default value

    ok = main(compiler=compiler, compile_in_one_file=onefile)
    if ok:
        print('INFO    : COMPILATION FINISHED SUCCESSFULLY!')
        sys.exit(0)
    else:
        print('ERROR   : BUILD FINISHED WITH ERRORS!')
        sys.exit(-1)
