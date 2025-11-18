# -*- coding: utf-8 -*-

import os
from typing import Optional

class InitialCleanUp:
    """
    Placeholder class. Future: housekeeping tasks over the input_dir.
    """
    def __init__(self):
        pass

    # ----------------------------- API pública ----------------------------- #
    def run(self, input_dir: str, module_name: Optional[str] = "", versioned_suffix: Optional[str] = None) -> str:
        if not os.path.isdir(input_dir):
            raise NotADirectoryError(f"Invalid directory: {input_dir}")
        print(f"{module_name} Working on folder: '{input_dir}'")
        # TODO: Implementar la lógica real
