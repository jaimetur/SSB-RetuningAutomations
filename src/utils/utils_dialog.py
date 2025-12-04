# ============================ OPTIONAL TKINTER UI =========================== #
try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
except Exception:
    # Tkinter not available (CLI-only environments)
    tk = None
    ttk = None
    filedialog = None
    messagebox = None

def ask_reopen_launcher() -> bool:
    """
    Ask the user if the launcher should reopen after a module finishes.

    - If Tkinter/messagebox are available, create a *temporary* hidden root
      just for this dialog and destroy it afterwards.
    - Otherwise, fall back to console.
    """
    title = "Finished"
    message = "The selected task has finished.\nDo you want to open the launcher again?"

    # Sin GUI → consola
    if tk is None or messagebox is None:
        return ask_yes_no_dialog(title, message, default=False)

    try:
        # Crear root temporal para este diálogo
        root = tk.Tk()
        root.withdraw()
        try:
            answer = messagebox.askyesno(title, message, parent=root)
            return bool(answer)
        finally:
            # MUY IMPORTANTE: destruir el root temporal
            root.destroy()
    except Exception:
        # Cualquier problema con Tk → fallback a consola
        return ask_yes_no_dialog(title, message, default=False)


def ask_yes_no_dialog(title: str, message: str, default: bool = False) -> bool:
    """
    Ask a Yes/No question using Tkinter (if available) or console as a fallback.
    """
    if messagebox is not None:
        try:
            # Ensure there is a Tk root before showing the dialog (optional, if you need it)
            if tk is not None and tk._default_root is None:
                root = tk.Tk()
                root.withdraw()
            return bool(messagebox.askyesno(title, message))
        except Exception:
            # If something goes wrong with Tkinter, fall back to console
            pass

    try:
        answer = input(f"{title}\n{message} [y/N]: ").strip().lower()
        return answer in ("y", "yes", "s", "si", "sí")
    except Exception:
        return default


def ask_yes_no_dialog_custom(title: str, message: str, default: bool = True) -> bool:
    """
    Show a Yes/No dialog.

    - If Tkinter is available, open a standalone window with a scrollable
      Text widget so that very long lines (e.g. Windows paths) are fully visible,
      including horizontal scroll.
    - This function creates its own Tk root and runs its own mainloop, so it does
      not depend on any existing Tk root or global state.
    - If Tkinter is not available (or fails), ask in the console.

    Returns True for "Yes" and False for "No".
    """

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # Print the full message so the user can see long paths also in console
    print("\n================ DIALOG MESSAGE ================\n")
    print(f"{title}\n")
    print(message)
    print("\n=============== END OF MESSAGE ===============\n")
    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

    # Local import to avoid issues with global imports or circular deps
    try:
        import tkinter as tk  # type: ignore
        from tkinter import ttk  # type: ignore
    except Exception:
        tk = None
        ttk = None

    def _cli_fallback() -> bool:
        """Console-based fallback for environments without GUI."""
        print("[ask_yes_no_dialog_custom] Using CLI fallback (no GUI available).")
        default_str = "Y/n" if default else "y/N"
        while True:
            try:
                ans = input(f"{title}\n{message}\n[{default_str}] ").strip().lower()
            except EOFError:
                # If stdin is not interactive, just return the default
                return default
            if not ans:
                return default
            if ans in ("y", "yes", "s", "si", "sí"):
                return True
            if ans in ("n", "no"):
                return False
            print("Please answer yes or no (y/n).")

    # If Tkinter is not available at all -> console fallback
    if tk is None or ttk is None:
        return _cli_fallback()

    try:
        result = {"value": default}

        # Create an isolated root for this dialog
        root = tk.Tk()
        root.title(title)

        # Optional: set a reasonable default window size
        # Wide enough to see long paths, but still resizable
        root.geometry("1200x400")
        root.resizable(True, True)

        # Main frame
        frm = ttk.Frame(root, padding=10)
        frm.grid(row=0, column=0, sticky="nsew")
        root.rowconfigure(0, weight=1)
        root.columnconfigure(0, weight=1)

        # Info label (can be used for a short description if needed)
        ttk.Label(frm, text="Detected Pre/Post folders:").grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 5))

        # Scrollable text area
        text_frame = ttk.Frame(frm)
        text_frame.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(0, 10))
        frm.rowconfigure(1, weight=1)
        frm.columnconfigure(0, weight=1)

        txt = tk.Text(text_frame, wrap="none", height=12, width=140)
        txt.insert("1.0", message)
        txt.configure(state="disabled")  # read-only

        yscroll = ttk.Scrollbar(text_frame, orient="vertical", command=txt.yview)
        xscroll = ttk.Scrollbar(text_frame, orient="horizontal", command=txt.xview)
        txt.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        txt.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        text_frame.rowconfigure(0, weight=1)
        text_frame.columnconfigure(0, weight=1)

        def on_yes():
            result["value"] = True
            root.destroy()

        def on_no():
            result["value"] = False
            root.destroy()

        # Buttons frame
        btn_frame = ttk.Frame(frm)
        btn_frame.grid(row=2, column=0, columnspan=2, sticky="e")

        btn_no = ttk.Button(btn_frame, text="No", command=on_no)
        btn_yes = ttk.Button(btn_frame, text="Yes", command=on_yes)

        btn_no.pack(side="right", padx=5)
        btn_yes.pack(side="right")

        # Default focus / keyboard shortcuts
        if default:
            btn_yes.focus_set()
        else:
            btn_no.focus_set()

        def on_return(event):
            if default:
                on_yes()
            else:
                on_no()

        root.bind("<Return>", on_return)
        root.bind("<Escape>", lambda e: on_no())

        # Bring window to front
        root.update_idletasks()
        try:
            root.lift()
            root.attributes("-topmost", True)
            root.after(200, lambda: root.attributes("-topmost", False))
        except Exception:
            pass

        # Center the window on screen
        w = root.winfo_width()
        h = root.winfo_height()
        sw = root.winfo_screenwidth()
        sh = root.winfo_screenheight()
        x = (sw // 2) - (w // 2)
        y = (sh // 2) - (h // 2)
        root.geometry(f"{w}x{h}+{x}+{y}")

        # Handle window close (X button) as "No" or default behavior
        def on_close():
            result["value"] = default
            root.destroy()

        root.protocol("WM_DELETE_WINDOW", on_close)

        # Start local event loop; this blocks until root.destroy() is called
        root.mainloop()

        return result["value"]

    except Exception as e:
        print("[ask_yes_no_dialog_custom] Standalone Tk window failed, using CLI fallback:", repr(e))
        return _cli_fallback()

