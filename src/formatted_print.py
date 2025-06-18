from datetime import datetime

def formatted_print(message: str, debug: bool = False, force_debug_only: bool = False):
    if force_debug_only and not debug:
        return

    if debug:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] {message}")
    else:
        print(message)