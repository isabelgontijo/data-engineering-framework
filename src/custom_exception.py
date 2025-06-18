class CustomException(Exception):
    """Custom exception."""
    def __init__(self, message: str):
        super().__init__(f"[Exception] {message}")