class QualityException(Exception):
    """Custom exception for data quality validation and logging errors."""
    def __init__(self, message: str):
        super().__init__(f"[QualityException] {message}")
