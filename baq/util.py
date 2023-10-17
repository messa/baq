try:
    from datetime import UTC
except ImportError:
    # datetime.UTC is new in Python 3.11
    from datetime import timezone
    UTC = timezone.utc
