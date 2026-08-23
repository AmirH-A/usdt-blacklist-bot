from typing import Optional


def format_usdt_balance(balance: Optional[float]) -> str:
    if balance is None:
        return "Unknown"
    if balance == 0:
        return "0 USDT"
    if balance >= 1_000_000:
        return f"{balance:,.0f} USDT"
    if balance >= 1000:
        return f"{balance:,.2f} USDT"
    return f"{balance:.2f} USDT"


def format_elapsed_time(seconds: Optional[float]) -> str:
    if seconds is None:
        return "Unknown"
    if seconds < 60:
        return f"{int(seconds)} seconds"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}h {minutes}m"


def format_time_ago(iso_time_str: Optional[str], now) -> str:
    if not iso_time_str:
        return "Never"
    try:
        from datetime import datetime
        scan_time = datetime.fromisoformat(iso_time_str.replace("Z", "+00:00"))
        diff = (now - scan_time).total_seconds()
        if diff < 5:
            return "Just now"
        elif diff < 60:
            return f"{int(diff)}s ago"
        elif diff < 3600:
            return f"{int(diff // 60)}m {int(diff % 60)}s ago"
        elif diff < 86400:
            return f"{int(diff // 3600)}h {int((diff % 3600) // 60)}m ago"
        else:
            return f"{int(diff // 86400)}d ago"
    except Exception:
        return iso_time_str[:19] if iso_time_str else "Unknown"
