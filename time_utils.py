from datetime import datetime, timedelta, timezone
import threading
from zoneinfo import ZoneInfo


CRON_NEXT_CACHE = {}
CRON_NEXT_CACHE_LOCK = threading.Lock()


def _value_at(data, *path):
    current = data
    for key in path:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current


def _first_truthy(*values):
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def format_ts(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        if value > 10_000_000_000:
            value = value / 1000.0
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
    if isinstance(value, str):
        return value
    return None


def epoch_seconds(value):
    iso_value = format_ts(value)
    if not iso_value:
        return None
    if isinstance(value, (int, float)):
        if value > 10_000_000_000:
            return value / 1000.0
        return float(value)
    normalized = str(iso_value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).timestamp()
    except ValueError:
        return None


def format_cron_expr(expr):
    if not expr or not isinstance(expr, str):
        return None
    parts = expr.split()
    if len(parts) < 2:
        return expr
    minute, hour = parts[0], parts[1]
    if minute.isdigit() and hour.isdigit():
        return f"{int(hour):02d}:{int(minute):02d}"
    return expr


def parse_cron_field(field, minimum, maximum, allow_wrap=False):
    if not isinstance(field, str) or not field.strip():
        return None
    values = set()
    for part in field.split(","):
        part = part.strip()
        if not part:
            continue
        step = 1
        if "/" in part:
            base, step_str = part.split("/", 1)
            if not step_str.isdigit():
                return None
            step = int(step_str)
            part = base or "*"
        if part == "*":
            start, end = minimum, maximum
        elif "-" in part:
            start_str, end_str = part.split("-", 1)
            if not (start_str.strip("-").isdigit() and end_str.strip("-").isdigit()):
                return None
            start, end = int(start_str), int(end_str)
        else:
            if not part.strip("-").isdigit():
                return None
            start = end = int(part)
        if allow_wrap:
            if start == 7:
                start = 0
            if end == 7:
                end = 0
        if start < minimum or start > maximum or end < minimum or end > maximum:
            return None
        if start <= end:
            values.update(range(start, end + 1, step))
        else:
            values.update(range(start, maximum + 1, step))
            values.update(range(minimum, end + 1, step))
    return values


def cron_matches(expr, candidate):
    if not isinstance(expr, str):
        return False
    parts = expr.split()
    if len(parts) != 5:
        return False
    minute_vals = parse_cron_field(parts[0], 0, 59)
    hour_vals = parse_cron_field(parts[1], 0, 23)
    day_vals = parse_cron_field(parts[2], 1, 31)
    month_vals = parse_cron_field(parts[3], 1, 12)
    dow_vals = parse_cron_field(parts[4], 0, 6, allow_wrap=True)
    if None in (minute_vals, hour_vals, day_vals, month_vals, dow_vals):
        return False
    python_dow = (candidate.weekday() + 1) % 7
    return (
        candidate.minute in minute_vals
        and candidate.hour in hour_vals
        and candidate.day in day_vals
        and candidate.month in month_vals
        and python_dow in dow_vals
    )


def compute_next_run_from_expr(expr, tz_name=None):
    if not isinstance(expr, str) or len(expr.split()) != 5:
        return None
    now = datetime.now(ZoneInfo(tz_name)) if tz_name else datetime.now().astimezone()
    current = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    cache_key = f"{tz_name or current.tzinfo}|{expr}|{current.strftime('%Y%m%d%H%M')}"
    with CRON_NEXT_CACHE_LOCK:
        cached = CRON_NEXT_CACHE.get(cache_key)
        if cached:
            return cached
    for _ in range(0, 60 * 24 * 370):
        if cron_matches(expr, current):
            result = current.isoformat()
            with CRON_NEXT_CACHE_LOCK:
                CRON_NEXT_CACHE.clear()
                CRON_NEXT_CACHE[cache_key] = result
            return result
        current += timedelta(minutes=1)
    return None


def dedupe_datetime_suffix(text):
    if not isinstance(text, str):
        return text
    parts = [part.strip() for part in text.split("·")]
    if len(parts) == 2 and parts[0].endswith(parts[1]):
        return parts[0]
    return text


def humanize_timestamp(value, tz_name=None):
    iso_value = format_ts(value)
    if not iso_value:
        return None
    normalized = iso_value.replace("Z", "+00:00")
    try:
        date = datetime.fromisoformat(normalized)
    except ValueError:
        return iso_value
    if tz_name:
        try:
            date = date.astimezone(ZoneInfo(tz_name))
        except Exception:
            date = date.astimezone()
    else:
        date = date.astimezone()
    now = datetime.now(date.tzinfo)
    if date.date() == now.date():
        return date.strftime("%H:%M")
    return date.strftime("%Y-%m-%d kl %H:%M")


def format_relative_delta(target, now):
    delta_seconds = int(target.timestamp() - now.timestamp())
    if delta_seconds <= 0:
        return None
    minutes = max(delta_seconds // 60, 1)
    if minutes < 60:
        return f"In {minutes} minute{'s' if minutes != 1 else ''}"
    hours = minutes // 60
    if hours < 24:
        return f"In {hours} hour{'s' if hours != 1 else ''}"
    days = hours // 24
    remaining_hours = hours % 24
    if remaining_hours:
        return f"In {days} day{'s' if days != 1 else ''} and {remaining_hours} hour{'s' if remaining_hours != 1 else ''}"
    return f"In {days} day{'s' if days != 1 else ''}"


def format_elapsed_since(value, tz_name=None):
    iso_value = format_ts(value)
    if not iso_value:
        return None
    normalized = iso_value.replace("Z", "+00:00")
    try:
        date = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if tz_name:
        try:
            date = date.astimezone(ZoneInfo(tz_name))
        except Exception:
            date = date.astimezone()
    else:
        date = date.astimezone()
    now = datetime.now(date.tzinfo)
    delta_seconds = max(int(now.timestamp() - date.timestamp()), 0)
    if delta_seconds < 60:
        return "just now"
    minutes = max(delta_seconds // 60, 1)
    if minutes < 60:
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = hours // 24
    return f"{days} day{'s' if days != 1 else ''} ago"


def humanize_schedule_timestamp(value, tz_name=None):
    iso_value = format_ts(value)
    if not iso_value:
        return None
    normalized = iso_value.replace("Z", "+00:00")
    try:
        date = datetime.fromisoformat(normalized)
    except ValueError:
        return iso_value
    if tz_name:
        try:
            date = date.astimezone(ZoneInfo(tz_name))
        except Exception:
            date = date.astimezone()
    else:
        date = date.astimezone()
    now = datetime.now(date.tzinfo)
    relative = format_relative_delta(date, now)
    return relative or date.strftime("%Y-%m-%d %H:%M")


def compact_value(value, fallback="Unknown"):
    if value in (None, "", [], {}):
        return fallback
    if isinstance(value, str):
        return dedupe_datetime_suffix(format_cron_expr(value) or value)
    if isinstance(value, (int, float)):
        return humanize_timestamp(value) or str(value)
    if isinstance(value, list):
        compact = [compact_value(item, fallback="") for item in value[:3]]
        compact = [item for item in compact if item]
        deduped = []
        for item in compact:
            if item not in deduped:
                deduped.append(item)
        return dedupe_datetime_suffix(" · ".join(deduped)) if deduped else fallback
    if isinstance(value, dict):
        for key in ("nextRunAtMs", "nextRunMs", "runAtMs", "timestampMs"):
            if key in value:
                stamp = humanize_timestamp(value.get(key), value.get("tz"))
                if stamp:
                    expr = format_cron_expr(_first_truthy(value.get("expr"), value.get("cron")))
                    if expr and stamp.endswith(expr):
                        return stamp
                    return dedupe_datetime_suffix(stamp)
        for key in ("nextRunAt", "nextRun", "runAt", "timestamp", "time"):
            if key in value:
                stamp = humanize_timestamp(value.get(key), value.get("tz")) or str(value.get(key))
                if stamp:
                    expr = format_cron_expr(_first_truthy(value.get("expr"), value.get("cron")))
                    if expr and stamp.endswith(expr):
                        return stamp
                    return dedupe_datetime_suffix(stamp)
        for key in ("label", "name", "message", "text", "expr", "cron", "schedule"):
            if value.get(key):
                return compact_value(value.get(key), fallback=fallback)
        parts = []
        for key in ("kind", "state", "status", "tz"):
            if value.get(key):
                parts.append(str(value.get(key)))
        return dedupe_datetime_suffix(" · ".join(parts)) if parts else fallback
    return str(value)


def compact_schedule_value(value, fallback="Unknown"):
    if value in (None, "", [], {}):
        return fallback
    if isinstance(value, str):
        next_from_expr = compute_next_run_from_expr(value)
        if next_from_expr:
            formatted = humanize_schedule_timestamp(next_from_expr)
            if formatted:
                return formatted
        parsed_epoch = epoch_seconds(value)
        if parsed_epoch is not None:
            formatted = humanize_schedule_timestamp(parsed_epoch)
            if formatted:
                return formatted
        return dedupe_datetime_suffix(format_cron_expr(value) or value)
    if isinstance(value, (int, float)):
        return humanize_schedule_timestamp(value) or str(value)
    if isinstance(value, list):
        compact = [compact_schedule_value(item, fallback="") for item in value[:3]]
        compact = [item for item in compact if item]
        return dedupe_datetime_suffix(" · ".join(compact)) if compact else fallback
    if isinstance(value, dict):
        for key in ("nextRunAtMs", "nextRunMs", "runAtMs", "timestampMs"):
            if key in value:
                stamp = humanize_schedule_timestamp(value.get(key), value.get("tz"))
                if stamp:
                    return dedupe_datetime_suffix(stamp)
        for key in ("nextRunAt", "nextRun", "runAt", "timestamp", "time"):
            if key in value:
                stamp = humanize_schedule_timestamp(value.get(key), value.get("tz")) or str(value.get(key))
                if stamp:
                    return dedupe_datetime_suffix(stamp)
        expr = _first_truthy(value.get("expr"), value.get("cron"))
        if isinstance(expr, str):
            next_from_expr = compute_next_run_from_expr(expr, value.get("tz"))
            if next_from_expr:
                formatted = humanize_schedule_timestamp(next_from_expr, value.get("tz"))
                if formatted:
                    return dedupe_datetime_suffix(formatted)
            return dedupe_datetime_suffix(format_cron_expr(expr) or expr)
        for key in ("label", "name", "message", "text", "schedule"):
            if value.get(key):
                return compact_schedule_value(value.get(key), fallback=fallback)
        return compact_value(value, fallback=fallback)
    return str(value)
