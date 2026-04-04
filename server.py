#!/usr/bin/env python3
import json
import mimetypes
import queue
import re
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

mimetypes.add_type("application/manifest+json", ".webmanifest")
mimetypes.add_type("image/svg+xml", ".svg")


BASE_DIR = Path(__file__).resolve().parent
PUBLIC_DIR = BASE_DIR / "public"
CONFIG_PATH = BASE_DIR / "config.json"
COMMAND_CACHE = {}
COMMAND_CACHE_LOCK = threading.Lock()
CHAT_CACHE = {}
CHAT_CACHE_LOCK = threading.Lock()
CRON_NEXT_CACHE = {}
CRON_NEXT_CACHE_LOCK = threading.Lock()
SECTION_CACHE = {}
SECTION_CACHE_LOCK = threading.Lock()


DEFAULT_CONFIG = {
    "server": {"host": "0.0.0.0", "port": 3000},
    "openclaw": {
        "cliPath": "openclaw",
        "gatewayUrl": "ws://127.0.0.1:18789",
        "token": "",
        "timeoutMs": 5000,
        "pollIntervalMs": 10000,
        "refreshMs": {
            "live": 5000,
            "presence": 8000,
            "activeSessions": 4000,
            "activity": 20000,
            "sessionsHistory": 45000,
            "cronMetadata": 90000,
            "agentsMetadata": 120000,
            "cronRuns": 180000,
        },
    },
    "dashboard": {
        "title": "OpenClaw Agent Control",
        "subtitle": "Live status from your local gateway",
        "activityItems": 8,
        "whatsapp": {"label": "WhatsApp", "cronJobIds": []},
        "agentCards": [
            {"id": "main", "label": "Main Agent"},
            {"id": "logger", "label": "Log Agent"},
        ],
    },
}


def refresh_seconds(config, key, default_ms):
    refresh_ms = value_at(config, "openclaw", "refreshMs", key)
    try:
        return max(int(refresh_ms or default_ms) / 1000.0, 1.0)
    except (TypeError, ValueError):
        return max(default_ms / 1000.0, 1.0)


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def load_json(path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def deep_merge(base, override):
    if not isinstance(base, dict) or not isinstance(override, dict):
        return override
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config():
    config = DEFAULT_CONFIG
    if CONFIG_PATH.exists():
        config = deep_merge(config, load_json(CONFIG_PATH))
    return config


def value_at(data, *path):
    current = data
    for key in path:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current


def normalize_list(value):
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in ("items", "entries", "agents", "jobs", "rows", "data"):
            nested = value.get(key)
            if isinstance(nested, list):
                return nested
        return list(value.values())
    return []


def first_truthy(*values):
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def parse_json_payload(text):
    decoder = json.JSONDecoder()
    stripped = (text or "").strip()
    if not stripped:
        raise json.JSONDecodeError("empty input", stripped, 0)

    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    for marker in ("{", "["):
        start = stripped.find(marker)
        while start != -1:
            try:
                payload, _ = decoder.raw_decode(stripped[start:])
                return payload
            except json.JSONDecodeError:
                start = stripped.find(marker, start + 1)
    raise json.JSONDecodeError("no json payload found", stripped, 0)


def run_openclaw_command(config, subcommand, include_remote=True, timeout_override_ms=None):
    openclaw = config["openclaw"]
    cli_path = openclaw.get("cliPath", "openclaw")
    timeout_ms = int(timeout_override_ms or openclaw.get("timeoutMs", 5000))
    command = [cli_path] + subcommand

    if include_remote:
        gateway_url = openclaw.get("gatewayUrl")
        token = openclaw.get("token")
        if gateway_url:
            command.extend(["--url", gateway_url])
        if token:
            command.extend(["--token", token])
        if "--timeout" not in subcommand:
            command.extend(["--timeout", str(timeout_ms)])

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=max(timeout_ms / 1000.0, 1.0),
            check=False,
        )
    except FileNotFoundError as exc:
        return {"ok": False, "error": f"Fant ikke OpenClaw CLI: {cli_path}", "detail": str(exc)}
    except subprocess.TimeoutExpired as exc:
        partial_stdout = (exc.stdout or "").strip() if isinstance(exc.stdout, str) else ""
        partial_payload = None
        if partial_stdout:
            try:
                partial_payload = parse_json_payload(partial_stdout)
            except json.JSONDecodeError:
                partial_payload = partial_stdout
        return {
            "ok": False,
            "error": "Tidsavbrudd mot OpenClaw CLI",
            "timedOut": True,
            "data": partial_payload,
        }

    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "").strip()
        return {"ok": False, "error": stderr or f"OpenClaw returnerte exit-kode {result.returncode}"}

    stdout = (result.stdout or "").strip()
    if not stdout:
        return {"ok": True, "data": None}

    try:
        return {"ok": True, "data": parse_json_payload(stdout)}
    except json.JSONDecodeError:
        return {"ok": True, "data": stdout}


def cached_openclaw_command(config, cache_key, subcommand, ttl_seconds, include_remote=True):
    now = time.time()
    with COMMAND_CACHE_LOCK:
        entry = COMMAND_CACHE.get(cache_key)
        if entry and now - entry.get("ts", 0) < ttl_seconds:
            return entry.get("value")
    value = run_openclaw_command(config, subcommand, include_remote=include_remote)
    with COMMAND_CACHE_LOCK:
        COMMAND_CACHE[cache_key] = {"ts": now, "value": value}
    return value


def gateway_call(config, method, params):
    result = run_openclaw_command(
        config,
        ["gateway", "call", method, "--params", json.dumps(params), "--json"],
        include_remote=True,
    )
    if not result.get("ok"):
        return result
    payload = result.get("data")
    if isinstance(payload, dict) and "result" in payload:
        return {"ok": True, "data": payload.get("result")}
    return result


def maybe_cached_command(config, cache_key, subcommand, ttl_seconds, include_remote=True, fresh=False, timeout_override_ms=None):
    if fresh:
        return run_openclaw_command(
            config,
            subcommand,
            include_remote=include_remote,
            timeout_override_ms=timeout_override_ms,
        )
    return cached_openclaw_command(config, cache_key, subcommand, ttl_seconds, include_remote=include_remote)


def section_value(name, min_interval_seconds, builder):
    now = time.time()
    with SECTION_CACHE_LOCK:
        entry = SECTION_CACHE.get(name)
        if entry and now - entry.get("ts", 0) < min_interval_seconds:
            return entry.get("value")
    previous_value = entry.get("value") if entry else None
    try:
        value = builder(previous_value)
    except Exception:
        if previous_value is not None:
            return previous_value
        raise
    with SECTION_CACHE_LOCK:
        SECTION_CACHE[name] = {"ts": now, "value": value}
    return value


def as_text(value, fallback="Unknown"):
    if value in (None, ""):
        return fallback
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, (dict, list)):
        return compact_value(value, fallback=fallback)
    return str(value)


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
    same_day = date.date() == now.date()
    if same_day:
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
        compact = deduped
        return dedupe_datetime_suffix(" · ".join(compact)) if compact else fallback
    if isinstance(value, dict):
        for key in ("nextRunAtMs", "nextRunMs", "runAtMs", "timestampMs"):
            if key in value:
                stamp = humanize_timestamp(value.get(key), value.get("tz"))
                if stamp:
                    expr = format_cron_expr(first_truthy(value.get("expr"), value.get("cron")))
                    if expr and stamp.endswith(expr):
                        return stamp
                    return dedupe_datetime_suffix(stamp)
        for key in ("nextRunAt", "nextRun", "runAt", "timestamp", "time"):
            if key in value:
                stamp = humanize_timestamp(value.get(key), value.get("tz")) or str(value.get(key))
                if stamp:
                    expr = format_cron_expr(first_truthy(value.get("expr"), value.get("cron")))
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
        expr = first_truthy(value.get("expr"), value.get("cron"))
        if expr:
            next_from_expr = compute_next_run_from_expr(str(expr), value.get("tz"))
            if next_from_expr:
                stamp = humanize_schedule_timestamp(next_from_expr, value.get("tz"))
                if stamp:
                    return dedupe_datetime_suffix(stamp)
        for key in ("label", "name", "message", "text", "expr", "cron", "schedule"):
            if value.get(key):
                return compact_schedule_value(value.get(key), fallback=fallback)
        return compact_value(value, fallback=fallback)
    return str(value)


def canonical_name(value):
    text = "".join(char for char in str(value).lower() if char.isalnum())
    for suffix in ("agent", "session", "worker"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def derive_agent_presence(raw_status, last_task="", schedule=""):
    text = " ".join(str(part).lower() for part in [raw_status, last_task, schedule] if part)
    if any(word in text for word in ("running", "working", "busy", "processing", "executing", "sending")):
        return {"code": "working", "label": "Working", "emoji": "⚙️"}
    if any(word in text for word in ("sleep", "waiting", "scheduled", "cron", "07:00", "19:00")):
        return {"code": "sleeping", "label": "Sleeping", "emoji": "💤"}
    if any(word in text for word in ("idle", "ready", "online", "configured", "enabled")):
        return {"code": "idle", "label": "Idle", "emoji": "🟢"}
    if any(word in text for word in ("error", "failed", "offline")):
        return {"code": "error", "label": "Error", "emoji": "🔴"}
    return {"code": "idle", "label": "Idle", "emoji": "🟢"}


def extract_active_sessions(sessions_result, active_window_seconds=240):
    sessions = []
    if isinstance(sessions_result, dict):
        sessions = normalize_list(
            first_truthy(
                sessions_result.get("sessions"),
                sessions_result.get("items"),
                sessions_result.get("entries"),
                sessions_result,
            )
        )
    elif isinstance(sessions_result, list):
        sessions = sessions_result

    active = {}
    now_ts = time.time()
    for session in sessions:
        if not isinstance(session, dict):
            continue
        updated_raw = first_truthy(session.get("updatedAt"), session.get("lastMessageAt"), session.get("ts"), session.get("time"))
        updated_epoch = epoch_seconds(updated_raw)
        if updated_epoch is None:
            continue
        if now_ts - updated_epoch > active_window_seconds:
            continue
        key_candidates = [
            canonical_name(first_truthy(session.get("agentId"), session.get("agent"), value_at(session, "meta", "agentId"))),
            canonical_name(first_truthy(session.get("sessionKey"), session.get("key"), session.get("id"))),
        ]
        payload = {
            "updatedAt": format_ts(updated_raw),
            "title": as_text(first_truthy(session.get("title"), session.get("name")), ""),
        }
        for candidate in key_candidates:
            if candidate:
                active[candidate] = payload
    return active


def extract_recent_sessions(sessions_result):
    sessions = []
    if isinstance(sessions_result, dict):
        sessions = normalize_list(
            first_truthy(
                sessions_result.get("sessions"),
                sessions_result.get("items"),
                sessions_result.get("entries"),
                sessions_result,
            )
        )
    elif isinstance(sessions_result, list):
        sessions = sessions_result

    recent = {}
    for session in sessions:
        if not isinstance(session, dict):
            continue
        updated_raw = first_truthy(
            session.get("updatedAt"),
            session.get("updatedAtMs"),
            session.get("lastMessageAt"),
            session.get("lastMessageAtMs"),
            session.get("ts"),
            session.get("time"),
        )
        updated_epoch = epoch_seconds(updated_raw)
        if updated_epoch is None:
            continue

        key = as_text(first_truthy(session.get("sessionKey"), session.get("key")), "")
        agent_id = canonical_name(first_truthy(session.get("agentId"), session.get("agent"), value_at(session, "meta", "agentId")))
        if not agent_id and key.startswith("agent:"):
            parts = key.split(":")
            if len(parts) >= 2:
                agent_id = canonical_name(parts[1])
        if not agent_id:
            continue

        session_type = "chat"
        if ":cron:" in key or key.startswith("cron:"):
            session_type = "cron"

        payload = {
            "updatedAt": format_ts(updated_raw),
            "updatedEpoch": updated_epoch,
            "key": key,
            "kind": session_type,
            "title": as_text(first_truthy(session.get("title"), session.get("name"), session.get("displayName")), ""),
        }
        previous = recent.get(agent_id)
        if not previous or updated_epoch > previous.get("updatedEpoch", 0):
            recent[agent_id] = payload
    return recent


def extract_health_summary(health):
    if not isinstance(health, dict):
        return {}
    summary = first_truthy(
        health.get("summary"),
        value_at(health, "status", "summary"),
        value_at(health, "gateway"),
        {},
    )
    return summary if isinstance(summary, dict) else {}


def extract_channels(health):
    channel_candidates = []
    if isinstance(health, dict):
        for key in ("channels", "linkChannel", "linkedChannels"):
            value = health.get(key)
            if isinstance(value, list):
                channel_candidates.extend(value)
            elif isinstance(value, dict):
                if all(isinstance(item, dict) for item in value.values()):
                    channel_candidates.extend(value.values())
                else:
                    channel_candidates.append(value)

    normalized = []
    seen = set()
    for item in channel_candidates:
        if not isinstance(item, dict):
            continue
        label = first_truthy(item.get("label"), item.get("name"), item.get("channel"), item.get("id"))
        if not label:
            continue
        key = str(label).lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "id": item.get("id") or key,
                "label": str(label),
                "status": as_text(
                    first_truthy(
                        item.get("status"),
                        item.get("state"),
                        "Online" if item.get("linked") or item.get("authenticated") else "Unknown",
                    )
                ),
                "detail": as_text(
                    first_truthy(
                        item.get("accountId"),
                        item.get("account"),
                        item.get("detail"),
                        item.get("transport"),
                    ),
                    "",
                ),
                "kind": "channel",
            }
        )
    return normalized


def mentions_whatsapp(*values):
    haystack = canonical_name(" ".join(str(value or "") for value in values))
    return "whatsapp" in haystack


def title_case_transport(value):
    text = str(value or "").strip()
    if not text:
        return "Transport"
    if canonical_name(text) == "whatsapp":
        return "WhatsApp"
    return text.replace("-", " ").replace("_", " ").title()


def sort_cron_jobs(cron_jobs):
    def sort_key(job):
        transport = canonical_name(first_truthy(job.get("deliveryChannel"), job.get("transport"), "zz"))
        agent = canonical_name(job.get("agentId", "zz"))
        next_run = as_text(first_truthy(job.get("nextRun"), job.get("lastRun")), "zz")
        label = as_text(job.get("label"), "zz")
        return (transport, agent, next_run, label)

    return sorted(cron_jobs, key=sort_key)


def extract_cron_jobs(cron_result):
    jobs = normalize_list(cron_result)
    normalized = []
    for item in jobs:
        if not isinstance(item, dict):
            continue
        schedule_obj = item.get("schedule") if isinstance(item.get("schedule"), dict) else {}
        payload_obj = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        delivery_obj = item.get("delivery") if isinstance(item.get("delivery"), dict) else {}
        normalized.append(
            {
                "id": as_text(first_truthy(item.get("id"), item.get("name")), "cron-job"),
                "label": as_text(first_truthy(item.get("name"), item.get("label")), "Cron Job"),
                "status": as_text(first_truthy(item.get("status"), item.get("state"), "Scheduled")),
                "agentId": as_text(first_truthy(item.get("agentId"), item.get("agent"), schedule_obj.get("agentId")), ""),
                "nextRun": compact_schedule_value(
                    first_truthy(
                        item.get("nextRunAt"),
                        item.get("nextRunAtMs"),
                        item.get("nextRun"),
                        schedule_obj.get("at"),
                        item.get("next"),
                        schedule_obj,
                        item.get("schedule"),
                    ),
                    "Not available",
                ),
                "lastRun": compact_value(first_truthy(item.get("lastRunAt"), item.get("lastRun"), item.get("lastSuccess")), ""),
                "message": as_text(first_truthy(item.get("message"), payload_obj.get("message"), payload_obj.get("text"), payload_obj.get("systemEvent")), ""),
                "deliveryChannel": as_text(first_truthy(delivery_obj.get("channel"), item.get("channel"), item.get("transport")), ""),
                "deliveryTarget": as_text(first_truthy(delivery_obj.get("to"), delivery_obj.get("target"), item.get("target")), ""),
                "deliveryMode": as_text(first_truthy(delivery_obj.get("mode"), item.get("mode")), ""),
                "model": as_text(first_truthy(item.get("model"), payload_obj.get("model")), ""),
                "provider": as_text(first_truthy(item.get("provider"), payload_obj.get("provider")), ""),
                "kind": "cron",
            }
        )
    return normalized


def parse_run_timestamp(run):
    return first_truthy(
        run.get("runAtMs"),
        run.get("runAt"),
        run.get("endedAt"),
        run.get("finishedAt"),
        run.get("completedAt"),
        run.get("startedAt"),
        run.get("time"),
        run.get("ts"),
        value_at(run, "result", "endedAt"),
        value_at(run, "result", "finishedAt"),
    )


def extract_latest_run_entry(runs_data):
    runs = []
    if isinstance(runs_data, dict):
        runs = normalize_list(
            first_truthy(
                runs_data.get("entries"),
                runs_data.get("runs"),
                runs_data.get("items"),
                runs_data.get("entries"),
                runs_data.get("history"),
                runs_data,
            )
        )
    elif isinstance(runs_data, list):
        runs = runs_data

    candidates = [run for run in runs if isinstance(run, dict)]
    if not candidates:
        return None

    def sort_key(run):
        raw = parse_run_timestamp(run)
        iso = format_ts(raw)
        return iso or ""

    return max(candidates, key=sort_key)


def enrich_cron_jobs_with_runs(config, cron_jobs):
    enriched = []
    cron_runs_ttl = refresh_seconds(config, "cronRuns", 180000)
    for job in cron_jobs:
        job_copy = dict(job)
        job_id = job_copy.get("id")
        if job_id:
            runs_result = cached_openclaw_command(
                config,
                f"cron-runs:{job_id}",
                ["cron", "runs", "--id", str(job_id), "--limit", "10", "--json"],
                ttl_seconds=cron_runs_ttl,
                include_remote=False,
            )
            if runs_result.get("ok"):
                latest = extract_latest_run_entry(runs_result.get("data"))
                if isinstance(latest, dict):
                    finished_at = parse_run_timestamp(latest)
                    if finished_at:
                        job_copy["lastRun"] = compact_value(finished_at, job_copy.get("lastRun") or "")
                    run_at = first_truthy(latest.get("runAtMs"), latest.get("runAt"), latest.get("ts"))
                    if run_at:
                        job_copy["runAt"] = compact_value(run_at, "")
                    action = as_text(first_truthy(latest.get("action"), latest.get("status")), "")
                    status = as_text(latest.get("status"), "")
                    if action:
                        job_copy["runAction"] = action
                    if status:
                        job_copy["runStatus"] = status
                    model = as_text(first_truthy(latest.get("model"), value_at(latest, "result", "model")), "")
                    provider = as_text(first_truthy(latest.get("provider"), value_at(latest, "result", "provider")), "")
                    if model:
                        job_copy["model"] = model
                    if provider:
                        job_copy["provider"] = provider
                    summary = first_truthy(
                        latest.get("summary"),
                        latest.get("message"),
                        latest.get("status"),
                        value_at(latest, "result", "summary"),
                        value_at(latest, "result", "status"),
                        value_at(latest, "payload", "message"),
                    )
                    if summary:
                        job_copy["message"] = as_text(summary, job_copy.get("message") or "")
                    error_text = as_text(latest.get("error"), "")
                    if error_text:
                        job_copy["runError"] = error_text
                    if mentions_whatsapp(
                        job_copy.get("label"),
                        job_copy.get("message"),
                        error_text,
                        latest.get("sessionKey"),
                        job_copy.get("deliveryChannel"),
                        job_copy.get("deliveryTarget"),
                    ):
                        job_copy["transport"] = "whatsapp"
                    session_key = as_text(latest.get("sessionKey"), "")
                    if session_key:
                        job_copy["sessionKey"] = session_key
                        session_parts = session_key.split(":")
                        if len(session_parts) >= 2:
                            job_copy["agentId"] = job_copy.get("agentId") or session_parts[1]
        enriched.append(job_copy)
    return enriched


def augment_channels_with_transports(config, channels, cron_jobs):
    whatsapp_config = value_at(config, "dashboard", "whatsapp") or {}
    configured_whatsapp_ids = {
        str(job_id)
        for job_id in normalize_list(whatsapp_config.get("cronJobIds") or [])
        if job_id
    }

    transport_jobs = []
    for job in cron_jobs:
        if not isinstance(job, dict):
            continue
        explicit_transport = canonical_name(first_truthy(job.get("deliveryChannel"), job.get("transport"), ""))
        configured_as_whatsapp = str(job.get("id", "")) in configured_whatsapp_ids
        if configured_as_whatsapp and not explicit_transport:
            explicit_transport = "whatsapp"
        if not explicit_transport and mentions_whatsapp(
            job.get("label"),
            job.get("message"),
            job.get("runError"),
            job.get("sessionKey"),
            job.get("deliveryTarget"),
        ):
            explicit_transport = "whatsapp"
        if not explicit_transport:
            continue
        transport_jobs.append((explicit_transport, job))

    if not transport_jobs:
        return channels

    transport_nodes = []
    for transport, job in transport_jobs:
        label = as_text(whatsapp_config.get("label"), "WhatsApp") if transport == "whatsapp" else title_case_transport(transport)
        node = {
            "id": f"{transport}-{job.get('id', 'link')}",
            "label": label,
            "status": "Online",
            "detail": "",
        }
        node["kind"] = "whatsapp" if transport == "whatsapp" else "transport"
        node["transport"] = transport
        node["agentId"] = as_text(job.get("agentId"), "")
        node["relatedAgentIds"] = [as_text(job.get("agentId"), "")]
        node["detail"] = f"Cron: {job.get('label', 'Transport Job')}"
        node["relatedCronId"] = job.get("id")
        node["relatedCronLabel"] = job.get("label")
        node["relatedCronIds"] = [job.get("id")] if job.get("id") else []
        if as_text(job.get("runStatus"), "").lower() in ("error", "failed", "fail"):
            node["status"] = "Attention"
        else:
            node["status"] = node.get("status") or "Online"
        transport_nodes.append(node)
    return transport_nodes + channels


def normalize_agent_record(item, fallback_label):
    if not isinstance(item, dict):
        presence = derive_agent_presence("configured", "", "waiting for cron data")
        return {
            "id": fallback_label.lower().replace(" ", "-"),
            "label": fallback_label,
            "name": "",
            "openclawId": "",
            "cronJobIds": [],
            "status": presence["label"],
            "statusCode": presence["code"],
            "statusEmoji": presence["emoji"],
            "lastTask": "No live metadata yet",
            "nextRun": "Waiting for cron data",
            "model": "Unknown",
        }

    label = as_text(first_truthy(item.get("label"), item.get("name"), item.get("id")), fallback_label)
    raw_status = as_text(first_truthy(item.get("status"), item.get("state"), item.get("enabled") and "Enabled"), "Unknown")
    last_task = as_text(
        first_truthy(
            item.get("lastTask"),
            item.get("lastAction"),
            item.get("lastMessage"),
            item.get("description"),
        ),
        "No recent task",
    )
    schedule = compact_schedule_value(first_truthy(item.get("nextRun"), item.get("schedule"), item.get("next")), "Not scheduled")
    presence = derive_agent_presence(raw_status, last_task, schedule)
    return {
        "id": as_text(first_truthy(item.get("id"), label.lower().replace(" ", "-"))),
        "label": label,
        "name": as_text(first_truthy(item.get("name"), item.get("displayName")), ""),
        "openclawId": as_text(first_truthy(item.get("id"), item.get("agentId")), ""),
        "cronJobIds": [],
        "status": presence["label"],
        "statusCode": presence["code"],
        "statusEmoji": presence["emoji"],
        "lastTask": last_task,
        "nextRun": schedule,
        "model": as_text(first_truthy(item.get("model"), item.get("defaultModel"), item.get("llm")), "Unknown"),
    }


def extract_agents(config, agents_result, cron_jobs, logs, active_sessions, recent_sessions):
    configured_cards = normalize_list(value_at(config, "dashboard", "agentCards"))
    raw_agents = normalize_list(agents_result)
    normalized = []
    configured_keys = {canonical_name(first_truthy(card.get("id"), card.get("label"), "")) for card in configured_cards}
    matched_indexes = set()

    for raw_index, card in enumerate(configured_cards):
        label = as_text(card.get("label"), "Agent")
        configured_name = as_text(card.get("name"), "")
        configured_openclaw_id = as_text(card.get("openclawId"), "")
        configured_cron_job_ids = [str(job_id) for job_id in normalize_list(card.get("cronJobIds")) if job_id]
        card_id = as_text(first_truthy(card.get("id"), label.lower().replace(" ", "-")))
        match = None
        for item_index, item in enumerate(raw_agents):
            if not isinstance(item, dict):
                continue
            raw_keys = {
                canonical_name(item.get("id")),
                canonical_name(item.get("name")),
                canonical_name(item.get("label")),
            }
            raw_keys.discard("")
            if canonical_name(card_id) in raw_keys or canonical_name(label) in raw_keys:
                match = item
                matched_indexes.add(item_index)
                break
        agent = normalize_agent_record(match, label)
        agent["id"] = card_id
        agent["label"] = label
        agent["name"] = configured_name or agent.get("name", "")
        agent["openclawId"] = configured_openclaw_id or agent.get("openclawId", "")
        agent["cronJobIds"] = configured_cron_job_ids
        attach_agent_context(agent, cron_jobs, logs, active_sessions, recent_sessions)
        normalized.append(agent)

    for item_index, item in enumerate(raw_agents):
        if not isinstance(item, dict):
            continue
        if item_index in matched_indexes:
            continue
        raw_keys = {
            canonical_name(item.get("id")),
            canonical_name(item.get("name")),
            canonical_name(item.get("label")),
        }
        raw_keys.discard("")
        if configured_cards and raw_keys & configured_keys:
            continue
        agent = normalize_agent_record(item, "Agent")
        attach_agent_context(agent, cron_jobs, logs, active_sessions, recent_sessions)
        normalized.append(agent)

    return normalized


def attach_agent_context(agent, cron_jobs, logs, active_sessions, recent_sessions):
    label = canonical_name(agent["label"])
    agent_id = canonical_name(agent["id"])
    agent_name = canonical_name(agent.get("name", ""))
    openclaw_id = canonical_name(agent.get("openclawId", ""))
    cron_job_ids = {str(job_id) for job_id in normalize_list(agent.get("cronJobIds")) if job_id}

    session_match = None
    for candidate in [openclaw_id, agent_id, label, agent_name]:
        if candidate and candidate in active_sessions:
            session_match = active_sessions[candidate]
            break
    if not session_match:
        for key, session in active_sessions.items():
            if any(candidate and candidate in key for candidate in [openclaw_id, agent_id, label, agent_name]):
                session_match = session
                break
    if session_match:
        agent["status"] = "Working"
        agent["statusCode"] = "working"
        agent["statusEmoji"] = "⚙️"
        if agent.get("lastTask") in ("No recent task", "No live metadata yet"):
            updated = session_match.get("updatedAt")
            agent["lastTask"] = format_elapsed_since(updated) or "Active chat session"

    recent_session = None
    for candidate in [openclaw_id, agent_id, label, agent_name]:
        if candidate and candidate in recent_sessions:
            recent_session = recent_sessions[candidate]
            break
    if not recent_session:
        for key, session in recent_sessions.items():
            if any(candidate and candidate in key for candidate in [openclaw_id, agent_id, label, agent_name]):
                recent_session = session
                break
    if recent_session and agent.get("lastTask") in ("No recent task", "No live metadata yet"):
        updated = recent_session.get("updatedAt")
        if recent_session.get("kind") == "cron":
            agent["lastTask"] = format_elapsed_since(updated) or "Recent cron activity"
        else:
            agent["lastTask"] = format_elapsed_since(updated) or "Recent chat activity"

    for job in cron_jobs:
        haystack = canonical_name(
            f"{job.get('label', '')} {job.get('id', '')} {job.get('agentId', '')} {job.get('message', '')} {job.get('sessionKey', '')}"
        )
        direct_job_match = str(job.get("id", "")) in cron_job_ids
        if direct_job_match or (label and label in haystack) or (agent_id and agent_id in haystack) or (agent_name and agent_name in haystack) or (openclaw_id and openclaw_id in haystack):
            agent["nextRun"] = first_truthy(job.get("nextRun"), job.get("lastRun"), agent.get("nextRun"))
            job_model = as_text(first_truthy(job.get("model"), job.get("provider")), "")
            if job_model:
                agent["model"] = job_model
            when = first_truthy(job.get("lastRun"), job.get("runAt"))
            if when:
                action = job.get("runAction") or "finished"
                status = job.get("runStatus") or "ok"
                agent["lastTask"] = f"Cron {action} {when} ({status})"
            break
    for entry in logs:
        haystack = canonical_name(f"{entry.get('message', '')} {entry.get('subsystem', '')}")
        if (label and label in haystack) or (agent_id and agent_id in haystack) or (agent_name and agent_name in haystack):
            agent["lastTask"] = entry.get("message") or agent["lastTask"]
            break


def merge_agent_history(previous_agents, current_agents):
    previous_by_key = {}
    for agent in normalize_list(previous_agents):
        if not isinstance(agent, dict):
            continue
        keys = {
            canonical_name(agent.get("id", "")),
            canonical_name(agent.get("label", "")),
            canonical_name(agent.get("name", "")),
            canonical_name(agent.get("openclawId", "")),
        }
        keys.discard("")
        for key in keys:
            previous_by_key[key] = agent

    for agent in current_agents:
        if not isinstance(agent, dict):
            continue
        keys = [
            canonical_name(agent.get("id", "")),
            canonical_name(agent.get("label", "")),
            canonical_name(agent.get("name", "")),
            canonical_name(agent.get("openclawId", "")),
        ]
        previous = next((previous_by_key.get(key) for key in keys if key and previous_by_key.get(key)), None)
        if not previous:
            continue
        if agent.get("lastTask") in ("No recent task", "No live metadata yet"):
            preserved = as_text(previous.get("lastTask"), "")
            if preserved and preserved not in ("No recent task", "No live metadata yet"):
                agent["lastTask"] = preserved


def resolve_agent_reference(agent_token):
    snapshot_agents = normalize_list(value_at(STORE.get(), "agents") or [])
    candidate = canonical_name(agent_token)
    for agent in snapshot_agents:
        if not isinstance(agent, dict):
            continue
        keys = {
            canonical_name(agent.get("id", "")),
            canonical_name(agent.get("label", "")),
            canonical_name(agent.get("name", "")),
            canonical_name(agent.get("openclawId", "")),
        }
        if candidate in keys:
            return {
                "cardId": as_text(agent.get("id"), ""),
                "label": as_text(agent.get("label"), "Agent"),
                "name": as_text(agent.get("name"), ""),
                "openclawId": as_text(first_truthy(agent.get("openclawId"), agent.get("id")), ""),
                "status": as_text(agent.get("status"), "Unknown"),
            }
    return {
        "cardId": as_text(agent_token, ""),
        "label": as_text(agent_token, "Agent"),
        "name": "",
        "openclawId": as_text(agent_token, ""),
        "status": "Unknown",
    }


def extract_sessions_payload(payload):
    if isinstance(payload, dict) and "result" in payload:
        payload = payload.get("result")
    sessions = normalize_list(
        first_truthy(
            value_at(payload, "sessions"),
            value_at(payload, "items"),
            value_at(payload, "entries"),
            value_at(payload, "rows"),
            payload,
        )
    )
    stores = normalize_list(value_at(payload, "stores") or [])
    return sessions, stores


def find_transcript_path(session_id, roots):
    if not session_id:
        return None
    session_filename = f"{session_id}.jsonl"
    for root in roots:
        if not root:
            continue
        root_path = Path(root)
        candidates = [
            root_path / session_filename,
            root_path.parent / session_filename,
            root_path / "sessions" / session_filename,
            root_path.parent / "sessions" / session_filename,
        ]
        for candidate in candidates:
            if candidate.exists():
                return str(candidate)
    return None


def merge_session_records(primary, secondary):
    merged = dict(secondary)
    merged.update(primary)
    for key in ("transcriptPath", "path", "file", "sessionKey", "key", "id", "sessionId"):
        if secondary.get(key) and not merged.get(key):
            merged[key] = secondary.get(key)
    return merged


def load_local_agent_sessions(config, agent_ref):
    local_result = cached_openclaw_command(
        config,
        "local:sessions.all",
        ["sessions", "--all-agents", "--json"],
        ttl_seconds=refresh_seconds(config, "sessionsHistory", 45000),
        include_remote=False,
    )
    if not local_result.get("ok"):
        return []
    local_payload = local_result.get("data") or {}
    local_sessions, stores = extract_sessions_payload(local_payload)
    store_paths = {}
    for store in stores:
        if not isinstance(store, dict):
            continue
        store_agent = canonical_name(first_truthy(store.get("agentId"), store.get("agent"), store.get("id")))
        store_path = first_truthy(store.get("path"), store.get("file"), store.get("sessionsPath"))
        if store_agent and store_path:
            store_paths[store_agent] = str(store_path)

    openclaw_id = canonical_name(agent_ref.get("openclawId", ""))
    matches = []
    for session in local_sessions:
        if not isinstance(session, dict):
            continue
        session_key = as_text(first_truthy(session.get("sessionKey"), session.get("key")), "")
        session_agent = canonical_name(first_truthy(session.get("agentId"), session.get("agent"), value_at(session, "meta", "agentId")))
        if not session_agent and session_key.startswith("agent:"):
            parts = session_key.split(":")
            if len(parts) >= 2:
                session_agent = canonical_name(parts[1])
        if openclaw_id and session_agent != openclaw_id and openclaw_id not in canonical_name(session_key):
            continue
        session_copy = dict(session)
        session_id = as_text(first_truthy(session.get("id"), session.get("sessionId")), "")
        if session_id and "transcriptPath" not in session_copy:
            search_roots = []
            if session_agent in store_paths:
                search_roots.append(store_paths[session_agent])
            store_hint = first_truthy(session_copy.get("path"), session_copy.get("file"), session_copy.get("sessionsPath"))
            if store_hint:
                search_roots.append(store_hint)
            transcript_path = find_transcript_path(session_id, search_roots)
            if transcript_path:
                session_copy["transcriptPath"] = transcript_path
        matches.append(session_copy)
    return matches


def load_agent_sessions(config, agent_ref, fresh=False, include_local=False):
    gateway_result = maybe_cached_command(
        config,
        "gateway:sessions.list",
        ["gateway", "call", "sessions.list", "--params", "{}", "--json"],
        ttl_seconds=8,
        include_remote=True,
        fresh=fresh,
    )
    gateway_payload = gateway_result.get("data") if gateway_result.get("ok") else {}
    gateway_sessions, _gateway_stores = extract_sessions_payload(gateway_payload)
    openclaw_id = canonical_name(agent_ref.get("openclawId", ""))
    all_sessions = {}
    local_sessions = load_local_agent_sessions(config, agent_ref) if include_local else []
    for session in gateway_sessions + local_sessions:
        if not isinstance(session, dict):
            continue
        session_key = as_text(first_truthy(session.get("sessionKey"), session.get("key")), "")
        session_id = as_text(first_truthy(session.get("id"), session.get("sessionId")), "")
        merge_key = session_key or session_id or str(id(session))
        if merge_key in all_sessions:
            all_sessions[merge_key] = merge_session_records(session, all_sessions[merge_key])
        else:
            all_sessions[merge_key] = dict(session)

    matches = []
    for session in all_sessions.values():
        if not isinstance(session, dict):
            continue
        session_key = as_text(first_truthy(session.get("sessionKey"), session.get("key")), "")
        session_agent = canonical_name(first_truthy(session.get("agentId"), session.get("agent"), value_at(session, "meta", "agentId")))
        if not session_agent and session_key.startswith("agent:"):
            parts = session_key.split(":")
            if len(parts) >= 2:
                session_agent = canonical_name(parts[1])
        if openclaw_id and session_agent != openclaw_id and openclaw_id not in canonical_name(session_key):
            continue
        matches.append(dict(session))

    def session_sort_key(session):
        return epoch_seconds(
            first_truthy(
                session.get("updatedAt"),
                session.get("updatedAtMs"),
                session.get("lastMessageAt"),
                session.get("lastMessageAtMs"),
                session.get("ts"),
                session.get("time"),
            )
        ) or 0

    return sorted(matches, key=session_sort_key, reverse=True)


def prefer_agent_session(sessions):
    if not sessions:
        return None
    for session in sessions:
        key = as_text(first_truthy(session.get("sessionKey"), session.get("key"), session.get("sessionKey")), "")
        kind = canonical_name(first_truthy(session.get("kind"), session.get("type"), ""))
        if kind == "main" or ":main" in key or key.endswith(":main"):
            return session
    return sessions[0]


def content_to_text(value):
    if value in (None, ""):
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = []
        for item in value:
          if isinstance(item, str):
              parts.append(item)
          elif isinstance(item, dict):
              parts.append(as_text(first_truthy(item.get("text"), item.get("content"), item.get("value")), ""))
        return "\n".join(part for part in parts if part)
    if isinstance(value, dict):
        return as_text(first_truthy(value.get("text"), value.get("content"), value.get("value")), "")
    return str(value)


CHAT_PREFIX_TS_RE = re.compile(r"^\[(?P<stamp>[^\]]+)\]\s*")
CHAT_TEXT_TS_RE = re.compile(r"\[(?P<stamp>[A-Za-z]{3}\s+\d{4}-\d{2}-\d{2}[^\]]*)\]")


def extract_prefixed_chat_timestamp(text):
    if not isinstance(text, str):
        return None, text
    match = CHAT_PREFIX_TS_RE.match(text.strip())
    if not match:
        return None, text
    stamp = match.group("stamp").strip()
    stripped = CHAT_PREFIX_TS_RE.sub("", text.strip(), count=1).strip()
    return stamp, stripped


def extract_inline_chat_timestamp(text):
    raw = str(text or "")
    if not raw:
        return None, raw
    match = CHAT_TEXT_TS_RE.search(raw)
    if not match:
        return None, raw
    stamp = match.group("stamp").strip()
    stripped = raw.replace(match.group(0), "").strip()
    return stamp, stripped


def deep_find_timestamp(value):
    if isinstance(value, dict):
        priority_keys = (
            "time",
            "ts",
            "createdAtMs",
            "createdAt",
            "updatedAtMs",
            "updatedAt",
            "timestampMs",
            "timestamp",
            "date",
        )
        for key in priority_keys:
            if key in value:
                candidate = value.get(key)
                if epoch_seconds(candidate) is not None:
                    return candidate
        for nested in value.values():
            found = deep_find_timestamp(nested)
            if found is not None:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = deep_find_timestamp(nested)
            if found is not None:
                return found
    return None


def chat_message_timestamp(entry):
    raw_time = first_truthy(
        entry.get("time"),
        entry.get("ts"),
        entry.get("createdAtMs"),
        entry.get("createdAt"),
        entry.get("updatedAtMs"),
        entry.get("updatedAt"),
        value_at(entry, "message", "time"),
        value_at(entry, "payload", "time"),
        value_at(entry, "meta", "time"),
        deep_find_timestamp(entry),
    )
    return (humanize_timestamp(raw_time) or compact_value(raw_time, "")), epoch_seconds(raw_time)


def parse_transcript_messages(path):
    transcript_path = Path(path)
    if not transcript_path.exists():
        return []
    messages = []
    with transcript_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                entry = parse_json_payload(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(entry, dict):
                continue
            role = as_text(first_truthy(entry.get("role"), value_at(entry, "message", "role"), entry.get("author")), "").lower()
            text = content_to_text(
                first_truthy(
                    entry.get("text"),
                    entry.get("content"),
                    entry.get("message"),
                    value_at(entry, "message", "content"),
                    value_at(entry, "payload", "text"),
                    value_at(entry, "payload", "message"),
                )
            ).strip()
            prefixed_time, text = extract_prefixed_chat_timestamp(text)
            inline_time, text = extract_inline_chat_timestamp(text)
            if not role and entry.get("type") in ("user", "assistant", "system"):
                role = entry.get("type")
            if not role:
                entry_type = as_text(entry.get("type"), "").lower()
                if "assistant" in entry_type:
                    role = "assistant"
                elif "user" in entry_type:
                    role = "user"
                elif "system" in entry_type:
                    role = "system"
            if role not in ("user", "assistant", "system") or not text:
                continue
            messages.append(
                {
                    "role": role,
                    "text": text,
                    "time": prefixed_time or inline_time or chat_message_timestamp(entry)[0],
                    "sortTs": chat_message_timestamp(entry)[1],
                }
            )
    return ensure_chat_message_times(messages[-80:])


def normalize_message_order(messages):
    timestamps = [message.get("sortTs") for message in messages if isinstance(message, dict) and message.get("sortTs") is not None]
    if len(timestamps) >= 2 and timestamps[0] > timestamps[-1]:
        return list(reversed(messages))
    return messages


def merge_chat_messages(primary, secondary):
    ordered_primary = normalize_message_order(list(primary or []))
    ordered_secondary = normalize_message_order(list(secondary or []))
    base = ordered_secondary if len(ordered_secondary) >= len(ordered_primary) else ordered_primary
    extra = ordered_primary if base is ordered_secondary else ordered_secondary
    merged = []
    seen = set()
    for message in base + extra:
        if not isinstance(message, dict):
            continue
        key = (
            message.get("role", ""),
            message.get("text", ""),
            message.get("time", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(message)
    return merged[-80:]


def ensure_chat_message_times(messages):
    normalized = []
    for message in normalize_list(messages):
        if not isinstance(message, dict):
            continue
        item = dict(message)
        if not item.get("time"):
            sort_ts = item.get("sortTs")
            if sort_ts is not None:
                item["time"] = humanize_timestamp(sort_ts) or ""
        normalized.append(item)
    return normalized


def get_cached_chat(agent_key):
    with CHAT_CACHE_LOCK:
        return dict(CHAT_CACHE.get(agent_key) or {})


def store_cached_chat(agent_key, payload):
    if not agent_key or not isinstance(payload, dict):
        return
    with CHAT_CACHE_LOCK:
        CHAT_CACHE[agent_key] = {
            "ts": time.time(),
            "payload": dict(payload),
        }


def build_agent_chat_payload(config, agent_token, fresh=False):
    agent_ref = resolve_agent_reference(agent_token)
    cache_key = canonical_name(first_truthy(agent_ref.get("openclawId"), agent_ref.get("cardId"), agent_token))
    cached_chat = get_cached_chat(cache_key)
    cached_payload = cached_chat.get("payload") if isinstance(cached_chat, dict) else {}
    if not fresh and isinstance(cached_payload, dict) and cached_payload.get("messages"):
        payload = dict(cached_payload)
        payload["messages"] = ensure_chat_message_times(payload.get("messages", []))
        payload["agent"] = agent_ref
        return payload
    sessions = load_agent_sessions(config, agent_ref, fresh=fresh, include_local=False)
    session = prefer_agent_session(sessions)
    session_key = as_text(first_truthy(session.get("sessionKey"), session.get("key")), "") if isinstance(session, dict) else ""
    gateway_messages = []
    if session_key:
        history_result = gateway_call(config, "chat.history", {"sessionKey": session_key, "limit": 80})
        if history_result.get("ok"):
            rows = normalize_list(
                first_truthy(
                    value_at(history_result.get("data"), "items"),
                    value_at(history_result.get("data"), "entries"),
                    value_at(history_result.get("data"), "messages"),
                    history_result.get("data"),
                )
            )
            normalized = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                role = as_text(first_truthy(row.get("role"), row.get("author"), row.get("type")), "").lower()
                if role not in ("user", "assistant", "system"):
                    continue
                text = content_to_text(
                    first_truthy(
                        row.get("text"),
                        row.get("content"),
                        row.get("message"),
                        value_at(row, "payload", "text"),
                        value_at(row, "payload", "message"),
                    )
                ).strip()
                prefixed_time, text = extract_prefixed_chat_timestamp(text)
                inline_time, text = extract_inline_chat_timestamp(text)
                if not text:
                    continue
                display_time, sort_ts = chat_message_timestamp(row)
                normalized.append(
                    {
                        "role": role,
                        "text": text,
                        "time": prefixed_time or inline_time or display_time,
                        "sortTs": sort_ts,
                    }
                )
            gateway_messages = ensure_chat_message_times(normalized)
    transcript_session = session
    if not gateway_messages:
        local_sessions = load_agent_sessions(config, agent_ref, fresh=False, include_local=True)
        local_preferred = prefer_agent_session(local_sessions)
        if local_preferred:
            transcript_session = local_preferred
            if not session:
                session = local_preferred
                session_key = as_text(first_truthy(session.get("sessionKey"), session.get("key")), "") if isinstance(session, dict) else ""
    transcript_path = first_truthy(
        transcript_session.get("transcriptPath") if isinstance(transcript_session, dict) else None,
        transcript_session.get("path") if isinstance(transcript_session, dict) else None,
        transcript_session.get("file") if isinstance(transcript_session, dict) else None,
    )
    transcript_messages = parse_transcript_messages(transcript_path) if transcript_path else []
    messages = ensure_chat_message_times(gateway_messages if gateway_messages else transcript_messages)
    payload = {
        "agent": agent_ref,
        "sessionId": as_text(first_truthy(session.get("id"), session.get("sessionId")), "") if isinstance(session, dict) else "",
        "sessionKey": session_key,
        "messages": messages,
    }
    if not payload["messages"] and isinstance(cached_payload, dict) and cached_payload.get("messages"):
        payload["messages"] = cached_payload.get("messages", [])
        payload["sessionId"] = payload["sessionId"] or cached_payload.get("sessionId", "")
        payload["sessionKey"] = payload["sessionKey"] or cached_payload.get("sessionKey", "")
    if payload["messages"]:
        store_cached_chat(cache_key, payload)
    return payload


def send_agent_message(config, agent_token, message, session_id=""):
    agent_ref = resolve_agent_reference(agent_token)
    sessions = load_agent_sessions(config, agent_ref, fresh=True, include_local=False)
    session = prefer_agent_session(sessions)
    session_key = as_text(first_truthy(session.get("sessionKey"), session.get("key")), "") if isinstance(session, dict) else ""

    if session_key:
        send_result = gateway_call(config, "chat.send", {"sessionKey": session_key, "text": message})
        if send_result.get("ok"):
            return {"ok": True, "result": send_result.get("data"), "error": None}

    command = ["agent", "--agent", agent_ref.get("openclawId") or agent_ref.get("cardId"), "--message", message, "--json"]
    if session_id:
        command.extend(["--session-id", session_id])
    send_timeout_ms = max(int(config["openclaw"].get("timeoutMs", 5000)) * 4, 15000)
    result = run_openclaw_command(config, command, include_remote=False, timeout_override_ms=send_timeout_ms)
    if result.get("ok"):
        return {"ok": True, "result": result.get("data"), "error": None}

    if result.get("timedOut"):
        return {
            "ok": True,
            "result": result.get("data"),
            "error": None,
            "warning": "OpenClaw svarte tregt, men meldingen kan ha blitt sendt.",
            "timedOut": True,
        }

    return {"ok": False, "result": result.get("data"), "error": result.get("error") or "Kunne ikke sende melding"}


def extract_activity(logs_result, limit):
    lines = []
    if isinstance(logs_result, list):
        lines = logs_result
    elif isinstance(logs_result, dict):
        for key in ("items", "entries", "logs", "data"):
            if isinstance(logs_result.get(key), list):
                lines = logs_result[key]
                break

    normalized = []
    for entry in reversed(lines[-max(limit * 2, limit):]):
        if not isinstance(entry, dict):
            continue
        log_payload = entry.get("log") if isinstance(entry.get("log"), dict) else entry
        message = as_text(
            first_truthy(
                log_payload.get("message"),
                log_payload.get("msg"),
                log_payload.get("text"),
                entry.get("raw"),
            ),
            "",
        ).strip()
        if not message:
            continue
        normalized.append(
            {
                "time": compact_value(first_truthy(log_payload.get("time"), log_payload.get("ts"), entry.get("time")), "now"),
                "message": message,
                "level": as_text(first_truthy(log_payload.get("level"), entry.get("type")), "info"),
                "subsystem": as_text(first_truthy(log_payload.get("subsystem"), log_payload.get("logger")), ""),
            }
        )
        if len(normalized) >= limit:
            break
    return normalized


def fallback_activity(gateway_online, channels, cron_jobs, agents_items):
    items = []
    items.append(
        {
            "time": humanize_timestamp(time.time()),
            "message": "Gateway er tilkoblet og dashboardet oppdateres lokalt." if gateway_online else "Gateway svarer ikke akkurat naa.",
            "level": "success" if gateway_online else "error",
            "subsystem": "gateway",
        }
    )
    if agents_items:
        items.append(
            {
                "time": humanize_timestamp(time.time()),
                "message": f"{len(agents_items)} agentkort lastet fra config og OpenClaw.",
                "level": "info",
                "subsystem": "agents",
            }
        )
    for job in cron_jobs[:2]:
        run_time = job.get("lastRun") or job.get("nextRun") or humanize_timestamp(time.time())
        run_phrase = f"siste kjoring: {job.get('lastRun')}" if job.get("lastRun") else f"neste planlagte kjoring: {job.get('nextRun', 'ikke rapportert')}"
        items.append(
            {
                "time": run_time,
                "message": f"{job.get('label', 'Cron Job')} {run_phrase}.",
                "level": "info",
                "subsystem": "cron",
            }
        )
    for channel in channels[:1]:
        items.append(
            {
                "time": humanize_timestamp(time.time()),
                "message": f"{channel.get('label', 'Kanal')} status: {channel.get('status', 'ukjent')}.",
                "level": "success" if "online" in str(channel.get("status", "")).lower() else "info",
                "subsystem": "channel",
            }
        )
    return items[:5]


def build_live_bundle(config):
    live_ttl = refresh_seconds(config, "live", 5000)
    presence_ttl = refresh_seconds(config, "presence", 8000)
    active_sessions_ttl = refresh_seconds(config, "activeSessions", 4000)

    health = cached_openclaw_command(config, "health", ["health", "--json"], ttl_seconds=live_ttl)
    status = cached_openclaw_command(config, "status", ["status", "--json"], ttl_seconds=live_ttl)
    presence = cached_openclaw_command(config, "presence", ["system", "presence", "--json"], ttl_seconds=presence_ttl)
    sessions = cached_openclaw_command(
        config,
        "sessions.active",
        ["sessions", "--all-agents", "--active", "240", "--json"],
        ttl_seconds=active_sessions_ttl,
        include_remote=False,
    )
    return {
        "health": health,
        "status": status,
        "presence": presence,
        "sessions": sessions,
    }


def build_cron_bundle(config):
    cron_ttl = refresh_seconds(config, "cronMetadata", 90000)
    cron = cached_openclaw_command(config, "cron.list", ["cron", "list", "--all", "--json"], ttl_seconds=cron_ttl)
    cron_jobs = extract_cron_jobs(cron.get("data") if cron.get("ok") else [])
    cron_jobs = enrich_cron_jobs_with_runs(config, cron_jobs[:6]) + cron_jobs[6:]
    return {"cron": cron, "cronJobs": sort_cron_jobs(cron_jobs)}


def build_agents_bundle(config):
    agents_ttl = refresh_seconds(config, "agentsMetadata", 120000)
    agents = cached_openclaw_command(
        config,
        "agents.list",
        ["agents", "list", "--json"],
        ttl_seconds=agents_ttl,
        include_remote=False,
    )
    return {"agents": agents}


def build_activity_bundle(config):
    activity_ttl = refresh_seconds(config, "activity", 20000)
    logs = cached_openclaw_command(
        config,
        f"logs:{int(value_at(config, 'dashboard', 'activityItems') or 8)}",
        ["logs", "--json", "--limit", str(int(value_at(config, "dashboard", "activityItems") or 8) * 3)],
        ttl_seconds=activity_ttl,
    )
    return {
        "logs": logs,
        "activity": extract_activity(logs.get("data") if logs.get("ok") else [], int(value_at(config, "dashboard", "activityItems") or 8)),
    }


def build_sessions_history_bundle(config):
    sessions_ttl = refresh_seconds(config, "sessionsHistory", 45000)
    sessions = cached_openclaw_command(
        config,
        "sessions.all",
        ["sessions", "--all-agents", "--json"],
        ttl_seconds=sessions_ttl,
        include_remote=False,
    )
    return {"sessions": sessions}


def build_snapshot(config):
    previous_snapshot = STORE.get() if "STORE" in globals() else {}
    live_bundle = section_value(
        "bundle:live",
        refresh_seconds(config, "live", 5000),
        lambda _previous: build_live_bundle(config),
    )
    cron_bundle = section_value(
        "bundle:cron",
        refresh_seconds(config, "cronMetadata", 90000),
        lambda _previous: build_cron_bundle(config),
    )
    agents_bundle = section_value(
        "bundle:agents",
        refresh_seconds(config, "agentsMetadata", 120000),
        lambda _previous: build_agents_bundle(config),
    )
    activity_bundle = section_value(
        "bundle:activity",
        refresh_seconds(config, "activity", 20000),
        lambda _previous: build_activity_bundle(config),
    )
    sessions_history_bundle = section_value(
        "bundle:sessions-history",
        refresh_seconds(config, "sessionsHistory", 45000),
        lambda _previous: build_sessions_history_bundle(config),
    )

    health = live_bundle.get("health", {})
    status = live_bundle.get("status", {})
    presence = live_bundle.get("presence", {})
    sessions = live_bundle.get("sessions", {})
    cron = cron_bundle.get("cron", {})
    agents = agents_bundle.get("agents", {})
    logs = activity_bundle.get("logs", {})
    sessions_history = sessions_history_bundle.get("sessions", {})

    health_data = health.get("data") if isinstance(health, dict) and health.get("ok") else {}
    status_data = status.get("data") if isinstance(status, dict) and status.get("ok") else {}
    cron_jobs = normalize_list(cron_bundle.get("cronJobs") or [])
    activity_items = normalize_list(activity_bundle.get("activity") or [])
    active_window_seconds = 240
    active_sessions = extract_active_sessions(sessions.get("data") if isinstance(sessions, dict) and sessions.get("ok") else [], active_window_seconds=active_window_seconds)
    recent_sessions = extract_recent_sessions(
        sessions_history.get("data") if isinstance(sessions_history, dict) and sessions_history.get("ok") else []
    )
    agents_items = extract_agents(config, agents.get("data") if agents.get("ok") else [], cron_jobs, activity_items, active_sessions, recent_sessions)
    merge_agent_history(value_at(previous_snapshot, "agents") or [], agents_items)
    channels = augment_channels_with_transports(config, extract_channels(health_data), cron_jobs)
    presence_items = normalize_list(presence.get("data") if presence.get("ok") else [])

    health_summary = extract_health_summary(health_data)
    online = bool(health.get("ok"))
    last_error = None
    for response in (logs, cron, presence, status, health):
        if not response.get("ok"):
            last_error = response.get("error")
            break

    last_cron = first_truthy(
        next((job.get("lastRun") for job in cron_jobs if job.get("lastRun")), None),
        next((job.get("nextRun") for job in cron_jobs if job.get("nextRun")), None),
        "No cron data",
    )

    mode = first_truthy(
        health_summary.get("mode"),
        value_at(status_data, "gateway", "mode"),
        value_at(health_data, "gateway", "mode"),
        "gateway",
    )

    if not activity_items:
        activity_items = fallback_activity(online, channels, cron_jobs, agents_items)

    snapshot = {
        "updatedAt": iso_now(),
        "config": {
            "title": value_at(config, "dashboard", "title") or "OpenClaw Agent Control",
            "subtitle": value_at(config, "dashboard", "subtitle") or "Live status",
        },
        "summary": {
            "gatewayOnline": online,
            "activeAgents": len(agents_items),
            "presenceClients": len(presence_items),
            "lastCron": last_cron,
            "lastError": last_error or "None",
        },
        "gateway": {
            "label": "OpenClaw Gateway",
            "status": "Online" if online else "Offline",
            "detail": as_text(
                first_truthy(
                    value_at(status_data, "gateway", "status"),
                    health_summary.get("status"),
                    mode,
                ),
                "Unknown",
            ),
            "host": urlparse(value_at(config, "openclaw", "gatewayUrl") or "").hostname or "127.0.0.1",
            "url": value_at(config, "openclaw", "gatewayUrl") or "",
            "uptime": as_text(first_truthy(health_summary.get("uptime"), value_at(health_data, "uptimeMs")), "Not reported"),
            "mode": as_text(mode, "gateway"),
        },
        "agents": agents_items,
        "channels": channels,
        "cronJobs": cron_jobs,
        "activity": activity_items,
        "sources": {
            "health": "ok" if health.get("ok") else health.get("error"),
            "status": "ok" if status.get("ok") else status.get("error"),
            "presence": "ok" if presence.get("ok") else presence.get("error"),
            "cron": "ok" if cron.get("ok") else cron.get("error"),
            "agents": "ok" if agents.get("ok") else agents.get("error"),
            "sessions": "ok" if sessions.get("ok") else sessions.get("error"),
            "logs": "ok" if logs.get("ok") else logs.get("error"),
        },
    }
    return snapshot


class SnapshotStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._snapshot = {
            "updatedAt": iso_now(),
            "config": {"title": "OpenClaw Agent Control", "subtitle": "Waiting for first update"},
            "summary": {"gatewayOnline": False, "activeAgents": 0, "presenceClients": 0, "lastCron": "Waiting", "lastError": "None"},
            "gateway": {"label": "OpenClaw Gateway", "status": "Starting", "detail": "Dashboard booting", "host": "127.0.0.1", "url": "", "uptime": "0", "mode": "gateway"},
            "agents": [],
            "channels": [],
            "cronJobs": [],
            "activity": [],
            "sources": {},
        }
        self._subscribers = []

    def get(self):
        with self._lock:
            return self._snapshot

    def subscribe(self):
        q = queue.Queue()
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q):
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    def update(self, snapshot):
        with self._lock:
            current_without_updated = dict(self._snapshot or {})
            next_without_updated = dict(snapshot or {})
            current_without_updated.pop("updatedAt", None)
            next_without_updated.pop("updatedAt", None)
            if next_without_updated == current_without_updated:
                self._snapshot["updatedAt"] = snapshot.get("updatedAt", self._snapshot.get("updatedAt"))
                return
            self._snapshot = snapshot
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            subscriber.put(snapshot)


STORE = SnapshotStore()


def poller():
    while True:
        config = load_config()
        try:
            snapshot = build_snapshot(config)
        except Exception as exc:  # pragma: no cover
            snapshot = STORE.get().copy()
            snapshot["updatedAt"] = iso_now()
            snapshot["summary"] = dict(snapshot.get("summary") or {})
            snapshot["summary"]["lastError"] = str(exc)
            snapshot["gateway"] = dict(snapshot.get("gateway") or {})
            snapshot["gateway"]["status"] = "Error"
            snapshot["gateway"]["detail"] = "Dashboard poll failed"
        STORE.update(snapshot)
        interval_ms = int(value_at(config, "openclaw", "pollIntervalMs") or 5000)
        time.sleep(max(interval_ms / 1000.0, 1.0))


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "OpenClawDashboard/1.0"

    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        parsed = urlparse(self.path)
        request_path = parsed.path

        if request_path in ("/", "/index.html"):
            self.serve_file(PUBLIC_DIR / "index.html")
            return
        if request_path == "/sw.js":
            self.serve_file(PUBLIC_DIR / "sw.js", extra_headers={"Service-Worker-Allowed": "/"})
            return
        if request_path == "/api/snapshot":
            self.serve_json(STORE.get())
            return
        if request_path == "/api/agent-chat":
            params = parse_qs(parsed.query or "")
            agent_token = first_truthy(*(params.get("agent") or []))
            if not agent_token:
                self.serve_json({"error": "Missing agent"}, status=HTTPStatus.BAD_REQUEST)
                return
            fresh = first_truthy(*(params.get("fresh") or [])) in ("1", "true", "yes")
            payload = build_agent_chat_payload(load_config(), agent_token, fresh=fresh)
            self.serve_json(payload)
            return
        if request_path == "/events":
            self.serve_sse()
            return
        if request_path.startswith("/public/"):
            relative = request_path.replace("/public/", "", 1)
            target = (PUBLIC_DIR / relative).resolve()
            if not str(target).startswith(str(PUBLIC_DIR.resolve())):
                self.send_error(HTTPStatus.FORBIDDEN)
                return
            self.serve_file(target)
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/api/agent-chat":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        length = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        try:
            payload = json.loads(raw or "{}")
        except json.JSONDecodeError:
            self.serve_json({"error": "Invalid JSON"}, status=HTTPStatus.BAD_REQUEST)
            return
        agent_token = as_text(payload.get("agent"), "")
        message = as_text(payload.get("message"), "").strip()
        session_id = as_text(payload.get("sessionId"), "")
        if not agent_token or not message:
            self.serve_json({"error": "Missing agent or message"}, status=HTTPStatus.BAD_REQUEST)
            return
        config = load_config()
        send_result = send_agent_message(config, agent_token, message, session_id=session_id)
        chat_payload = build_agent_chat_payload(config, agent_token, fresh=False)
        chat_payload["agent"] = resolve_agent_reference(agent_token)
        chat_payload["sessionId"] = chat_payload.get("sessionId") or session_id
        chat_payload["send"] = send_result
        status = HTTPStatus.OK if send_result.get("ok") else HTTPStatus.BAD_GATEWAY
        self.serve_json(chat_payload, status=status)

    def serve_json(self, payload, status=HTTPStatus.OK):
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def serve_file(self, path, extra_headers=None):
        if not path.exists() or not path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        mime, _ = mimetypes.guess_type(str(path))
        if path.resolve() == (PUBLIC_DIR / "index.html").resolve():
            html = path.read_text(encoding="utf-8")
            css = (PUBLIC_DIR / "styles.css").read_text(encoding="utf-8")
            js = (PUBLIC_DIR / "app.js").read_text(encoding="utf-8")
            html = html.replace('<link rel="stylesheet" href="/public/styles.css" />', f"<style>\n{css}\n</style>")
            html = html.replace('<script src="/public/app.js"></script>', f"<script>\n{js}\n</script>")
            body = html.encode("utf-8")
            mime = "text/html; charset=utf-8"
        else:
            body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime or "application/octet-stream")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def serve_sse(self):
        q = STORE.subscribe()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        def write_event(payload):
            body = f"data: {json.dumps(payload)}\n\n".encode("utf-8")
            self.wfile.write(body)
            self.wfile.flush()

        try:
            write_event(STORE.get())
            while True:
                payload = q.get(timeout=30)
                write_event(payload)
        except (BrokenPipeError, ConnectionResetError, queue.Empty):
            pass
        finally:
            STORE.unsubscribe(q)


def main():
    config = load_config()
    host = value_at(config, "server", "host") or "127.0.0.1"
    port = int(value_at(config, "server", "port") or 3000)

    thread = threading.Thread(target=poller, daemon=True)
    thread.start()

    httpd = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Dashboard listening on http://{host}:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
