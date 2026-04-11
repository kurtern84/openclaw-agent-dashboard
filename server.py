#!/usr/bin/env python3
import json
import mimetypes
import queue
import threading
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from agents_utils import (
    derive_agent_presence,
    extract_active_sessions,
    extract_agents,
    extract_recent_cron_session_updates,
    extract_recent_sessions,
    merge_agent_history,
)
from chat_utils import (
    build_agent_chat_payload,
    merge_chat_messages,
    send_agent_message,
)
from cron_utils import (
    augment_channels_with_transports,
    enrich_cron_jobs_with_runs,
    extract_cron_jobs,
    extract_latest_run_entry,
    mentions_whatsapp,
    parse_run_timestamp,
    sort_cron_jobs,
    title_case_transport,
)
from openclaw_client import (
    cached_openclaw_command,
    gateway_call,
    maybe_cached_command,
    parse_json_payload,
    run_openclaw_command,
)
from time_utils import (
    compact_schedule_value,
    compact_value,
    compute_next_run_from_expr,
    epoch_seconds,
    format_cron_expr,
    format_elapsed_since,
    format_ts,
    humanize_schedule_timestamp,
    humanize_timestamp,
)

mimetypes.add_type("application/manifest+json", ".webmanifest")
mimetypes.add_type("image/svg+xml", ".svg")


BASE_DIR = Path(__file__).resolve().parent
PUBLIC_DIR = BASE_DIR / "public"
CONFIG_PATH = BASE_DIR / "config.json"
SECTION_CACHE = {}
SECTION_CACHE_LOCK = threading.Lock()


DEFAULT_CONFIG = {
    "server": {"host": "0.0.0.0", "port": 3000},
    "openclaw": {
        "cliPath": "openclaw",
        "gatewayUrl": "ws://127.0.0.1:18789",
        "token": "",
        "timeoutMs": 5000,
        "pollIntervalMs": 15000,
        "refreshMs": {
            "live": 15000,
            "presence": 60000,
            "activeSessions": 10000,
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


def canonical_name(value):
    text = "".join(char for char in str(value).lower() if char.isalnum())
    for suffix in ("agent", "session", "worker"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


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
    live_ttl = refresh_seconds(config, "live", 15000)
    active_sessions_ttl = refresh_seconds(config, "activeSessions", 10000)

    health = cached_openclaw_command(config, "health", ["health", "--json"], ttl_seconds=live_ttl)
    status = cached_openclaw_command(config, "status", ["status", "--json"], ttl_seconds=live_ttl)
    # Presence is best-effort only. On this machine the `openclaw system`
    # subprocess appears to be the main CPU/temperature offender, so keep
    # presence disabled in the live path and fall back to an empty list.
    presence = {"ok": True, "data": []}
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
    recent_cron_session_updates = extract_recent_cron_session_updates(
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

    last_run_raw_values = []
    for job in cron_jobs:
        candidate = first_truthy(job.get("lastRunRaw"), job.get("runAtRaw"), job.get("runAt"), job.get("lastRun"))
        if candidate and epoch_seconds(candidate) is not None:
            last_run_raw_values.append(candidate)
    last_run_raw_values.extend(value for value in recent_cron_session_updates if epoch_seconds(value) is not None)
    last_run_raw_values.extend(
        session.get("updatedAt")
        for session in recent_sessions.values()
        if isinstance(session, dict) and session.get("kind") == "cron" and epoch_seconds(session.get("updatedAt")) is not None
    )
    next_run_raw_values = []
    for job in cron_jobs:
        candidate = first_truthy(job.get("nextRunRaw"), job.get("nextRun"))
        if candidate and epoch_seconds(candidate) is not None:
            next_run_raw_values.append(candidate)
            continue
        expr = as_text(job.get("scheduleExpr"), "")
        if expr:
            derived_next = compute_next_run_from_expr(expr)
            if derived_next and epoch_seconds(derived_next) is not None:
                next_run_raw_values.append(derived_next)
    latest_last_run = None
    if last_run_raw_values:
        latest_last_run = max(last_run_raw_values, key=lambda value: epoch_seconds(value) or 0)
    nearest_next_run = None
    if next_run_raw_values:
        nearest_next_run = min(next_run_raw_values, key=lambda value: epoch_seconds(value) or float("inf"))
    last_cron = format_elapsed_since(latest_last_run) or "No recent cron"
    next_cron = humanize_schedule_timestamp(nearest_next_run) or "Not scheduled"

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
            "nextCron": next_cron,
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
            "summary": {
                "gatewayOnline": False,
                "activeAgents": 0,
                "presenceClients": 0,
                "lastCron": "Waiting",
                "nextCron": "Waiting",
                "lastError": "None",
            },
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
            payload = build_agent_chat_payload(load_config(), agent_token, resolve_agent_reference, fresh=fresh)
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
        send_result = send_agent_message(config, agent_token, message, resolve_agent_reference, session_id=session_id)
        chat_payload = build_agent_chat_payload(config, agent_token, resolve_agent_reference, fresh=False)
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
