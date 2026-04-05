import json
import re
import threading
import time
from pathlib import Path

from openclaw_client import cached_openclaw_command, gateway_call, maybe_cached_command, parse_json_payload, run_openclaw_command
from time_utils import compact_value, epoch_seconds, humanize_timestamp


CHAT_CACHE = {}
CHAT_CACHE_LOCK = threading.Lock()
CHAT_PREFIX_TS_RE = re.compile(r"^\[(?P<stamp>[^\]]+)\]\s*")
CHAT_TEXT_TS_RE = re.compile(r"\[(?P<stamp>[A-Za-z]{3}\s+\d{4}-\d{2}-\d{2}[^\]]*)\]")


def _value_at(data, *path):
    current = data
    for key in path:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current


def _normalize_list(value):
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in ("items", "entries", "agents", "jobs", "rows", "data"):
            nested = value.get(key)
            if isinstance(nested, list):
                return nested
        return list(value.values())
    return []


def _first_truthy(*values):
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _compact_value(value, fallback="Unknown"):
    if value in (None, "", []):
        return fallback
    if isinstance(value, dict):
        parts = []
        for key, nested_value in value.items():
            if nested_value in (None, "", [], {}):
                continue
            text = _compact_value(nested_value, "")
            if text:
                parts.append(f"{key}: {text}")
        return ", ".join(parts) if parts else fallback
    if isinstance(value, list):
        parts = [_compact_value(item, "") for item in value]
        parts = [part for part in parts if part]
        return ", ".join(parts) if parts else fallback
    return str(value)


def _as_text(value, fallback="Unknown"):
    if value in (None, ""):
        return fallback
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, (dict, list)):
        return _compact_value(value, fallback=fallback)
    return str(value)


def _canonical_name(value):
    text = "".join(char for char in str(value).lower() if char.isalnum())
    for suffix in ("agent", "session", "worker"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def _refresh_seconds(config, key, default_ms):
    refresh_ms = _value_at(config, "openclaw", "refreshMs", key)
    try:
        return max(int(refresh_ms or default_ms) / 1000.0, 1.0)
    except (TypeError, ValueError):
        return max(default_ms / 1000.0, 1.0)


def extract_sessions_payload(payload):
    if isinstance(payload, dict) and "result" in payload:
        payload = payload.get("result")
    sessions = _normalize_list(
        _first_truthy(
            _value_at(payload, "sessions"),
            _value_at(payload, "items"),
            _value_at(payload, "entries"),
            _value_at(payload, "rows"),
            payload,
        )
    )
    stores = _normalize_list(_value_at(payload, "stores") or [])
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
        ttl_seconds=_refresh_seconds(config, "sessionsHistory", 45000),
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
        store_agent = _canonical_name(_first_truthy(store.get("agentId"), store.get("agent"), store.get("id")))
        store_path = _first_truthy(store.get("path"), store.get("file"), store.get("sessionsPath"))
        if store_agent and store_path:
            store_paths[store_agent] = str(store_path)

    openclaw_id = _canonical_name(agent_ref.get("openclawId", ""))
    matches = []
    for session in local_sessions:
        if not isinstance(session, dict):
            continue
        session_key = _as_text(_first_truthy(session.get("sessionKey"), session.get("key")), "")
        session_agent = _canonical_name(_first_truthy(session.get("agentId"), session.get("agent"), _value_at(session, "meta", "agentId")))
        if not session_agent and session_key.startswith("agent:"):
            parts = session_key.split(":")
            if len(parts) >= 2:
                session_agent = _canonical_name(parts[1])
        if openclaw_id and session_agent != openclaw_id and openclaw_id not in _canonical_name(session_key):
            continue
        session_copy = dict(session)
        session_id = _as_text(_first_truthy(session.get("id"), session.get("sessionId")), "")
        if session_id and "transcriptPath" not in session_copy:
            search_roots = []
            if session_agent in store_paths:
                search_roots.append(store_paths[session_agent])
            store_hint = _first_truthy(session_copy.get("path"), session_copy.get("file"), session_copy.get("sessionsPath"))
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
    openclaw_id = _canonical_name(agent_ref.get("openclawId", ""))
    all_sessions = {}
    local_sessions = load_local_agent_sessions(config, agent_ref) if include_local else []
    for session in gateway_sessions + local_sessions:
        if not isinstance(session, dict):
            continue
        session_key = _as_text(_first_truthy(session.get("sessionKey"), session.get("key")), "")
        session_id = _as_text(_first_truthy(session.get("id"), session.get("sessionId")), "")
        merge_key = session_key or session_id or str(id(session))
        if merge_key in all_sessions:
            all_sessions[merge_key] = merge_session_records(session, all_sessions[merge_key])
        else:
            all_sessions[merge_key] = dict(session)

    matches = []
    for session in all_sessions.values():
        if not isinstance(session, dict):
            continue
        session_key = _as_text(_first_truthy(session.get("sessionKey"), session.get("key")), "")
        session_agent = _canonical_name(_first_truthy(session.get("agentId"), session.get("agent"), _value_at(session, "meta", "agentId")))
        if not session_agent and session_key.startswith("agent:"):
            parts = session_key.split(":")
            if len(parts) >= 2:
                session_agent = _canonical_name(parts[1])
        if openclaw_id and session_agent != openclaw_id and openclaw_id not in _canonical_name(session_key):
            continue
        matches.append(dict(session))

    def session_sort_key(session):
        return epoch_seconds(
            _first_truthy(
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
        key = _as_text(_first_truthy(session.get("sessionKey"), session.get("key"), session.get("sessionKey")), "")
        kind = _canonical_name(_first_truthy(session.get("kind"), session.get("type"), ""))
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
                parts.append(_as_text(_first_truthy(item.get("text"), item.get("content"), item.get("value")), ""))
        return "\n".join(part for part in parts if part)
    if isinstance(value, dict):
        return _as_text(_first_truthy(value.get("text"), value.get("content"), value.get("value")), "")
    return str(value)


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
    raw_time = _first_truthy(
        entry.get("time"),
        entry.get("ts"),
        entry.get("createdAtMs"),
        entry.get("createdAt"),
        entry.get("updatedAtMs"),
        entry.get("updatedAt"),
        _value_at(entry, "message", "time"),
        _value_at(entry, "payload", "time"),
        _value_at(entry, "meta", "time"),
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
            role = _as_text(_first_truthy(entry.get("role"), _value_at(entry, "message", "role"), entry.get("author")), "").lower()
            text = content_to_text(
                _first_truthy(
                    entry.get("text"),
                    entry.get("content"),
                    entry.get("message"),
                    _value_at(entry, "message", "content"),
                    _value_at(entry, "payload", "text"),
                    _value_at(entry, "payload", "message"),
                )
            ).strip()
            prefixed_time, text = extract_prefixed_chat_timestamp(text)
            inline_time, text = extract_inline_chat_timestamp(text)
            if not role and entry.get("type") in ("user", "assistant", "system"):
                role = entry.get("type")
            if not role:
                entry_type = _as_text(entry.get("type"), "").lower()
                if "assistant" in entry_type:
                    role = "assistant"
                elif "user" in entry_type:
                    role = "user"
                elif "system" in entry_type:
                    role = "system"
            if role not in ("user", "assistant", "system") or not text:
                continue
            display_time, sort_ts = chat_message_timestamp(entry)
            messages.append(
                {
                    "role": role,
                    "text": text,
                    "time": prefixed_time or inline_time or display_time,
                    "sortTs": sort_ts,
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
    for message in _normalize_list(messages):
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


def build_agent_chat_payload(config, agent_token, resolve_agent_reference, fresh=False):
    agent_ref = resolve_agent_reference(agent_token)
    cache_key = _canonical_name(_first_truthy(agent_ref.get("openclawId"), agent_ref.get("cardId"), agent_token))
    cached_chat = get_cached_chat(cache_key)
    cached_payload = cached_chat.get("payload") if isinstance(cached_chat, dict) else {}
    if not fresh and isinstance(cached_payload, dict) and cached_payload.get("messages"):
        payload = dict(cached_payload)
        payload["messages"] = ensure_chat_message_times(payload.get("messages", []))
        payload["agent"] = agent_ref
        return payload
    sessions = load_agent_sessions(config, agent_ref, fresh=fresh, include_local=False)
    session = prefer_agent_session(sessions)
    session_key = _as_text(_first_truthy(session.get("sessionKey"), session.get("key")), "") if isinstance(session, dict) else ""
    gateway_messages = []
    if session_key:
        history_result = gateway_call(config, "chat.history", {"sessionKey": session_key, "limit": 80})
        if history_result.get("ok"):
            rows = _normalize_list(
                _first_truthy(
                    _value_at(history_result.get("data"), "items"),
                    _value_at(history_result.get("data"), "entries"),
                    _value_at(history_result.get("data"), "messages"),
                    history_result.get("data"),
                )
            )
            normalized = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                role = _as_text(_first_truthy(row.get("role"), row.get("author"), row.get("type")), "").lower()
                if role not in ("user", "assistant", "system"):
                    continue
                text = content_to_text(
                    _first_truthy(
                        row.get("text"),
                        row.get("content"),
                        row.get("message"),
                        _value_at(row, "payload", "text"),
                        _value_at(row, "payload", "message"),
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
                session_key = _as_text(_first_truthy(session.get("sessionKey"), session.get("key")), "") if isinstance(session, dict) else ""
    transcript_path = _first_truthy(
        transcript_session.get("transcriptPath") if isinstance(transcript_session, dict) else None,
        transcript_session.get("path") if isinstance(transcript_session, dict) else None,
        transcript_session.get("file") if isinstance(transcript_session, dict) else None,
    )
    transcript_messages = parse_transcript_messages(transcript_path) if transcript_path else []
    messages = ensure_chat_message_times(gateway_messages if gateway_messages else transcript_messages)
    payload = {
        "agent": agent_ref,
        "sessionId": _as_text(_first_truthy(session.get("id"), session.get("sessionId")), "") if isinstance(session, dict) else "",
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


def send_agent_message(config, agent_token, message, resolve_agent_reference, session_id=""):
    agent_ref = resolve_agent_reference(agent_token)
    sessions = load_agent_sessions(config, agent_ref, fresh=True, include_local=False)
    session = prefer_agent_session(sessions)
    session_key = _as_text(_first_truthy(session.get("sessionKey"), session.get("key")), "") if isinstance(session, dict) else ""

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
