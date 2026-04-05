import time

from time_utils import compact_schedule_value, epoch_seconds, format_elapsed_since, format_ts


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
        sessions = _normalize_list(
            _first_truthy(
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
        updated_raw = _first_truthy(session.get("updatedAt"), session.get("lastMessageAt"), session.get("ts"), session.get("time"))
        updated_epoch = epoch_seconds(updated_raw)
        if updated_epoch is None:
            continue
        if now_ts - updated_epoch > active_window_seconds:
            continue
        key_candidates = [
            _canonical_name(_first_truthy(session.get("agentId"), session.get("agent"), _value_at(session, "meta", "agentId"))),
            _canonical_name(_first_truthy(session.get("sessionKey"), session.get("key"), session.get("id"))),
        ]
        payload = {
            "updatedAt": format_ts(updated_raw),
            "title": _as_text(_first_truthy(session.get("title"), session.get("name")), ""),
        }
        for candidate in key_candidates:
            if candidate:
                active[candidate] = payload
    return active


def extract_recent_sessions(sessions_result):
    sessions = []
    if isinstance(sessions_result, dict):
        sessions = _normalize_list(
            _first_truthy(
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
        updated_raw = _first_truthy(
            session.get("updatedAt"),
            session.get("updatedAtMs"),
            session.get("createdAt"),
            session.get("createdAtMs"),
            session.get("lastMessageAt"),
            session.get("lastMessageAtMs"),
            session.get("ts"),
            session.get("time"),
            _value_at(session, "meta", "updatedAt"),
            _value_at(session, "meta", "updatedAtMs"),
            _value_at(session, "meta", "createdAt"),
            _value_at(session, "meta", "createdAtMs"),
        )
        updated_epoch = epoch_seconds(updated_raw)
        if updated_epoch is None:
            continue

        key = _as_text(_first_truthy(session.get("sessionKey"), session.get("key")), "")
        agent_id = _canonical_name(_first_truthy(session.get("agentId"), session.get("agent"), _value_at(session, "meta", "agentId")))
        if not agent_id and key.startswith("agent:"):
            parts = key.split(":")
            if len(parts) >= 2:
                agent_id = _canonical_name(parts[1])
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
            "title": _as_text(_first_truthy(session.get("title"), session.get("name"), session.get("displayName")), ""),
        }
        previous = recent.get(agent_id)
        if not previous or updated_epoch > previous.get("updatedEpoch", 0):
            recent[agent_id] = payload
    return recent


def extract_recent_cron_session_updates(sessions_result):
    sessions = []
    if isinstance(sessions_result, dict):
        sessions = _normalize_list(
            _first_truthy(
                sessions_result.get("sessions"),
                sessions_result.get("items"),
                sessions_result.get("entries"),
                sessions_result,
            )
        )
    elif isinstance(sessions_result, list):
        sessions = sessions_result

    updates = []
    for session in sessions:
        if not isinstance(session, dict):
            continue
        key = _as_text(_first_truthy(session.get("sessionKey"), session.get("key")), "")
        if not (":cron:" in key or key.startswith("cron:")):
            continue
        updated_raw = _first_truthy(
            session.get("updatedAt"),
            session.get("updatedAtMs"),
            session.get("createdAt"),
            session.get("createdAtMs"),
            session.get("lastMessageAt"),
            session.get("lastMessageAtMs"),
            session.get("ts"),
            session.get("time"),
            _value_at(session, "meta", "updatedAt"),
            _value_at(session, "meta", "updatedAtMs"),
            _value_at(session, "meta", "createdAt"),
            _value_at(session, "meta", "createdAtMs"),
        )
        if updated_raw and epoch_seconds(updated_raw) is not None:
            updates.append(format_ts(updated_raw))
    return updates


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

    label = _as_text(_first_truthy(item.get("label"), item.get("name"), item.get("id")), fallback_label)
    raw_status = _as_text(_first_truthy(item.get("status"), item.get("state"), item.get("enabled") and "Enabled"), "Unknown")
    last_task = _as_text(
        _first_truthy(
            item.get("lastTask"),
            item.get("lastAction"),
            item.get("lastMessage"),
            item.get("description"),
        ),
        "No recent task",
    )
    schedule = compact_schedule_value(_first_truthy(item.get("nextRun"), item.get("schedule"), item.get("next")), "Not scheduled")
    presence = derive_agent_presence(raw_status, last_task, schedule)
    return {
        "id": _as_text(_first_truthy(item.get("id"), label.lower().replace(" ", "-"))),
        "label": label,
        "name": _as_text(_first_truthy(item.get("name"), item.get("displayName")), ""),
        "openclawId": _as_text(_first_truthy(item.get("id"), item.get("agentId")), ""),
        "cronJobIds": [],
        "status": presence["label"],
        "statusCode": presence["code"],
        "statusEmoji": presence["emoji"],
        "lastTask": last_task,
        "nextRun": schedule,
        "model": _as_text(_first_truthy(item.get("model"), item.get("defaultModel"), item.get("llm")), "Unknown"),
    }


def attach_agent_context(agent, cron_jobs, logs, active_sessions, recent_sessions):
    label = _canonical_name(agent["label"])
    agent_id = _canonical_name(agent["id"])
    agent_name = _canonical_name(agent.get("name", ""))
    openclaw_id = _canonical_name(agent.get("openclawId", ""))
    cron_job_ids = {str(job_id) for job_id in _normalize_list(agent.get("cronJobIds")) if job_id}

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
        haystack = _canonical_name(
            f"{job.get('label', '')} {job.get('id', '')} {job.get('agentId', '')} {job.get('message', '')} {job.get('sessionKey', '')}"
        )
        direct_job_match = str(job.get("id", "")) in cron_job_ids
        if direct_job_match or (label and label in haystack) or (agent_id and agent_id in haystack) or (agent_name and agent_name in haystack) or (openclaw_id and openclaw_id in haystack):
            agent["nextRun"] = _first_truthy(job.get("nextRun"), job.get("lastRun"), agent.get("nextRun"))
            job_model = _as_text(_first_truthy(job.get("model"), job.get("provider")), "")
            if job_model:
                agent["model"] = job_model
            when = _first_truthy(job.get("lastRun"), job.get("runAt"))
            if when:
                action = job.get("runAction") or "finished"
                status = job.get("runStatus") or "ok"
                agent["lastTask"] = f"Cron {action} {when} ({status})"
            break
    for entry in logs:
        haystack = _canonical_name(f"{entry.get('message', '')} {entry.get('subsystem', '')}")
        if (label and label in haystack) or (agent_id and agent_id in haystack) or (agent_name and agent_name in haystack):
            agent["lastTask"] = entry.get("message") or agent["lastTask"]
            break


def extract_agents(config, agents_result, cron_jobs, logs, active_sessions, recent_sessions):
    configured_cards = _normalize_list(_value_at(config, "dashboard", "agentCards"))
    raw_agents = _normalize_list(agents_result)
    normalized = []
    configured_keys = {_canonical_name(_first_truthy(card.get("id"), card.get("label"), "")) for card in configured_cards}
    matched_indexes = set()

    for item_index, card in enumerate(configured_cards):
        label = _as_text(card.get("label"), "Agent")
        configured_name = _as_text(card.get("name"), "")
        configured_openclaw_id = _as_text(card.get("openclawId"), "")
        configured_cron_job_ids = [str(job_id) for job_id in _normalize_list(card.get("cronJobIds")) if job_id]
        card_id = _as_text(_first_truthy(card.get("id"), label.lower().replace(" ", "-")))
        match = None
        for raw_index, item in enumerate(raw_agents):
            if not isinstance(item, dict):
                continue
            raw_keys = {
                _canonical_name(item.get("id")),
                _canonical_name(item.get("name")),
                _canonical_name(item.get("label")),
            }
            raw_keys.discard("")
            if _canonical_name(card_id) in raw_keys or _canonical_name(label) in raw_keys:
                match = item
                matched_indexes.add(raw_index)
                break
        agent = normalize_agent_record(match, label)
        agent["id"] = card_id
        agent["label"] = label
        agent["name"] = configured_name or agent.get("name", "")
        agent["openclawId"] = configured_openclaw_id or agent.get("openclawId", "")
        agent["cronJobIds"] = configured_cron_job_ids
        attach_agent_context(agent, cron_jobs, logs, active_sessions, recent_sessions)
        normalized.append(agent)

    for raw_index, item in enumerate(raw_agents):
        if not isinstance(item, dict):
            continue
        if raw_index in matched_indexes:
            continue
        raw_keys = {
            _canonical_name(item.get("id")),
            _canonical_name(item.get("name")),
            _canonical_name(item.get("label")),
        }
        raw_keys.discard("")
        if configured_cards and raw_keys & configured_keys:
            continue
        agent = normalize_agent_record(item, "Agent")
        attach_agent_context(agent, cron_jobs, logs, active_sessions, recent_sessions)
        normalized.append(agent)

    return normalized


def merge_agent_history(previous_agents, current_agents):
    previous_by_key = {}
    for agent in _normalize_list(previous_agents):
        if not isinstance(agent, dict):
            continue
        keys = {
            _canonical_name(agent.get("id", "")),
            _canonical_name(agent.get("label", "")),
            _canonical_name(agent.get("name", "")),
            _canonical_name(agent.get("openclawId", "")),
        }
        keys.discard("")
        for key in keys:
            previous_by_key[key] = agent

    for agent in current_agents:
        if not isinstance(agent, dict):
            continue
        keys = [
            _canonical_name(agent.get("id", "")),
            _canonical_name(agent.get("label", "")),
            _canonical_name(agent.get("name", "")),
            _canonical_name(agent.get("openclawId", "")),
        ]
        previous = next((previous_by_key.get(key) for key in keys if key and previous_by_key.get(key)), None)
        if not previous:
            continue
        if agent.get("lastTask") in ("No recent task", "No live metadata yet"):
            preserved = _as_text(previous.get("lastTask"), "")
            if preserved and preserved not in ("No recent task", "No live metadata yet"):
                agent["lastTask"] = preserved
