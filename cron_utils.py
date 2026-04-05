from openclaw_client import cached_openclaw_command
from time_utils import compact_schedule_value, compact_value, epoch_seconds, format_ts


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


def _canonical_name(value):
    text = "".join(char for char in str(value).lower() if char.isalnum())
    for suffix in ("agent", "session", "worker"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def _as_text(value, fallback="Unknown"):
    if value in (None, ""):
        return fallback
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, (dict, list)):
        return compact_value(value, fallback=fallback)
    return str(value)


def _refresh_seconds(config, key, default_ms):
    refresh_ms = _value_at(config, "openclaw", "refreshMs", key)
    try:
        return max(int(refresh_ms or default_ms) / 1000.0, 1.0)
    except (TypeError, ValueError):
        return max(default_ms / 1000.0, 1.0)


def mentions_whatsapp(*values):
    haystack = _canonical_name(" ".join(str(value or "") for value in values))
    return "whatsapp" in haystack


def title_case_transport(value):
    text = str(value or "").strip()
    if not text:
        return "Transport"
    if _canonical_name(text) == "whatsapp":
        return "WhatsApp"
    return text.replace("-", " ").replace("_", " ").title()


def sort_cron_jobs(cron_jobs):
    def sort_key(job):
        transport = _canonical_name(_first_truthy(job.get("deliveryChannel"), job.get("transport"), "zz"))
        agent = _canonical_name(job.get("agentId", "zz"))
        next_run = _as_text(_first_truthy(job.get("nextRun"), job.get("lastRun")), "zz")
        label = _as_text(job.get("label"), "zz")
        return (transport, agent, next_run, label)

    return sorted(cron_jobs, key=sort_key)


def extract_cron_jobs(cron_result):
    jobs = _normalize_list(cron_result)
    normalized = []
    for item in jobs:
        if not isinstance(item, dict):
            continue
        schedule_obj = item.get("schedule") if isinstance(item.get("schedule"), dict) else {}
        payload_obj = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        delivery_obj = item.get("delivery") if isinstance(item.get("delivery"), dict) else {}
        normalized.append(
            {
                "id": _as_text(_first_truthy(item.get("id"), item.get("name")), "cron-job"),
                "label": _as_text(_first_truthy(item.get("name"), item.get("label")), "Cron Job"),
                "status": _as_text(_first_truthy(item.get("status"), item.get("state"), "Scheduled")),
                "agentId": _as_text(_first_truthy(item.get("agentId"), item.get("agent"), schedule_obj.get("agentId")), ""),
                "nextRunRaw": _first_truthy(
                    item.get("nextRunAt"),
                    item.get("nextRunAtMs"),
                    item.get("nextRun"),
                    schedule_obj.get("at"),
                    item.get("next"),
                ),
                "nextRun": compact_schedule_value(
                    _first_truthy(
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
                "lastRun": compact_value(_first_truthy(item.get("lastRunAt"), item.get("lastRun"), item.get("lastSuccess")), ""),
                "message": _as_text(_first_truthy(item.get("message"), payload_obj.get("message"), payload_obj.get("text"), payload_obj.get("systemEvent")), ""),
                "deliveryChannel": _as_text(_first_truthy(delivery_obj.get("channel"), item.get("channel"), item.get("transport")), ""),
                "deliveryTarget": _as_text(_first_truthy(delivery_obj.get("to"), delivery_obj.get("target"), item.get("target")), ""),
                "deliveryMode": _as_text(_first_truthy(delivery_obj.get("mode"), item.get("mode")), ""),
                "model": _as_text(_first_truthy(item.get("model"), payload_obj.get("model")), ""),
                "provider": _as_text(_first_truthy(item.get("provider"), payload_obj.get("provider")), ""),
                "kind": "cron",
            }
        )
    return normalized


def parse_run_timestamp(run):
    return _first_truthy(
        run.get("runAtMs"),
        run.get("runAt"),
        run.get("updatedAtMs"),
        run.get("updatedAt"),
        run.get("createdAtMs"),
        run.get("createdAt"),
        run.get("endedAt"),
        run.get("finishedAt"),
        run.get("completedAt"),
        run.get("startedAt"),
        run.get("time"),
        run.get("ts"),
        _value_at(run, "meta", "updatedAt"),
        _value_at(run, "meta", "updatedAtMs"),
        _value_at(run, "meta", "createdAt"),
        _value_at(run, "meta", "createdAtMs"),
        _value_at(run, "result", "endedAt"),
        _value_at(run, "result", "finishedAt"),
        _value_at(run, "result", "completedAt"),
        _value_at(run, "result", "updatedAt"),
        _value_at(run, "result", "updatedAtMs"),
        _value_at(run, "result", "createdAt"),
        _value_at(run, "result", "createdAtMs"),
    )


def extract_latest_run_entry(runs_data):
    runs = []
    if isinstance(runs_data, dict):
        runs = _normalize_list(
            _first_truthy(
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
    cron_runs_ttl = _refresh_seconds(config, "cronRuns", 180000)
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
                        job_copy["lastRunRaw"] = finished_at
                        job_copy["lastRun"] = compact_value(finished_at, job_copy.get("lastRun") or "")
                    run_at = _first_truthy(latest.get("runAtMs"), latest.get("runAt"), latest.get("ts"))
                    if run_at:
                        job_copy["runAt"] = compact_value(run_at, "")
                    action = _as_text(_first_truthy(latest.get("action"), latest.get("status")), "")
                    status = _as_text(latest.get("status"), "")
                    if action:
                        job_copy["runAction"] = action
                    if status:
                        job_copy["runStatus"] = status
                    model = _as_text(_first_truthy(latest.get("model"), _value_at(latest, "result", "model")), "")
                    provider = _as_text(_first_truthy(latest.get("provider"), _value_at(latest, "result", "provider")), "")
                    if model:
                        job_copy["model"] = model
                    if provider:
                        job_copy["provider"] = provider
                    summary = _first_truthy(
                        latest.get("summary"),
                        latest.get("message"),
                        latest.get("status"),
                        _value_at(latest, "result", "summary"),
                        _value_at(latest, "result", "status"),
                        _value_at(latest, "payload", "message"),
                    )
                    if summary:
                        job_copy["message"] = _as_text(summary, job_copy.get("message") or "")
                    error_text = _as_text(latest.get("error"), "")
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
                    session_key = _as_text(latest.get("sessionKey"), "")
                    if session_key:
                        job_copy["sessionKey"] = session_key
                        session_parts = session_key.split(":")
                        if len(session_parts) >= 2:
                            job_copy["agentId"] = job_copy.get("agentId") or session_parts[1]
        enriched.append(job_copy)
    return enriched


def augment_channels_with_transports(config, channels, cron_jobs):
    whatsapp_config = _value_at(config, "dashboard", "whatsapp") or {}
    configured_whatsapp_ids = {
        str(job_id)
        for job_id in _normalize_list(whatsapp_config.get("cronJobIds") or [])
        if job_id
    }

    transport_jobs = []
    for job in cron_jobs:
        if not isinstance(job, dict):
            continue
        explicit_transport = _canonical_name(_first_truthy(job.get("deliveryChannel"), job.get("transport"), ""))
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
        label = _as_text(whatsapp_config.get("label"), "WhatsApp") if transport == "whatsapp" else title_case_transport(transport)
        node = {
            "id": f"{transport}-{job.get('id', 'link')}",
            "label": label,
            "status": "Online",
            "detail": "",
        }
        node["kind"] = "whatsapp" if transport == "whatsapp" else "transport"
        node["transport"] = transport
        node["agentId"] = _as_text(job.get("agentId"), "")
        node["relatedAgentIds"] = [_as_text(job.get("agentId"), "")]
        node["detail"] = f"Cron: {job.get('label', 'Transport Job')}"
        node["relatedCronId"] = job.get("id")
        node["relatedCronLabel"] = job.get("label")
        node["relatedCronIds"] = [job.get("id")] if job.get("id") else []
        if _as_text(job.get("runStatus"), "").lower() in ("error", "failed", "fail"):
            node["status"] = "Attention"
        else:
            node["status"] = node.get("status") or "Online"
        transport_nodes.append(node)
    return transport_nodes + channels
