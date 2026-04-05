import json
import subprocess
import threading
import time


COMMAND_CACHE = {}
COMMAND_CACHE_LOCK = threading.Lock()


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
