# OpenClaw Dashboard

A lightweight local dashboard for OpenClaw Gateway, designed to run on the same Ubuntu server as OpenClaw.

This project works as an **internal control panel** for agents, cron jobs, transports, live activity, and agent interaction.

---

## Screenshots

### Overview  
![OpenClaw Dashboard Overview](docs/images/dashboard-overview.png)

### Hierarchy View  
![OpenClaw Dashboard Hierarchy](docs/images/dashboard-hierarchy.png)

### Agent Chat  
![OpenClaw Dashboard Agent Chat](docs/images/dashboard-chat.png)

---

## Overview

OpenClaw Dashboard gives you:

- A **live overview** of your system  
- A **visual representation** of agents and flows  
- A way to **interact directly with agents**  
- A **local-first control surface** for OpenClaw  

This is not just monitoring.  
It is designed to give you **visibility + control** in one place.

---

## What it does

- Reads local settings from `config.json`
- Fetches status and metadata from OpenClaw via CLI and Gateway
- Displays agents, cron jobs, transports, activity, and chat
- Supports both `Scene` and `Hierarchy` views
- Pushes updates to the frontend using SSE
- Keeps the OpenClaw gateway token server-side
- Works over localhost or internal LAN

---

## Key Features

### Interactive Agent Control
- Click any agent to open a session  
- Chat directly with agents  
- View last output, observations and suggestions  

### Scene View (Visual Layout)
- Drag & drop agents freely  
- Organize your system visually  
- Build your own mental model of the system  

### Hierarchy View (Structured Layout)
- See relationships between:  
  - Main agent  
  - Sub-agents  
  - Cron jobs  
  - Transports  

### Real-Time Updates
- Server-Sent Events (SSE)  
- Live updates without page reload  
- Recent activity timeline  

### Cron & Automation Visibility
- See upcoming jobs  
- Track scheduling  
- Understand flow from agent → cron → transport  

### Transport Visibility
- Supports integrations like Telegram and WhatsApp  
- See where outputs are delivered  
- Confirm system activity  

---

## What you can monitor

- Agent status (sleeping / active)  
- Last task and output  
- Cron schedules  
- Gateway connection  
- Recent activity  
- Errors (if any)  

---

## What you can control

- Open agent sessions  
- Send messages directly to agents  
- Trigger behavior through interaction  
- Organize layout in Scene view  

---

## How it works

The dashboard runs as its own local web server on port `3000`.

OpenClaw Gateway runs separately and typically listens on:

- `127.0.0.1:18789`

The dashboard communicates with OpenClaw through the backend using CLI and Gateway calls.

This means:
- No direct frontend → gateway communication  
- Gateway token stays server-side  

Typical layout:

- Dashboard: `0.0.0.0:3000` or `127.0.0.1:3000`  
- OpenClaw Gateway: `127.0.0.1:18789`  

---

## OpenClaw calls used

The dashboard may use:

- `openclaw health --json`  
- `openclaw status --json`  
- `openclaw system presence --json`  
- `openclaw cron list --all --json`  
- `openclaw agents list --json`  
- `openclaw logs --json`  

Some heavier pollers can be toggled via `config.json`.

On lower-power machines, continuous polling may increase CPU usage.

---

## Setup

```bash
git clone https://github.com/kurtern84/openclaw-agent-dashboard.git
cd openclaw-agent-dashboard
cp config.example.json config.json
python3 server.py
```

---

## Access

### Localhost
```text
http://127.0.0.1:3000
```

### Internal LAN
```text
http://SERVER_IP:3000
```

### SSH tunnel
```bash
ssh -L 18789:127.0.0.1:18789 -L 3000:127.0.0.1:3000 USER@SERVER_IP
```

---

## Example config

```json
{
  "server": {
    "host": "0.0.0.0",
    "port": 3000
  },
  "openclaw": {
    "cliPath": "openclaw",
    "gatewayUrl": "ws://127.0.0.1:18789",
    "token": "SET_GATEWAY_TOKEN_HERE",
    "timeoutMs": 5000,
    "pollIntervalMs": 20000,
    "features": {
      "statusPolling": false,
      "presencePolling": false,
      "activeSessionsPolling": false,
      "sessionsHistoryPolling": false,
      "cronRunsPolling": false,
      "logsPolling": true
    }
  }
}
```

---

## Security principles

- Gateway token is never exposed to frontend  
- Backend handles all OpenClaw communication  
- Intended for local/internal use  
- Gateway should remain localhost-only  

---

## Philosophy

> You should understand and control your system — not just watch it.

No fake automation.  
No hidden processes.  
No “AI magic”.

---

## Status

Actively used in a real setup.  
Not a demo.

---

## License

MIT
