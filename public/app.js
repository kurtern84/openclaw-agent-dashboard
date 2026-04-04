const SVG_NS = "http://www.w3.org/2000/svg";

const summaryBar = document.getElementById("summary-bar");
const agentColumn = document.getElementById("agent-column");
const nodeStack = document.getElementById("node-stack");
const activityList = document.getElementById("activity-list");
const chatModalShell = document.getElementById("chat-modal-shell");
const chatCloseBtn = document.getElementById("chat-close");
const chatModalTitle = document.getElementById("chat-modal-title");
const chatModalSubtitle = document.getElementById("chat-modal-subtitle");
const chatThread = document.getElementById("chat-thread");
const chatForm = document.getElementById("chat-form");
const chatInput = document.getElementById("chat-input");
const chatSendBtn = document.getElementById("chat-send");
const chatHint = document.getElementById("chat-hint");

const titleEl = document.getElementById("dashboard-title");
const subtitleEl = document.getElementById("dashboard-subtitle");
const gatewayLabelEl = document.getElementById("gateway-label");
const gatewayStatusEl = document.getElementById("gateway-status");
const gatewayDetailEl = document.getElementById("gateway-detail");
const updatedAtEl = document.getElementById("updated-at");
const stageEl = document.querySelector(".stage");
const stageSceneEl = document.getElementById("stage-scene");
const hubEl = document.getElementById("gateway-hub");
const connectionLayer = document.querySelector(".connection-layer");
const viewToggleEl = document.querySelector(".view-toggle");

const summaryItemTemplate = document.getElementById("summary-item-template");
const agentCardTemplate = document.getElementById("agent-card-template");
const nodeTemplate = document.getElementById("node-template");
const activityTemplate = document.getElementById("activity-template");

const sceneState = {
  items: [],
  running: false,
  paths: new Map(),
  scale: 1,
  offsets: {},
  drag: null,
  viewMode: "scene",
  hierarchyClones: []
};

const chatState = {
  agentId: "",
  sessionId: "",
  timer: null,
  refreshTimeout: null,
  replyWatchTimer: null,
  requestId: 0,
  inFlight: false,
  sending: false,
  signature: "",
  waitingForReply: false
};
let pendingSnapshot = null;
const SCENE_LAYOUT_KEY = "openclaw-dashboard-layout-v1";
const SCENE_VIEW_KEY = "openclaw-dashboard-view-v1";

function loadSceneLayout() {
  try {
    const raw = localStorage.getItem(SCENE_LAYOUT_KEY);
    const parsed = raw ? JSON.parse(raw) : {};
    sceneState.scale = Math.max(0.45, Math.min(2.1, Number(parsed.scale) || 1));
    sceneState.offsets = parsed.offsets && typeof parsed.offsets === "object" ? parsed.offsets : {};
  } catch {
    sceneState.scale = 1;
    sceneState.offsets = {};
  }
}

function loadViewMode() {
  try {
    const saved = localStorage.getItem(SCENE_VIEW_KEY);
    sceneState.viewMode = saved === "hierarchy" ? "hierarchy" : "scene";
  } catch {
    sceneState.viewMode = "scene";
  }
}

function saveViewMode() {
  try {
    localStorage.setItem(SCENE_VIEW_KEY, sceneState.viewMode);
  } catch {}
}

function applyViewMode() {
  stageEl.classList.toggle("hierarchy-mode", sceneState.viewMode === "hierarchy");
  if (sceneState.viewMode !== "hierarchy") {
    clearHierarchyClones();
  }
  if (viewToggleEl) {
    [...viewToggleEl.querySelectorAll("[data-view-mode]")].forEach((button) => {
      button.classList.toggle("is-active", button.dataset.viewMode === sceneState.viewMode);
    });
  }
}

function saveSceneLayout() {
  localStorage.setItem(
    SCENE_LAYOUT_KEY,
    JSON.stringify({
      scale: sceneState.scale,
      offsets: sceneState.offsets
    })
  );
}

function applySceneScale() {
  stageEl.style.setProperty("--scene-scale", String(sceneState.scale));
  stageSceneEl.style.setProperty("--scene-scale", String(sceneState.scale));
}

function clearHierarchyClones() {
  (sceneState.hierarchyClones || []).forEach((el) => el.remove());
  sceneState.hierarchyClones = [];
}

function createHierarchyClone(sourceEl, cloneKey) {
  const clone = sourceEl.cloneNode(true);
  clone.dataset.slot = `${sourceEl.dataset.slot || "node"}-${cloneKey}`;
  clone.dataset.clone = "hierarchy";
  clone.classList.add("hierarchy-clone");
  clone.style.display = "";
  clone.style.left = "";
  clone.style.top = "";
  clone.style.transform = "";
  stageSceneEl.appendChild(clone);
  sceneState.hierarchyClones.push(clone);
  return clone;
}

function getSceneScale() {
  return sceneState.scale || 1;
}

function getManualOffset(slot) {
  const saved = sceneState.offsets?.[slot];
  return {
    x: Number(saved?.x) || 0,
    y: Number(saved?.y) || 0
  };
}

function setManualOffset(slot, offset) {
  sceneState.offsets[slot] = {
    x: Math.round(offset.x),
    y: Math.round(offset.y)
  };
  saveSceneLayout();
}

function statusTone(value) {
  const text = String(value || "").toLowerCase();
  if (text.includes("online") || text.includes("ok") || text.includes("success")) return "good";
  if (text.includes("offline") || text.includes("error") || text.includes("fail")) return "bad";
  return "warn";
}

function agentTone(agent) {
  const code = String(agent?.statusCode || "").toLowerCase();
  if (code === "working") return "good";
  if (code === "error" || code === "offline") return "bad";
  if (code === "sleeping" || code === "idle") return "warn";
  return statusTone(agent?.status);
}

function formatClock(input) {
  if (!input) return "Ikke oppdatert";
  const date = new Date(input);
  if (Number.isNaN(date.getTime())) return String(input);
  return date.toLocaleTimeString("no-NO", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  });
}

function setText(element, value, fallback = "Ikke tilgjengelig") {
  element.textContent = value || fallback;
}

function transportIconMarkup(item) {
  const label = String(item.label || "").toLowerCase();
  const transport = String(item.transport || "").toLowerCase();
  if (item.kind === "whatsapp" || transport === "whatsapp" || label.includes("whatsapp")) {
    return `
      <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
        <path d="M12.05 3.2a8.72 8.72 0 0 0-7.51 13.16L3.2 20.8l4.56-1.3a8.72 8.72 0 1 0 4.29-16.3Zm0 1.82a6.9 6.9 0 0 1 5.91 10.47 6.9 6.9 0 0 1-7.78 3.08l-.43-.13-2.66.76.79-2.59-.15-.45a6.9 6.9 0 0 1 4.32-9.14Zm-3.2 3.45c-.23 0-.6.08-.91.42-.31.34-1.18 1.15-1.18 2.8s1.21 3.25 1.38 3.48c.17.23 2.36 3.78 5.82 5.15 2.86 1.13 3.45.9 4.08.84.63-.06 2.03-.83 2.32-1.63.29-.8.29-1.48.2-1.63-.09-.15-.34-.23-.71-.42-.37-.19-2.18-1.1-2.52-1.23-.34-.13-.59-.19-.83.19-.24.38-.94 1.23-1.15 1.48-.21.25-.42.28-.79.09-.37-.19-1.56-.58-2.97-1.84-1.1-.99-1.84-2.2-2.05-2.57-.21-.38-.02-.58.16-.77.16-.16.37-.42.56-.63.19-.21.25-.35.37-.58.12-.23.06-.44-.03-.63-.09-.19-.83-2.15-1.14-2.94-.3-.76-.6-.78-.83-.79h-.71Z"/>
      </svg>
    `;
  }
  if (transport === "telegram" || label.includes("telegram")) {
    return `
      <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
        <path d="M20.67 4.34c.64-.25 1.27.29 1.1.95l-2.84 13.26c-.14.65-.86.96-1.44.62l-4.33-2.53-2.21 2.12c-.3.29-.82.11-.87-.31l-.33-3.16 7.33-6.62c.22-.2-.07-.53-.32-.37l-9.06 5.72-3.88-1.28c-.71-.23-.75-1.22-.06-1.52l16.91-6.88Z"/>
      </svg>
    `;
  }
  return "";
}

function renderSummary(summary) {
  summaryBar.innerHTML = "";
  const items = [
    { label: "Gateway", value: summary.gatewayOnline ? "Online" : "Offline", tone: summary.gatewayOnline ? "good" : "bad" },
    { label: "Active Agents", value: String(summary.activeAgents ?? 0), tone: "good" },
    { label: "Presence Clients", value: String(summary.presenceClients ?? 0), tone: "warn" },
    { label: "Last Cron", value: summary.lastCron || "Unknown", tone: "warn" },
    { label: "Next Cron", value: summary.nextCron || "Unknown", tone: "warn" },
    { label: "Last Error", value: summary.lastError || "None", tone: summary.lastError && summary.lastError !== "None" ? "bad" : "good" }
  ];

  items.forEach((item) => {
    const node = summaryItemTemplate.content.firstElementChild.cloneNode(true);
    node.dataset.tone = item.tone;
    node.querySelector(".summary-label").textContent = item.label;
    node.querySelector(".summary-value").textContent = item.value;
    summaryBar.appendChild(node);
  });
}

function renderAgents(agents) {
  agentColumn.innerHTML = "";
  if (!agents.length) {
    const empty = document.createElement("article");
    empty.className = "agent-card empty";
    empty.innerHTML = "<h2>Ingen agentkort ennå</h2><p>Legg til agenter i config.json eller vent til OpenClaw svarer på <code>agents list</code>.</p>";
    agentColumn.appendChild(empty);
    return;
  }

  agents.forEach((agent) => {
    const node = agentCardTemplate.content.firstElementChild.cloneNode(true);
    node.dataset.tone = agentTone(agent);
    node.dataset.presence = agent.statusCode || "idle";
    node.dataset.slot = agent.id || agent.label.toLowerCase();
    node.dataset.openclawId = agent.openclawId || "";
    node.dataset.agentName = agent.name || "";
    node.dataset.cronJobIds = (agent.cronJobIds || []).join(",");
    node.dataset.orbitType = "agent";
    setText(node.querySelector(".agent-title"), agent.label);
    setText(node.querySelector(".agent-name"), agent.name, "");
    setText(node.querySelector(".agent-presence-emoji"), agent.statusEmoji, "🟢");
    setText(node.querySelector(".agent-presence-text"), agent.status, "Idle");
    setText(node.querySelector(".agent-model"), agent.model, "Unknown");
    setText(node.querySelector(".agent-status"), agent.status);
    setText(node.querySelector(".agent-task"), agent.lastTask);
    setText(node.querySelector(".agent-next"), agent.nextRun);
    const chatButton = node.querySelector(".agent-chat-button");
    chatButton.textContent = agent.statusCode === "working" ? "Chat" : "Wake";
    chatButton.dataset.agent = agent.id || agent.openclawId || agent.label;
    agentColumn.appendChild(node);
  });
}

function renderNodes(channels, cronJobs) {
  nodeStack.innerHTML = "";
  const featuredChannels = channels.filter((item) => item.kind === "whatsapp" || item.kind === "transport");
  const regularChannels = channels.filter((item) => item.kind !== "whatsapp" && item.kind !== "transport");
  const items = [
    ...featuredChannels.map((item) => ({ ...item, accent: "channel" })),
    ...cronJobs.map((item) => ({ ...item, accent: "cron" })),
    ...regularChannels.map((item) => ({ ...item, accent: "channel" }))
  ];

  if (!items.length) {
    const empty = document.createElement("article");
    empty.className = "node-pill empty";
    empty.innerHTML = "<strong>Ingen kanaldata</strong><p>Dashboardet venter p&#229; svar fra <code>health</code> og <code>cron list</code>.</p>";
    nodeStack.appendChild(empty);
    return;
  }

  items.forEach((item) => {
    const node = nodeTemplate.content.firstElementChild.cloneNode(true);
    node.dataset.kind = item.kind || item.accent || "channel";
    node.dataset.transport = item.transport || "";
    node.dataset.tone = statusTone(item.status);
    node.dataset.slot = item.id || item.label.toLowerCase();
    node.dataset.agentId = item.agentId || "";
    node.dataset.relatedAgentIds = (item.relatedAgentIds || []).join(",");
    node.dataset.relatedCronId = item.relatedCronId || "";
    node.dataset.relatedCronIds = (item.relatedCronIds || []).join(",");
    node.dataset.orbitType = "node";
    setText(
      node.querySelector(".node-kind"),
      item.kind === "cron" ? "Cron Job" : (item.kind === "whatsapp" || item.kind === "transport") ? "Transport" : "Channel",
      ""
    );
    const icon = node.querySelector(".node-icon");
    if (icon) {
      icon.innerHTML = transportIconMarkup(item);
    }
    setText(node.querySelector(".node-title"), item.label);
    const detail = item.kind === "cron"
      ? `${item.nextRun || "No next run"}`
      : (item.kind === "whatsapp" || item.kind === "transport")
        ? `${item.status || "Unknown"}`
        : `${item.status || "Unknown"}${item.detail ? ` · ${item.detail}` : ""}`;
    setText(node.querySelector(".node-detail"), detail);
    nodeStack.appendChild(node);
  });
}

function renderActivity(items) {
  activityList.innerHTML = "";
  if (!items.length) {
    const empty = document.createElement("p");
    empty.className = "activity-empty";
    empty.textContent = "Ingen aktivitet enda. Prøv å sende en kommando i OpenClaw eller sjekk at `openclaw logs --json` fungerer.";
    activityList.appendChild(empty);
    return;
  }

  items.slice(0, 3).forEach((item) => {
    const node = activityTemplate.content.firstElementChild.cloneNode(true);
    node.dataset.tone = statusTone(item.level);
    setText(node.querySelector(".activity-time"), formatClock(item.time), "Nå");
    setText(node.querySelector(".activity-message"), item.message);
    activityList.appendChild(node);
  });
}

function makePath(id, accent = "cyan") {
  if (sceneState.paths.has(id)) {
    const existing = sceneState.paths.get(id);
    existing.group.style.display = "";
    return existing;
  }

  const group = document.createElementNS(SVG_NS, "g");
  const path = document.createElementNS(SVG_NS, "path");
  const dotA = document.createElementNS(SVG_NS, "circle");
  const dotB = document.createElementNS(SVG_NS, "circle");
  path.setAttribute("class", `beam beam-${accent}`);
  dotA.setAttribute("class", `beam-dot beam-dot-${accent}`);
  dotB.setAttribute("class", `beam-dot beam-dot-${accent}`);
  dotA.setAttribute("r", "0.95");
  dotB.setAttribute("r", "0.65");
  group.appendChild(path);
  group.appendChild(dotA);
  group.appendChild(dotB);
  connectionLayer.appendChild(group);
  const record = { group, path, dotA, dotB };
  sceneState.paths.set(id, record);
  return record;
}

function makeLinkPath(id, accent = "lime") {
  return makePath(`link-${id}`, accent);
}

function clearPaths() {
  connectionLayer.innerHTML = "";
  sceneState.paths.clear();
}

function isDraggableElement(target) {
  return target.closest(".agent-card, .node-pill");
}

function distributeOnArc(count, { startX, endX, startY, endY, bow = 0, accent }) {
  const items = [];
  if (count <= 0) return items;
  if (count === 1) {
    return [{ anchorX: (startX + endX) / 2, anchorY: (startY + endY) / 2, driftX: 0, driftY: 4, accent }];
  }
  for (let index = 0; index < count; index += 1) {
    const t = index / (count - 1);
    const x = startX + (endX - startX) * t;
    const y = startY + (endY - startY) * t;
    const arcOffset = Math.sin(t * Math.PI) * bow;
    items.push({
      anchorX: x + arcOffset,
      anchorY: y,
      driftX: 0,
      driftY: 4,
      accent
    });
  }
  return items;
}

function collectSceneItems() {
  const items = [];
  const agentElements = [...agentColumn.querySelectorAll(".agent-card")];
  const nodeElements = [...nodeStack.querySelectorAll(".node-pill")];
  const transportElements = nodeElements.filter((el) => el.dataset.kind === "whatsapp" || el.dataset.kind === "transport");
  const cronElements = nodeElements.filter((el) => el.dataset.kind === "cron");
  const channelElements = nodeElements.filter((el) => el.dataset.kind !== "whatsapp" && el.dataset.kind !== "transport" && el.dataset.kind !== "cron");

  const agentDefs = distributeOnArc(agentElements.length, { startX: 0.30, endX: 0.22, startY: 0.18, endY: 0.74, bow: -0.07, accent: "cyan" });
  const transportDefs = distributeOnArc(transportElements.length, { startX: 0.88, endX: 0.90, startY: 0.28, endY: 0.56, bow: 0.02, accent: "lime" });
  const cronDefs = distributeOnArc(cronElements.length, { startX: 0.66, endX: 0.80, startY: 0.18, endY: 0.78, bow: 0.08, accent: "gold" });
  const channelDefs = distributeOnArc(channelElements.length, { startX: 0.78, endX: 0.82, startY: 0.56, endY: 0.82, bow: 0.02, accent: "gold" });

  agentElements.forEach((el, index) => {
    const def = agentDefs[index] || { anchorX: 0.24, anchorY: 0.4, driftX: 0, driftY: 4, accent: "cyan" };
    items.push({ id: `agent-${index}`, el, ...def, type: "agent" });
  });

  let nodeIndex = 0;
  transportElements.forEach((el, index) => {
    const def = transportDefs[index] || { anchorX: 0.89, anchorY: 0.4, driftX: 0, driftY: 3, accent: "lime" };
    items.push({ id: `node-${nodeIndex++}`, el, ...def, type: "node" });
  });
  cronElements.forEach((el, index) => {
    const def = cronDefs[index] || { anchorX: 0.78, anchorY: 0.5, driftX: 0, driftY: 4, accent: "gold" };
    items.push({ id: `node-${nodeIndex++}`, el, ...def, type: "node" });
  });
  channelElements.forEach((el, index) => {
    const def = channelDefs[index] || { anchorX: 0.82, anchorY: 0.7, driftX: 0, driftY: 4, accent: "gold" };
    items.push({ id: `node-${nodeIndex++}`, el, ...def, type: "node" });
  });

  sceneState.items = items;
  clearPaths();
  items.forEach((item) => makePath(item.id, item.accent));
}

function distributeEvenly(count, start, end) {
  if (count <= 0) return [];
  if (count === 1) return [(start + end) / 2];
  return Array.from({ length: count }, (_, index) => start + ((end - start) * index) / (count - 1));
}

function isPrimaryAgentItem(item) {
  const values = [
    item.el.dataset.slot || "",
    item.el.querySelector(".agent-title")?.textContent || "",
    item.el.querySelector(".agent-name")?.textContent || ""
  ]
    .join(" ")
    .toLowerCase();
  return values.includes("main") || values.includes("nexsus") || values.includes("ceo");
}

function canonicalKey(...values) {
  return values
    .filter(Boolean)
    .join(" ")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, "")
    .trim();
}

function extractTransportCronLabel(nodeItem) {
  const detail = nodeItem.el.querySelector(".node-detail")?.textContent || "";
  const match = detail.match(/cron:\s*(.+)$/i);
  return match ? canonicalKey(match[1]) : "";
}

function scheduleAnchor(value) {
  const text = (value || "").trim();
  if (!text) return "";
  const explicitDateTime = text.match(/\b\d{4}-\d{2}-\d{2}\s+kl\s+\d{2}:\d{2}\b/i);
  if (explicitDateTime) return canonicalKey(explicitDateTime[0]);
  const relativeDateTime = text.match(/\b(i dag|i morgen)\s+kl\s+\d{2}:\d{2}\b/i);
  if (relativeDateTime) return canonicalKey(relativeDateTime[0]);
  const clockOnly = text.match(/\b\d{2}:\d{2}\b/);
  if (clockOnly) return canonicalKey(clockOnly[0]);
  return canonicalKey(text.split("·")[0]);
}

function stageLocal(point, sceneRect, logicalWidth, logicalHeight) {
  const safeWidth = sceneRect.width || logicalWidth || 1;
  const safeHeight = sceneRect.height || logicalHeight || 1;
  return {
    x: (point.x - sceneRect.left) * (logicalWidth / safeWidth),
    y: (point.y - sceneRect.top) * (logicalHeight / safeHeight)
  };
}

function edgePoint(start, end, offset) {
  const dx = end.x - start.x;
  const dy = end.y - start.y;
  const length = Math.hypot(dx, dy) || 1;
  return {
    x: start.x + (dx / length) * offset,
    y: start.y + (dy / length) * offset
  };
}

function edgePointForRect(center, target, rect, padding = 4) {
  const dx = target.x - center.x;
  const dy = target.y - center.y;
  const halfW = Math.max((rect.width || 0) / 2 + padding, 1);
  const halfH = Math.max((rect.height || 0) / 2 + padding, 1);

  if (Math.abs(dx) < 0.001 && Math.abs(dy) < 0.001) {
    return { x: center.x, y: center.y };
  }

  const scaleX = dx === 0 ? Infinity : halfW / Math.abs(dx);
  const scaleY = dy === 0 ? Infinity : halfH / Math.abs(dy);
  const scale = Math.min(scaleX, scaleY);

  return {
    x: center.x + dx * scale,
    y: center.y + dy * scale
  };
}

function updateConnection(item, hubCenter, cardCenter, stageRect, live, timeMs) {
  const { path, dotA, dotB } = makePath(item.id, item.accent);
  const start = edgePoint(hubCenter, cardCenter, 156);
  const end = edgePoint(cardCenter, hubCenter, 86);
  const ctrl = {
    x: (start.x + end.x) / 2,
    y: (start.y + end.y) / 2 + Math.sin(timeMs / 1200 + (item.anchorX || 0.5) * 10) * 18
  };
  const d = `M ${start.x} ${start.y} Q ${ctrl.x} ${ctrl.y} ${end.x} ${end.y}`;
  path.setAttribute("d", d);
  path.style.opacity = live ? "0.9" : "0.45";
  path.style.strokeDasharray = live ? "8 10" : "6 12";
  path.style.strokeDashoffset = live ? `${-(timeMs / 70) % 200}` : "0";

  const travelA = ((timeMs / 1900) + (item.anchorX || 0.5)) % 1;
  const travelB = ((timeMs / 2600) + (item.anchorY || 0.5) + 0.33) % 1;

  const moveDot = (dot, t, active) => {
    const qx = (1 - t) * (1 - t) * start.x + 2 * (1 - t) * t * ctrl.x + t * t * end.x;
    const qy = (1 - t) * (1 - t) * start.y + 2 * (1 - t) * t * ctrl.y + t * t * end.y;
    dot.setAttribute("cx", qx.toFixed(2));
    dot.setAttribute("cy", qy.toFixed(2));
    dot.style.opacity = active ? "1" : "0.18";
  };

  moveDot(dotA, live ? travelA : 0.08, live);
  moveDot(dotB, live ? travelB : 0.16, live && (item.type === "agent" || item.type === "node"));
}

function updateNodeLink(sourceEl, targetEl, sceneRect, logicalWidth, logicalHeight, timeMs, accent = "lime") {
  if (!sourceEl || !targetEl) return;
  const sourceRect = sourceEl.getBoundingClientRect();
  const targetRect = targetEl.getBoundingClientRect();
  const sourceCenter = stageLocal({
    x: sourceRect.left + sourceRect.width / 2,
    y: sourceRect.top + sourceRect.height / 2
  }, sceneRect, logicalWidth, logicalHeight);
  const targetCenter = stageLocal({
    x: targetRect.left + targetRect.width / 2,
    y: targetRect.top + targetRect.height / 2
  }, sceneRect, logicalWidth, logicalHeight);
  const logicalSourceRect = {
    width: sourceRect.width * (logicalWidth / (sceneRect.width || logicalWidth || 1)),
    height: sourceRect.height * (logicalHeight / (sceneRect.height || logicalHeight || 1))
  };
  const logicalTargetRect = {
    width: targetRect.width * (logicalWidth / (sceneRect.width || logicalWidth || 1)),
    height: targetRect.height * (logicalHeight / (sceneRect.height || logicalHeight || 1))
  };
  const from = edgePointForRect(sourceCenter, targetCenter, logicalSourceRect, 6);
  const to = edgePointForRect(targetCenter, sourceCenter, logicalTargetRect, 6);
  const ctrl = {
    x: (from.x + to.x) / 2 + 16,
    y: (from.y + to.y) / 2 - 22
  };
  const linkId = `${sourceEl.dataset.slot || "whatsapp"}-${targetEl.dataset.slot || "target"}`;
  const { path, dotA, dotB } = makeLinkPath(linkId, accent);
  const d = `M ${from.x} ${from.y} Q ${ctrl.x} ${ctrl.y} ${to.x} ${to.y}`;
  path.setAttribute("d", d);
  path.style.opacity = "0.82";
  path.style.strokeDasharray = "7 9";
  path.style.strokeDashoffset = `${-(timeMs / 80) % 160}`;

  const moveDot = (dot, t) => {
    const qx = (1 - t) * (1 - t) * from.x + 2 * (1 - t) * t * ctrl.x + t * t * to.x;
    const qy = (1 - t) * (1 - t) * from.y + 2 * (1 - t) * t * ctrl.y + t * t * to.y;
    dot.setAttribute("cx", qx.toFixed(2));
    dot.setAttribute("cy", qy.toFixed(2));
    dot.style.opacity = "0.95";
  };

  moveDot(dotA, ((timeMs / 1800) % 1));
  moveDot(dotB, ((timeMs / 2300) + 0.33) % 1);
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function layoutAgentItems(stageRect, topReserved, bottomPadding) {
  const agentItems = sceneState.items.filter((item) => item.type === "agent");
  if (!agentItems.length) return new Map();

  const availableTop = topReserved + 12;
  const availableBottom = stageRect.height - bottomPadding - 12;
  const heights = agentItems.map((item) => item.el.getBoundingClientRect().height);
  const maxHeight = Math.max(...heights, 0);
  const rowHeight = maxHeight + 20;
  const rows = Math.ceil(agentItems.length / 2);
  const span = rowHeight * rows;
  let cursorY = clamp(
    availableTop + (availableBottom - availableTop - span) / 2,
    availableTop,
    Math.max(availableTop, availableBottom - span)
  );

  const positions = new Map();
  for (let row = 0; row < rows; row += 1) {
    const leftIndex = row * 2;
    const rightIndex = leftIndex + 1;
    const rowItems = [agentItems[leftIndex], agentItems[rightIndex]].filter(Boolean);
    const t = rows === 1 ? 0.5 : row / (rows - 1);
    const curve = Math.sin(t * Math.PI);
    const outerX = 0.16 + curve * 0.03;
    const innerX = 0.29 + curve * 0.04;

    rowItems.forEach((item, column) => {
      const rect = item.el.getBoundingClientRect();
      const offsetY = (rowHeight - rect.height) / 2;
      const laneX = column === 0 ? outerX : innerX;
      positions.set(item.id, {
        x: stageRect.width * laneX - rect.width / 2,
        y: cursorY + offsetY
      });
    });

    cursorY += rowHeight;
  }
  return positions;
}

function layoutHierarchyScene(timeMs = performance.now()) {
  clearHierarchyClones();
  const stageWidth = stageEl.clientWidth;
  let stageHeight = stageEl.clientHeight;
  const logicalTop = 72;
  const sidePadding = 44;
  const bottomPadding = 36;
  const rowGap = 92;
  const childGap = 22;
  let maxBottom = 0;
  const lineJobs = [];

  stageEl.style.minHeight = "900px";
  stageEl.style.height = "900px";
  stageSceneEl.style.minHeight = "900px";
  stageSceneEl.style.height = "100%";

  const agentItems = sceneState.items.filter((item) => item.type === "agent");
  const nodeItems = sceneState.items.filter((item) => item.type === "node");
  const transportItems = nodeItems.filter((item) => item.el.dataset.kind === "whatsapp" || item.el.dataset.kind === "transport");
  const cronItems = nodeItems.filter((item) => item.el.dataset.kind === "cron");
  const placed = new Set();
  agentItems.forEach((item) => {
    item.el.style.display = "";
  });
  nodeItems.forEach((item) => {
    item.el.style.display = "none";
  });
  const primaryAgent = agentItems.find(isPrimaryAgentItem) || agentItems[0] || null;
  const otherAgents = agentItems.filter((item) => item !== primaryAgent);
  const agentByKey = new Map();
  const agentByCronId = new Map();
  const agentBySchedule = new Map();
  const cronById = new Map();
  const cronByTitle = new Map();

  if (primaryAgent) {
    const width = primaryAgent.el.offsetWidth;
    const height = primaryAgent.el.offsetHeight;
    const x = clamp(stageWidth * 0.5 - width / 2, sidePadding, stageWidth - width - sidePadding);
    const y = Math.max(logicalTop, logicalTop + 18);
    primaryAgent.el.style.left = `${x}px`;
    primaryAgent.el.style.top = `${y}px`;
    primaryAgent.el.style.transform = "translate3d(0,0,0)";
    placed.add(primaryAgent.id);
    maxBottom = Math.max(maxBottom, y + height);
    [
      primaryAgent.el.dataset.slot,
      primaryAgent.el.dataset.openclawId,
      primaryAgent.el.querySelector(".agent-title")?.textContent,
      primaryAgent.el.dataset.agentName
    ].forEach((value) => {
      const key = canonicalKey(value);
      if (key) agentByKey.set(key, primaryAgent);
    });
    const scheduleKey = scheduleAnchor(primaryAgent.el.querySelector(".agent-next")?.textContent || "");
    if (scheduleKey) {
      const list = agentBySchedule.get(scheduleKey) || [];
      list.push(primaryAgent);
      agentBySchedule.set(scheduleKey, list);
    }
    (primaryAgent.el.dataset.cronJobIds || "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean)
      .forEach((cronId) => agentByCronId.set(cronId, primaryAgent));
  }

  const primaryBottom = primaryAgent
    ? parseFloat(primaryAgent.el.style.top || "0") + primaryAgent.el.offsetHeight
    : logicalTop + 120;
  const secondaryTop = primaryBottom + rowGap;
  const agentWidths = otherAgents.map((item) => item.el.offsetWidth);
  const maxAgentWidth = Math.max(...agentWidths, primaryAgent ? primaryAgent.el.offsetWidth : 220, 220);
  const columnGap = 64;
  const totalColumnsWidth = otherAgents.length
    ? (otherAgents.length * maxAgentWidth) + ((otherAgents.length - 1) * columnGap)
    : 0;
  const startX = Math.max(sidePadding, (stageWidth - totalColumnsWidth) / 2);
  const fallbackSecondaryXs = distributeEvenly(otherAgents.length, 0.2, 0.8);

  otherAgents.forEach((item, index) => {
    const width = item.el.offsetWidth;
    const height = item.el.offsetHeight;
    const centeredInColumn = totalColumnsWidth
      ? startX + (index * (maxAgentWidth + columnGap)) + (maxAgentWidth - width) / 2
      : stageWidth * fallbackSecondaryXs[index] - width / 2;
    const x = clamp(centeredInColumn, sidePadding, stageWidth - width - sidePadding);
    const y = Math.max(logicalTop, secondaryTop);
    item.el.style.left = `${x}px`;
    item.el.style.top = `${y}px`;
    item.el.style.transform = "translate3d(0,0,0)";
    placed.add(item.id);
    maxBottom = Math.max(maxBottom, y + height);
    [
      item.el.dataset.slot,
      item.el.dataset.openclawId,
      item.el.querySelector(".agent-title")?.textContent,
      item.el.dataset.agentName
    ].forEach((value) => {
      const key = canonicalKey(value);
      if (key) agentByKey.set(key, item);
    });
    const scheduleKey = scheduleAnchor(item.el.querySelector(".agent-next")?.textContent || "");
    if (scheduleKey) {
      const list = agentBySchedule.get(scheduleKey) || [];
      list.push(item);
      agentBySchedule.set(scheduleKey, list);
    }
    (item.el.dataset.cronJobIds || "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean)
      .forEach((cronId) => agentByCronId.set(cronId, item));
    if (primaryAgent) {
      lineJobs.push([primaryAgent.el, item.el, "cyan"]);
    }
  });

  const hierarchyNodeGroups = new Map();
  const attachToGroup = (agentItem, nodeItem) => {
    if (!agentItem) return;
    const list = hierarchyNodeGroups.get(agentItem.id) || [];
    list.push(nodeItem);
    hierarchyNodeGroups.set(agentItem.id, list);
  };

  const cronResolvedAgent = new Map();
  const transportsByCronIdGlobal = new Map();

  const resolveAgentForNode = (nodeItem) => {
    const cronId = nodeItem.el.dataset.slot || "";
    if (cronId && agentByCronId.has(cronId)) {
      return agentByCronId.get(cronId);
    }
    const direct = canonicalKey(nodeItem.el.dataset.agentId);
    if (direct && agentByKey.has(direct)) return agentByKey.get(direct);
    const viaCron = (nodeItem.el.dataset.relatedCronIds || nodeItem.el.dataset.relatedCronId || "")
      .split(",")
      .map((value) => value.trim())
      .map((relatedCronId) => cronResolvedAgent.get(relatedCronId) || agentByCronId.get(relatedCronId))
      .find(Boolean);
    if (viaCron) return viaCron;
    if (nodeItem.el.dataset.kind === "cron") {
      const scheduleKey = scheduleAnchor(nodeItem.el.querySelector(".node-detail")?.textContent || "");
      const scheduleMatches = agentBySchedule.get(scheduleKey) || [];
      if (scheduleMatches.length === 1) return scheduleMatches[0];
    }
    if (nodeItem.el.dataset.kind === "whatsapp" || nodeItem.el.dataset.kind === "transport") {
      const titleKey = extractTransportCronLabel(nodeItem);
      const matchedCron = titleKey ? cronByTitle.get(titleKey) : null;
      if (matchedCron) {
        const viaCronTitle = cronResolvedAgent.get(matchedCron.el.dataset.slot || "");
        if (viaCronTitle) return viaCronTitle;
      }
    }
    const related = (nodeItem.el.dataset.relatedAgentIds || "")
      .split(",")
      .map((value) => canonicalKey(value))
      .find((value) => value && agentByKey.has(value));
    if (related) return agentByKey.get(related);
    return null;
  };

  cronItems.forEach((item) => {
    if (item.el.dataset.slot) {
      cronById.set(item.el.dataset.slot, item);
    }
    const titleKey = canonicalKey(item.el.querySelector(".node-title")?.textContent || "");
    if (titleKey) {
      cronByTitle.set(titleKey, item);
    }
    const match = resolveAgentForNode(item);
    if (match) {
      attachToGroup(match, item);
      if (item.el.dataset.slot) {
        cronResolvedAgent.set(item.el.dataset.slot, match);
      }
    }
  });
  transportItems.forEach((item) => {
    const explicitCronIds = (item.el.dataset.relatedCronIds || item.el.dataset.relatedCronId || "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean);
    const titleCronKey = extractTransportCronLabel(item);
    const titleCron = titleCronKey ? cronByTitle.get(titleCronKey) : null;
    const allCronIds = [...new Set([...explicitCronIds, ...(titleCron?.el.dataset.slot ? [titleCron.el.dataset.slot] : [])])];
    allCronIds.forEach((cronId) => {
      const list = transportsByCronIdGlobal.get(cronId) || [];
      list.push(item);
      transportsByCronIdGlobal.set(cronId, list);
    });
    const match = resolveAgentForNode(item);
    if (match) {
      attachToGroup(match, item);
    }
  });

  hierarchyNodeGroups.forEach((items, agentId) => {
    const agentItem = agentItems.find((entry) => entry.id === agentId);
    if (!agentItem) return;
    const agentLeft = parseFloat(agentItem.el.style.left || "0");
    const agentTop = parseFloat(agentItem.el.style.top || "0");
    const agentWidth = agentItem.el.offsetWidth;
    const agentHeight = agentItem.el.offsetHeight;
    const transportGroup = items.filter((item) => item.el.dataset.kind === "whatsapp" || item.el.dataset.kind === "transport");
    const cronGroup = items.filter((item) => item.el.dataset.kind === "cron");
    const looseTransports = [];
    transportGroup.forEach((item) => {
      const explicitCronIds = (item.el.dataset.relatedCronIds || item.el.dataset.relatedCronId || "")
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
      const titleCronKey = extractTransportCronLabel(item);
      const titleCron = titleCronKey ? cronByTitle.get(titleCronKey) : null;
      const hasCronLink = explicitCronIds.length > 0 || Boolean(titleCron?.el.dataset.slot);
      if (!hasCronLink) {
        looseTransports.push(item);
      }
    });
    const centerX = agentLeft + agentWidth / 2;
    let currentTop = agentTop + agentHeight + 44;

    cronGroup.forEach((item) => {
      const renderedCron = createHierarchyClone(item.el, `${agentId}-${item.id}-cron`);
      const width = renderedCron.offsetWidth;
      const height = renderedCron.offsetHeight;
      const x = clamp(centerX - width / 2, sidePadding, stageWidth - width - sidePadding);
      const y = Math.max(logicalTop, currentTop);
      renderedCron.style.left = `${x}px`;
      renderedCron.style.top = `${y}px`;
      renderedCron.style.transform = "translate3d(0,0,0)";
      renderedCron.style.display = "";
      placed.add(item.id);
      lineJobs.push([agentItem.el, renderedCron, "cyan"]);
      maxBottom = Math.max(maxBottom, y + height);
      currentTop += height + childGap;

      const childTransports = transportsByCronIdGlobal.get(item.el.dataset.slot || "") || [];
      childTransports.forEach((transportItem) => {
        const renderedTransport = createHierarchyClone(
          transportItem.el,
          `${agentId}-${item.id}-${transportItem.id}-transport`
        );
        const transportWidth = renderedTransport.offsetWidth;
        const transportHeight = renderedTransport.offsetHeight;
        const transportX = clamp(centerX - transportWidth / 2, sidePadding, stageWidth - transportWidth - sidePadding);
        const transportY = Math.max(logicalTop, currentTop);
        renderedTransport.style.left = `${transportX}px`;
        renderedTransport.style.top = `${transportY}px`;
        renderedTransport.style.transform = "translate3d(0,0,0)";
        renderedTransport.style.display = "";
        placed.add(transportItem.id);
        lineJobs.push([renderedCron, renderedTransport, "cyan"]);
        maxBottom = Math.max(maxBottom, transportY + transportHeight);
        currentTop += transportHeight + childGap;
      });
    });

    looseTransports.forEach((item) => {
      const renderedTransport = createHierarchyClone(item.el, `${agentId}-${item.id}-transport`);
      const width = renderedTransport.offsetWidth;
      const height = renderedTransport.offsetHeight;
      const x = clamp(centerX - width / 2, sidePadding, stageWidth - width - sidePadding);
      const y = Math.max(logicalTop, currentTop);
      renderedTransport.style.left = `${x}px`;
      renderedTransport.style.top = `${y}px`;
      renderedTransport.style.transform = "translate3d(0,0,0)";
      renderedTransport.style.display = "";
      placed.add(item.id);
      lineJobs.push([agentItem.el, renderedTransport, "cyan"]);
      maxBottom = Math.max(maxBottom, y + height);
      currentTop += height + childGap;
    });
  });

  sceneState.items.forEach((item) => {
    if (item.type !== "agent" || placed.has(item.id)) return;
    const width = item.el.offsetWidth;
    const height = item.el.offsetHeight;
    item.el.style.left = `${stageWidth / 2 - width / 2}px`;
    item.el.style.top = `${stageHeight / 2 - height / 2}px`;
    item.el.style.transform = "translate3d(0,0,0)";
  });

  nodeItems.forEach((item) => {
    item.el.style.display = "none";
  });

  const requiredHeight = Math.max(900, Math.ceil(maxBottom + bottomPadding + 60));
  stageEl.style.minHeight = `${requiredHeight}px`;
  stageEl.style.height = `${requiredHeight}px`;
  stageSceneEl.style.minHeight = `${requiredHeight}px`;
  stageSceneEl.style.height = `${requiredHeight}px`;
  stageHeight = requiredHeight;

  connectionLayer.setAttribute("viewBox", `0 0 ${stageWidth} ${stageHeight}`);
  connectionLayer.setAttribute("width", String(stageWidth));
  connectionLayer.setAttribute("height", String(stageHeight));
  const sceneRect = stageSceneEl.getBoundingClientRect();
  lineJobs.forEach(([sourceEl, targetEl, accent]) => {
    updateNodeLink(sourceEl, targetEl, sceneRect, stageWidth, stageHeight, timeMs, accent);
  });
}

function layoutScene(timeMs = performance.now()) {
  const portraitLike = window.innerHeight > window.innerWidth * 1.05;
  if (window.innerWidth <= 1180 || portraitLike) {
    stageEl.classList.add("stack-mode");
    stageEl.classList.toggle("portrait-mode", portraitLike);
    return;
  }

  if (sceneState.viewMode === "hierarchy") {
    stageEl.classList.remove("stack-mode");
    stageEl.classList.remove("portrait-mode");
    layoutHierarchyScene(timeMs);
    return;
  }

  clearHierarchyClones();
  stageEl.style.height = "";

  stageEl.classList.remove("stack-mode");
  stageEl.classList.remove("portrait-mode");
  const stageRect = stageEl.getBoundingClientRect();
  const sceneRect = stageSceneEl.getBoundingClientRect();
  const stageWidth = stageEl.clientWidth;
  const stageHeight = stageEl.clientHeight;
  const hubRect = hubEl.getBoundingClientRect();
  const hubCenter = stageLocal({
    x: hubRect.left + hubRect.width / 2,
    y: hubRect.top + hubRect.height / 2
  }, sceneRect, stageWidth, stageHeight);
  const live = stageEl.dataset.live === "true";
  const topReserved = 150;
  const sidePadding = 24;
  const bottomPadding = 24;
  const dragOverflowX = Math.max(180, stageWidth * 0.18);
  const dragOverflowY = Math.max(180, stageHeight * 0.18);
  const logicalStageRect = { ...stageRect, width: stageWidth, height: stageHeight };
  const agentPositions = layoutAgentItems(logicalStageRect, topReserved, bottomPadding);

  connectionLayer.setAttribute("viewBox", `0 0 ${stageWidth} ${stageHeight}`);
  connectionLayer.setAttribute("width", String(stageWidth));
  connectionLayer.setAttribute("height", String(stageHeight));
  sceneState.paths.forEach((record) => {
    record.group.style.display = "none";
  });
  sceneState.items.forEach((item) => {
    item.el.style.display = "";
  });

  sceneState.items.forEach((item, index) => {
    if (agentPositions.has(item.id)) {
      const slot = item.el.dataset.slot || item.id;
      const manualOffset = getManualOffset(slot);
      const elWidth = item.el.offsetWidth;
      const elHeight = item.el.offsetHeight;
      const preset = agentPositions.get(item.id);
      const floatY = Math.cos(timeMs / (1700 + index * 140) + index) * 3;
      const cardX = clamp(
        preset.x + manualOffset.x,
        sidePadding - dragOverflowX,
        stageWidth - elWidth - sidePadding + dragOverflowX
      );
      const cardY = clamp(
        preset.y + floatY + manualOffset.y,
        topReserved - dragOverflowY,
        stageHeight - elHeight - bottomPadding + dragOverflowY
      );
      item.el.style.left = `${cardX}px`;
      item.el.style.top = `${cardY}px`;
      item.el.style.transform = "translate3d(0,0,0)";

      const cardCenter = {
        x: cardX + elWidth / 2,
        y: cardY + elHeight / 2
      };
      updateConnection(item, hubCenter, cardCenter, sceneRect, live, timeMs);
      return;
    }
    const slot = item.el.dataset.slot || item.id;
    const manualOffset = getManualOffset(slot);
    const elWidth = item.el.offsetWidth;
    const elHeight = item.el.offsetHeight;
    const floatX = Math.sin(timeMs / (2200 + index * 120) + index) * (item.driftX || 0);
    const floatY = Math.cos(timeMs / (1700 + index * 140) + index) * (item.driftY || 0);
    const targetX = stageWidth * item.anchorX - elWidth / 2 + floatX + manualOffset.x;
    const targetY = topReserved + (stageHeight - topReserved - bottomPadding) * item.anchorY - elHeight / 2 + floatY + manualOffset.y;
    const cardX = clamp(
      targetX,
      sidePadding - dragOverflowX,
      stageWidth - elWidth - sidePadding + dragOverflowX
    );
    const cardY = clamp(
      targetY,
      topReserved - dragOverflowY,
      stageHeight - elHeight - bottomPadding + dragOverflowY
    );
    item.el.style.left = `${cardX}px`;
    item.el.style.top = `${cardY}px`;
    item.el.style.transform = "translate3d(0,0,0)";

    const cardCenter = {
      x: cardX + elWidth / 2,
      y: cardY + elHeight / 2
    };
    if (item.el.dataset.kind !== "whatsapp" && item.el.dataset.kind !== "transport") {
      updateConnection(item, hubCenter, cardCenter, sceneRect, live, timeMs);
    }
  });

  const transportNodes = [...nodeStack.querySelectorAll('.node-pill[data-kind="whatsapp"], .node-pill[data-kind="transport"]')];
  transportNodes.forEach((transportNode) => {
    const relatedCronIds = (transportNode.dataset.relatedCronIds || transportNode.dataset.relatedCronId || "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean);
    relatedCronIds.forEach((cronId) => {
      const cronNode = nodeStack.querySelector(`.node-pill[data-slot="${CSS.escape(cronId)}"]`);
      if (cronNode) {
        updateNodeLink(transportNode, cronNode, sceneRect, stageWidth, stageHeight, timeMs);
      }
    });
  });
}

function startSceneEngine() {
  if (sceneState.running) return;
  sceneState.running = true;

  const tick = (timeMs) => {
    if (sceneState.viewMode === "scene") {
      layoutScene(timeMs);
    }
    requestAnimationFrame(tick);
  };

  requestAnimationFrame(tick);
}

function refreshScene() {
  collectSceneItems();
  layoutScene(performance.now());
}

function startDrag(target, clientX, clientY) {
  const draggable = isDraggableElement(target);
  if (!draggable) return;
  if (target.closest(".agent-chat-button")) return;
  const slot = draggable.dataset.slot;
  if (!slot) return;
  const current = getManualOffset(slot);
  sceneState.drag = {
    slot,
    startX: clientX,
    startY: clientY,
    offsetX: current.x,
    offsetY: current.y
  };
  stageEl.classList.add("is-dragging");
}

function moveDrag(clientX, clientY) {
  if (!sceneState.drag) return;
  const scale = getSceneScale();
  const dx = (clientX - sceneState.drag.startX) / scale;
  const dy = (clientY - sceneState.drag.startY) / scale;
  setManualOffset(sceneState.drag.slot, {
    x: sceneState.drag.offsetX + dx,
    y: sceneState.drag.offsetY + dy
  });
  layoutScene(performance.now());
}

function stopDrag() {
  if (!sceneState.drag) return;
  sceneState.drag = null;
  stageEl.classList.remove("is-dragging");
}

function extractInlineChatTimestamp(text) {
  const raw = String(text || "");
  if (!raw) return { time: "", text: "" };
  const lines = raw.split("\n");
  const timeIndex = lines.findIndex((line) => /^\[[^\]]+\]\s*$/.test(line.trim()));
  if (timeIndex === -1) {
    return { time: "", text: raw };
  }
  const time = lines[timeIndex].trim().replace(/^\[|\]$/g, "");
  const cleaned = lines.filter((_, index) => index !== timeIndex).join("\n").trim();
  return { time, text: cleaned };
}

function formatChatTimestamp(message) {
  if (message?.time) return message.time;
  const text = String(message?.text || "");
  const extracted = extractInlineChatTimestamp(text);
  if (extracted.time) return extracted.time;
  const sortTs = Number(message?.sortTs);
  if (!Number.isFinite(sortTs) || sortTs <= 0) return "";
  const date = new Date(sortTs * 1000);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleTimeString("no-NO", {
    hour: "2-digit",
    minute: "2-digit"
  });
}

function renderChatMessages(messages) {
  chatThread.innerHTML = "";
  if (!messages.length) {
    const empty = document.createElement("p");
    empty.className = "chat-empty";
    empty.textContent = "Ingen meldinger ennå. Send en melding for å starte eller vekke session.";
    chatThread.appendChild(empty);
    return;
  }

  messages.forEach((message) => {
    const node = document.createElement("article");
    node.className = "chat-message";
    node.dataset.role = message.role || "system";
    const meta = document.createElement("div");
    meta.className = "chat-meta";
    const role = document.createElement("span");
    role.className = "chat-role";
    role.textContent = message.role || "system";
    meta.appendChild(role);
    const displayTime = formatChatTimestamp(message);
    if (displayTime) {
      const time = document.createElement("span");
      time.className = "chat-time";
      time.textContent = displayTime;
      meta.appendChild(time);
    }
    const text = document.createElement("p");
    text.className = "chat-text";
    const extracted = extractInlineChatTimestamp(message.text || "");
    text.textContent = extracted.text || String(message.text || "");
    node.appendChild(meta);
    node.appendChild(text);
    chatThread.appendChild(node);
  });
  chatThread.scrollTop = chatThread.scrollHeight;
}

function optimisticChatPayload(currentMessages, messageText, sessionId) {
  return {
    sessionId: sessionId || "",
    messages: [
      ...(currentMessages || []),
      {
        role: "user",
        text: messageText,
        time: new Date().toLocaleTimeString("no-NO", {
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit"
        })
      }
    ]
  };
}

function chatSignature(payload) {
  return JSON.stringify({
    sessionId: payload.sessionId || "",
    messages: (payload.messages || []).map((message) => ({
      role: message.role || "",
      text: message.text || "",
      time: message.time || ""
    }))
  });
}

function hasAssistantReplyAfterLastUser(messages) {
  const rows = messages || [];
  let lastUserIndex = -1;
  rows.forEach((message, index) => {
    if ((message.role || "").toLowerCase() === "user") {
      lastUserIndex = index;
    }
  });
  if (lastUserIndex === -1) return true;
  for (let index = lastUserIndex + 1; index < rows.length; index += 1) {
    if ((rows[index].role || "").toLowerCase() === "assistant") {
      return true;
    }
  }
  return false;
}

async function loadChat(agentId, { keepScroll = false, fresh = false } = {}) {
  if (chatState.inFlight) return;
  chatState.inFlight = true;
  const requestId = ++chatState.requestId;
  try {
    const response = await fetch(`/api/agent-chat?agent=${encodeURIComponent(agentId)}${fresh ? "&fresh=1" : ""}`);
    const payload = await response.json();
    if (requestId !== chatState.requestId || agentId !== chatState.agentId) {
      return;
    }
    chatState.sessionId = payload.sessionId || "";
    chatModalTitle.textContent = payload.agent?.label || "Chat";
    chatModalSubtitle.textContent = payload.agent?.name
      ? `${payload.agent.name} · ${payload.agent.status || "Unknown"}`
      : `${payload.agent?.status || "Unknown"}`;
    if (!chatState.sending) {
      chatHint.textContent = chatState.sessionId
        ? "Session er klar. Send melding for å fortsette samtalen."
        : "Send en melding for å starte eller vekke agenten.";
    }
    const signature = chatSignature(payload);
    const previousScroll = chatThread.scrollTop;
    const previousHeight = chatThread.scrollHeight;
    const nearBottom = previousHeight - previousScroll - chatThread.clientHeight < 48;
    if (signature !== chatState.signature) {
      chatState.signature = signature;
      renderChatMessages(payload.messages || []);
      if (keepScroll && !nearBottom) {
        const nextHeight = chatThread.scrollHeight;
        chatThread.scrollTop = Math.max(0, previousScroll + (nextHeight - previousHeight));
      }
    }
    if (chatState.waitingForReply && hasAssistantReplyAfterLastUser(payload.messages || [])) {
      chatState.waitingForReply = false;
      if (chatState.replyWatchTimer) {
        window.clearInterval(chatState.replyWatchTimer);
        chatState.replyWatchTimer = null;
      }
      chatHint.textContent = "Svar mottatt.";
    }
  } finally {
    if (requestId === chatState.requestId) {
      chatState.inFlight = false;
    }
  }
}

async function openChat(agentId) {
  chatState.agentId = agentId;
  chatModalShell.hidden = false;
  document.body.style.overflow = "hidden";
  chatModalTitle.textContent = "Laster chat...";
  chatModalSubtitle.textContent = "Henter session fra OpenClaw...";
  renderChatMessages([]);
  await loadChat(agentId, { fresh: false });
  scheduleChatRefresh(150);
  if (chatState.timer) {
    window.clearInterval(chatState.timer);
  }
  chatState.timer = window.setInterval(() => {
    if (chatState.agentId) {
      loadChat(chatState.agentId, { keepScroll: true }).catch(() => {});
    }
  }, 8000);
  chatInput.focus();
}

function closeChat() {
  chatModalShell.hidden = true;
  document.body.style.overflow = "";
  chatState.agentId = "";
  chatState.sessionId = "";
  chatState.signature = "";
  chatState.requestId = 0;
  chatState.inFlight = false;
  if (chatState.timer) {
    window.clearInterval(chatState.timer);
    chatState.timer = null;
  }
  if (chatState.replyWatchTimer) {
    window.clearInterval(chatState.replyWatchTimer);
    chatState.replyWatchTimer = null;
  }
  if (chatState.refreshTimeout) {
    window.clearTimeout(chatState.refreshTimeout);
    chatState.refreshTimeout = null;
  }
  if (pendingSnapshot) {
    render(pendingSnapshot);
    pendingSnapshot = null;
  }
}

function scheduleChatRefresh(delayMs) {
  if (chatState.refreshTimeout) {
    window.clearTimeout(chatState.refreshTimeout);
  }
  chatState.refreshTimeout = window.setTimeout(() => {
    chatState.refreshTimeout = null;
    if (!chatState.agentId) return;
    loadChat(chatState.agentId, { keepScroll: true, fresh: true }).catch(() => {});
  }, delayMs);
}

function scheduleChatRefreshBurst() {
  [900, 2200, 4500].forEach((delayMs) => {
    window.setTimeout(() => {
      if (!chatState.agentId) return;
      loadChat(chatState.agentId, { keepScroll: true, fresh: true }).catch(() => {});
    }, delayMs);
  });
}

function startReplyWatch() {
  if (chatState.replyWatchTimer) {
    window.clearInterval(chatState.replyWatchTimer);
  }
  let checksLeft = 20;
  chatState.replyWatchTimer = window.setInterval(() => {
    if (!chatState.agentId || !chatState.waitingForReply || checksLeft <= 0) {
      window.clearInterval(chatState.replyWatchTimer);
      chatState.replyWatchTimer = null;
      return;
    }
    checksLeft -= 1;
    loadChat(chatState.agentId, { keepScroll: true, fresh: true }).catch(() => {});
  }, 2000);
}

function render(snapshot) {
  setText(titleEl, snapshot.config?.title, "OpenClaw Agent Control");
  setText(subtitleEl, snapshot.config?.subtitle, "Live view");
  setText(gatewayLabelEl, "OpenClaw Working Staff");
  setText(gatewayStatusEl, snapshot.gateway?.status, "Unknown");
  gatewayStatusEl.dataset.tone = statusTone(snapshot.gateway?.status);
  stageEl.dataset.live = snapshot.summary?.gatewayOnline ? "true" : "false";
  setText(
    gatewayDetailEl,
    `${snapshot.gateway?.detail || "Unknown"} · ${snapshot.gateway?.host || "127.0.0.1"}`
  );
  setText(updatedAtEl, `Oppdatert ${formatClock(snapshot.updatedAt)}`);

  renderSummary(snapshot.summary || {});
  renderAgents(snapshot.agents || []);
  renderNodes(snapshot.channels || [], snapshot.cronJobs || []);
  renderActivity(snapshot.activity || []);
  refreshScene();
}

async function bootstrap() {
  loadSceneLayout();
  loadViewMode();
  applySceneScale();
  applyViewMode();
  const response = await fetch("/api/snapshot");
  if (!response.ok) throw new Error("Klarte ikke hente snapshot");
  const snapshot = await response.json();
  render(snapshot);
  startSceneEngine();
}

bootstrap().catch((error) => {
  updatedAtEl.textContent = error.message;
});

if ("serviceWorker" in navigator) {
  window.addEventListener("load", () => {
    navigator.serviceWorker.register("/sw.js").catch(() => {});
  });
}

const stream = new EventSource("/events");
stream.onmessage = (event) => {
  const snapshot = JSON.parse(event.data);
  if (!chatModalShell.hidden) {
    pendingSnapshot = snapshot;
    return;
  }
  render(snapshot);
};

stream.onerror = () => {
  updatedAtEl.textContent = "Tilkobling til sanntidsstrøm ble brutt, prøver igjen...";
};

window.addEventListener("resize", () => {
  refreshScene();
});

stageSceneEl.addEventListener("wheel", (event) => {
  if (event.target.closest(".chat-modal, .activity-card")) return;
  if (sceneState.viewMode !== "scene") return;
  event.preventDefault();
  const nextScale = Math.max(0.45, Math.min(2.1, sceneState.scale + (event.deltaY < 0 ? 0.05 : -0.05)));
  if (nextScale === sceneState.scale) return;
  sceneState.scale = Number(nextScale.toFixed(2));
  applySceneScale();
  saveSceneLayout();
  refreshScene();
}, { passive: false });

stageSceneEl.addEventListener("pointerdown", (event) => {
  if (sceneState.viewMode !== "scene") return;
  startDrag(event.target, event.clientX, event.clientY);
});

window.addEventListener("pointermove", (event) => {
  moveDrag(event.clientX, event.clientY);
});

window.addEventListener("pointerup", () => {
  stopDrag();
});

window.addEventListener("pointercancel", () => {
  stopDrag();
});

agentColumn.addEventListener("click", (event) => {
  const button = event.target.closest(".agent-chat-button");
  if (!button) return;
  const agentId = button.dataset.agent;
  if (agentId) {
    openChat(agentId).catch(() => {
      chatModalTitle.textContent = "Chat utilgjengelig";
      chatModalSubtitle.textContent = "Kunne ikke hente session fra OpenClaw.";
      chatModalShell.hidden = false;
    });
  }
});

chatModalShell.addEventListener("click", (event) => {
  if (event.target.closest("[data-close-chat='true']")) {
    closeChat();
  }
});

chatCloseBtn.addEventListener("click", () => {
  closeChat();
});

chatForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!chatState.agentId || chatState.sending) return;
  const message = chatInput.value.trim();
  if (!message) return;
  chatState.sending = true;
  chatSendBtn.disabled = true;
  const optimisticPayload = optimisticChatPayload(
    [...chatThread.querySelectorAll(".chat-message")].map((node) => ({
      role: node.dataset.role || "system",
      text: node.querySelector(".chat-text")?.textContent || "",
      time: node.querySelector(".chat-time")?.textContent || ""
    })),
    message,
    chatState.sessionId
  );
  chatState.signature = chatSignature(optimisticPayload);
  renderChatMessages(optimisticPayload.messages || []);
  chatState.waitingForReply = true;
  chatHint.textContent = "Sender melding...";
  try {
    const response = await fetch("/api/agent-chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        agent: chatState.agentId,
        sessionId: chatState.sessionId,
        message
      })
    });
    const payload = await response.json();
    if (!response.ok || payload.send?.ok === false) {
      chatState.waitingForReply = false;
      chatHint.textContent = payload.send?.error || payload.error || "Kunne ikke sende melding til agenten.";
      scheduleChatRefresh(800);
      return;
    }
    chatState.sessionId = payload.sessionId || chatState.sessionId;
    chatInput.value = "";
    if ((payload.messages || []).length) {
      chatState.signature = chatSignature(payload);
      renderChatMessages(payload.messages || []);
    }
    chatHint.textContent = payload.send?.warning || "Melding sendt. Venter på svar...";
    scheduleChatRefreshBurst();
    startReplyWatch();
  } finally {
    chatState.sending = false;
    chatSendBtn.disabled = false;
  }
});

window.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !chatModalShell.hidden) {
    closeChat();
  }
});

if (viewToggleEl) {
  viewToggleEl.addEventListener("click", (event) => {
    const button = event.target.closest("[data-view-mode]");
    if (!button) return;
    const nextMode = button.dataset.viewMode === "hierarchy" ? "hierarchy" : "scene";
    if (nextMode === sceneState.viewMode) return;
    sceneState.viewMode = nextMode;
    applyViewMode();
    saveViewMode();
    refreshScene();
  });
}
