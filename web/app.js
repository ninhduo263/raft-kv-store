const connectionLabel = document.querySelector("#connection-label");
const clusterCount = document.querySelector("#cluster-count");
const quorumValue = document.querySelector("#quorum-value");
const leaderReadout = document.querySelector("#leader-readout");
const nodeList = document.querySelector("#node-list");
const topology = document.querySelector("#topology");
const faultButtons = document.querySelector("#fault-buttons");
const faultMessage = document.querySelector("#fault-message");
const replicaRows = document.querySelector("#replica-rows");
const replicaHeading = document.querySelector("#replica-heading");
const eventList = document.querySelector("#event-list");
const eventCount = document.querySelector("#event-count");
const lastUpdated = document.querySelector("#last-updated");
const kvResult = document.querySelector("#kv-result");
const knownEvents = [];
let previousSnapshot = "";
let busy = false;

function escapeHtml(value) {
  return String(value).replace(
    /[&<>"']/g,
    (character) =>
      ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      })[character],
  );
}

function logEvent(message) {
  knownEvents.unshift({
    time: new Date().toLocaleTimeString("en-GB", { hour12: false }),
    message,
  });
  knownEvents.splice(8);
  renderEvents();
}

function renderEvents() {
  eventCount.textContent = String(knownEvents.length).padStart(2, "0");
  eventList.innerHTML = knownEvents.length
    ? knownEvents
        .map(
          (event) =>
            `<li><time>${event.time}</time><span>${escapeHtml(event.message)}</span></li>`,
        )
        .join("")
    : '<li class="event-empty">No events recorded.</li>';
}

function inferIsolated(nodes) {
  const ids = Object.keys(nodes);
  return (
    ids.find(
      (id) =>
        nodes[id].available !== false &&
        (nodes[id].blocked || []).length === ids.length - 1 &&
        ids
          .filter((peer) => peer !== id)
          .every((peer) => (nodes[peer]?.blocked || []).includes(id)),
    ) || null
  );
}

function renderTopology(nodes) {
  const ids = Object.keys(nodes).sort();
  if (!ids.length) return;
  const centerX = 200;
  const centerY = 145;
  const radius = ids.length === 1 ? 0 : ids.length > 4 ? 112 : 93;
  const positions = Object.fromEntries(
    ids.map((id, index) => {
      const angle = -Math.PI / 2 + (Math.PI * 2 * index) / ids.length;
      return [
        id,
        {
          x: centerX + Math.cos(angle) * radius,
          y: centerY + Math.sin(angle) * radius,
        },
      ];
    }),
  );
  const edges = [];
  ids.forEach((id, index) =>
    ids.slice(index + 1).forEach((peer) => {
      const blocked =
        (nodes[id].blocked || []).includes(peer) ||
        (nodes[peer].blocked || []).includes(id);
      const offline =
        nodes[id].available === false || nodes[peer].available === false;
      edges.push(
        `<line class="topology-link${blocked ? " blocked" : ""}${offline ? " offline" : ""}" x1="${positions[id].x}" y1="${positions[id].y}" x2="${positions[peer].x}" y2="${positions[peer].y}" />`,
      );
    }),
  );
  const markers = ids.map((id) => {
    const node = nodes[id];
    const role = node.available === false ? "unavailable" : node.role;
    const label =
      node.available === false
        ? "OFFLINE"
        : String(node.role || "unknown").toUpperCase();
    const { x, y } = positions[id];
    return (
      `<g><circle class="topology-node ${escapeHtml(role)}" cx="${x}" cy="${y}" r="26" />` +
      `<text class="topology-node-label${role === "leader" ? " inverse" : ""}" x="${x}" y="${y + 4}">${escapeHtml(id)}</text>` +
      `<text class="topology-role-label" x="${x}" y="${y + 43}">${escapeHtml(label)}</text></g>`
    );
  });
  topology.innerHTML = `<title>Raft cluster peers and blocked links</title>${edges.join("")}${markers.join("")}`;
  const links = edges.length;
  const cut = ids.reduce(
    (count, id) =>
      count +
      (nodes[id].blocked || []).filter(
        (peer) => ids.indexOf(peer) > ids.indexOf(id),
      ).length,
    0,
  );
  document.querySelector("#link-count").textContent =
    `${links - cut} ACTIVE / ${links} TOTAL`;
}

function renderNodes(nodes) {
  const ids = Object.keys(nodes).sort();
  nodeList.innerHTML = ids
    .map((id) => {
      const node = nodes[id];
      const unavailable = node.available === false;
      const role = unavailable ? "offline" : node.role;
      const label = unavailable ? "offline" : node.role || "unknown";
      const blocked = (node.blocked || []).length
        ? `BLOCKED: ${(node.blocked || []).join(", ")}`
        : "";
      return (
        `<article class="node-row"><div class="node-row-head"><span class="node-id">${escapeHtml(id)}</span>` +
        `<span class="node-role ${escapeHtml(role)}">${escapeHtml(label)}</span></div>` +
        `<div class="node-metrics"><span>TERM <b>${unavailable ? "--" : node.term}</b></span>` +
        `<span>COMMIT <b>${unavailable ? "--" : node.commit}</b></span></div>` +
        `<div class="node-blocked">${escapeHtml(blocked)}</div></article>`
      );
    })
    .join("");
}

function renderFaultButtons(nodes) {
  const ids = Object.keys(nodes).sort();
  faultButtons.innerHTML = ids
    .map(
      (id) =>
        `<button class="button button-isolate" type="button" data-isolate="${escapeHtml(id)}">Isolate ${escapeHtml(id)}</button>`,
    )
    .join("");
  faultButtons.querySelectorAll("[data-isolate]").forEach((button) => {
    button.addEventListener("click", () =>
      setPartition(button.dataset.isolate),
    );
  });
}

function renderReplicas(nodes) {
  const ids = Object.keys(nodes).sort();
  replicaHeading.textContent = ids.length
    ? ids.join(" / ").toUpperCase()
    : "REPLICAS";
  const keys = [
    ...new Set(ids.flatMap((id) => Object.keys(nodes[id].kv || {}))),
  ].sort();
  if (!keys.length) {
    replicaRows.innerHTML =
      '<tr><td class="empty-state" colspan="2">No committed keys yet.</td></tr>';
    return;
  }
  replicaRows.innerHTML = keys
    .map(
      (key) =>
        `<tr><td>${escapeHtml(key)}</td><td>` +
        ids
          .map((id) => {
            const value = nodes[id].kv?.[key];
            const shown =
              nodes[id].available === false
                ? "offline"
                : value === undefined
                  ? "—"
                  : value;
            const marker =
              value === undefined
                ? ""
                : `<span class="replica-index">${escapeHtml(id)}</span>`;
            return `<span class="replica-cell"><span class="replica-value" title="${escapeHtml(shown)}">${escapeHtml(shown)}</span>${marker}</span>`;
          })
          .join("") +
        "</td></tr>",
    )
    .join("");
}

function updateEvents(nodes) {
  const snapshot = Object.fromEntries(
    Object.entries(nodes).map(([id, node]) => [
      id,
      {
        available: node.available,
        role: node.role,
        term: node.term,
      },
    ]),
  );
  if (previousSnapshot) {
    const previous = JSON.parse(previousSnapshot);
    const oldLeader = Object.keys(previous).find(
      (id) => previous[id].role === "leader",
    );
    const newLeader = Object.keys(snapshot).find(
      (id) => snapshot[id].role === "leader",
    );
    if (oldLeader !== newLeader) {
      logEvent(
        newLeader
          ? `${newLeader} elected leader in term ${snapshot[newLeader].term}.`
          : "No active leader; cluster is electing.",
      );
    } else {
      for (const id of Object.keys(snapshot)) {
        if (previous[id]?.available !== snapshot[id].available) {
          logEvent(
            snapshot[id].available === false
              ? `${id} is unreachable.`
              : `${id} is reachable again.`,
          );
        } else if (previous[id]?.term !== snapshot[id].term) {
          logEvent(`${id} advanced to term ${snapshot[id].term}.`);
        }
      }
    }
  }
  previousSnapshot = JSON.stringify(snapshot);
}

function renderCluster(payload) {
  const nodes = payload.nodes || {};
  const ids = Object.keys(nodes).sort();
  const available = ids.filter((id) => nodes[id].available !== false).length;
  const leaders = ids.filter(
    (id) => nodes[id].available !== false && nodes[id].role === "leader",
  );
  connectionLabel.textContent =
    available === ids.length ? "CONNECTED" : "DEGRADED";
  clusterCount.textContent = `${available} / ${ids.length} NODES`;
  quorumValue.innerHTML = `${payload.majority ?? "--"} <small>OF ${ids.length}</small>`;
  leaderReadout.textContent = leaders.length
    ? `LEADER · ${leaders[0].toUpperCase()}`
    : "NO ACTIVE LEADER";
  renderTopology(nodes);
  renderNodes(nodes);
  renderFaultButtons(nodes);
  renderReplicas(nodes);
  const isolated = inferIsolated(nodes);
  faultMessage.classList.remove("is-error");
  faultMessage.textContent = isolated
    ? `${isolated} isolated from the cluster.`
    : "Network links are healthy.";
  updateEvents(nodes);
  lastUpdated.textContent = `SAMPLED ${new Date().toLocaleTimeString("en-GB", { hour12: false })}`;
}

async function refreshCluster() {
  try {
    const response = await fetch("/cluster/status", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    renderCluster(await response.json());
  } catch (error) {
    connectionLabel.textContent = "DISCONNECTED";
    faultMessage.classList.add("is-error");
    faultMessage.textContent = `Cluster status unavailable: ${error.message}`;
  }
}

async function setPartition(isolate) {
  if (busy) return;
  busy = true;
  setControlsDisabled(true);
  faultMessage.classList.remove("is-error");
  faultMessage.textContent = isolate
    ? `Isolating ${isolate}…`
    : "Restoring peer links…";
  try {
    const response = await fetch("/admin/partition", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ isolate }),
    });
    const result = await response.json();
    if (!response.ok)
      throw new Error(
        result.errors?.map((item) => item.node).join(", ") ||
          result.error ||
          `HTTP ${response.status}`,
      );
    logEvent(
      isolate
        ? `${isolate} isolated; majority partition remains connected.`
        : "Peer links restored.",
    );
    await refreshCluster();
  } catch (error) {
    faultMessage.classList.add("is-error");
    faultMessage.textContent = `Partition update failed: ${error.message}`;
    logEvent(`Partition update failed: ${error.message}`);
  } finally {
    busy = false;
    setControlsDisabled(false);
  }
}

function setControlsDisabled(disabled) {
  document.querySelectorAll(".controls-section button").forEach((button) => {
    button.disabled = disabled;
  });
}

async function kvRequest(method) {
  const key = document.querySelector("#kv-key").value.trim();
  if (!key) {
    showKvResult("Enter a key first.", true);
    return;
  }
  const value = document.querySelector("#kv-value").value;
  showKvResult(`${method} request in progress…`);
  try {
    const response = await fetch(`/kv/${encodeURIComponent(key)}`, {
      method,
      ...(method === "PUT"
        ? {
            headers: { "Content-Type": "text/plain; charset=utf-8" },
            body: value,
          }
        : {}),
    });
    const result = await response.json();
    if (!response.ok)
      throw new Error(result.error || `HTTP ${response.status}`);
    const message =
      method === "GET"
        ? `${key} = ${result.value ?? "(not found)"}`
        : method === "DELETE"
          ? `Deleted ${key}.`
          : `Committed ${key} at index ${result.index}, term ${result.term}.`;
    showKvResult(message, false, true);
    logEvent(message);
    await refreshCluster();
  } catch (error) {
    showKvResult(error.message, true);
    logEvent(`${method} failed: ${error.message}`);
  }
}

function showKvResult(message, isError = false, isSuccess = false) {
  kvResult.textContent = message;
  kvResult.classList.toggle("is-error", isError);
  kvResult.classList.toggle("is-success", isSuccess);
}

document.querySelector("#put-form").addEventListener("submit", (event) => {
  event.preventDefault();
  kvRequest("PUT");
});
document
  .querySelector("#read-button")
  .addEventListener("click", () => kvRequest("GET"));
document
  .querySelector("#delete-button")
  .addEventListener("click", () => kvRequest("DELETE"));
document
  .querySelector("#heal-button")
  .addEventListener("click", () => setPartition(null));
document.querySelector("#clock").textContent = new Date().toLocaleTimeString(
  "en-GB",
  { hour12: false },
);
setInterval(() => {
  document.querySelector("#clock").textContent = new Date().toLocaleTimeString(
    "en-GB",
    { hour12: false },
  );
}, 1000);
refreshCluster();
setInterval(refreshCluster, 1400);
