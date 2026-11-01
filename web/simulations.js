const protocolTabs = document.querySelectorAll("[data-protocol-tab]");

protocolTabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    document.body.dataset.protocol = tab.dataset.protocolTab;
    document.querySelector("#mode-description").textContent = {
      raft: "Live Raft cluster · observe leader election and majority commits",
      paxos: "Paxos sandbox · inspect ballots, promises and accepted values",
      pbft: "PBFT sandbox · inject Byzantine faults and advance protocol views",
    }[tab.dataset.protocolTab];
    protocolTabs.forEach((item) => {
      const active = item === tab;
      item.classList.toggle("is-active", active);
      item.setAttribute("aria-selected", String(active));
    });
  });
});

function escapeProtocolText(value) {
  return String(value).replace(/[&<>"']/g, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[character]);
}

function compareBallots(left, right) {
  if (!right) return 1;
  return left[0] - right[0] || left[1] - right[1];
}

function ballotLabel(ballot) {
  return ballot ? `(${ballot[0]},${ballot[1]})` : "—";
}

let paxosState;

function resetPaxos() {
  paxosState = {
    majority: 2,
    queue: [],
    events: [],
    nextMessage: 1,
    acceptors: ["A1", "A2", "A3"].map((id) => ({
      id,
      promised: null,
      acceptedBallot: null,
      acceptedValue: null,
    })),
    proposers: [
      { id: "P1", ballot: [1, 1], input: "#paxos-value-p1" },
      { id: "P2", ballot: [1, 2], input: "#paxos-value-p2" },
    ].map((proposer) => ({
      ...proposer,
      value: document.querySelector(proposer.input).value.trim() || proposer.id,
      phase: "idle",
      promises: [],
      accepts: [],
    })),
    learned: null,
  };
  document.querySelector("#paxos-status").textContent = "Ready for proposals.";
  renderPaxos();
}

function paxosEvent(message) {
  paxosState.events.unshift({
    time: new Date().toLocaleTimeString("en-GB", { hour12: false }),
    message,
  });
  paxosState.events.splice(12);
}

function enqueuePaxos(type, from, to, ballot, details = {}) {
  paxosState.queue.push({
    id: paxosState.nextMessage++,
    type,
    from,
    to,
    ballot: [...ballot],
    ...details,
  });
  paxosEvent(`Queued ${type}: ${from} → ${to} at ${ballotLabel(ballot)}.`);
}

function startPaxos(proposerId) {
  const proposer = paxosState.proposers.find((item) => item.id === proposerId);
  if (!proposer || proposer.phase !== "idle") return;
  proposer.value = document.querySelector(proposer.input).value.trim() || proposer.id;
  proposer.phase = "preparing";
  proposer.promises = [];
  proposer.accepts = [];
  paxosState.acceptors.forEach((acceptor) => {
    enqueuePaxos("PREPARE", proposer.id, acceptor.id, proposer.ballot);
  });
  document.querySelector("#paxos-status").textContent = `${proposer.id} is asking for promises.`;
  renderPaxos();
}

function findPaxosNode(id) {
  return paxosState.acceptors.find((item) => item.id === id)
    || paxosState.proposers.find((item) => item.id === id);
}

function receivePaxos(message) {
  const proposer = paxosState.proposers.find((item) => item.id === message.to);
  const acceptor = paxosState.acceptors.find((item) => item.id === message.to);

  if (message.type === "PREPARE" && acceptor) {
    if (compareBallots(message.ballot, acceptor.promised) >= 0) {
      acceptor.promised = [...message.ballot];
      enqueuePaxos("PROMISE", acceptor.id, message.from, message.ballot, {
        acceptedBallot: acceptor.acceptedBallot && [...acceptor.acceptedBallot],
        acceptedValue: acceptor.acceptedValue,
      });
      paxosEvent(`${acceptor.id} promised ${ballotLabel(message.ballot)}.`);
    } else {
      enqueuePaxos("NACK", acceptor.id, message.from, message.ballot, {
        promisedBallot: acceptor.promised && [...acceptor.promised],
      });
      paxosEvent(`${acceptor.id} rejected stale ballot ${ballotLabel(message.ballot)}.`);
    }
    return;
  }

  if (message.type === "PROMISE" && proposer) {
    if (proposer.phase !== "preparing" || compareBallots(message.ballot, proposer.ballot) !== 0) return;
    if (!proposer.promises.some((promise) => promise.acceptor === message.from)) {
      proposer.promises.push({
        acceptor: message.from,
        acceptedBallot: message.acceptedBallot,
        acceptedValue: message.acceptedValue,
      });
    }
    if (proposer.promises.length >= paxosState.majority) {
      const previous = proposer.promises
        .filter((promise) => promise.acceptedBallot)
        .sort((left, right) => compareBallots(right.acceptedBallot, left.acceptedBallot))[0];
      if (previous) proposer.value = previous.acceptedValue;
      proposer.phase = "accepting";
      paxosEvent(`${proposer.id} received a majority of promises${previous ? ` and adopted ${previous.acceptedValue}` : ""}.`);
      paxosState.acceptors.forEach((item) => {
        enqueuePaxos("ACCEPT", proposer.id, item.id, proposer.ballot, { value: proposer.value });
      });
    }
    return;
  }

  if (message.type === "ACCEPT" && acceptor) {
    if (compareBallots(message.ballot, acceptor.promised) >= 0) {
      acceptor.promised = [...message.ballot];
      acceptor.acceptedBallot = [...message.ballot];
      acceptor.acceptedValue = message.value;
      enqueuePaxos("ACCEPTED", acceptor.id, message.from, message.ballot, { value: message.value });
      paxosEvent(`${acceptor.id} accepted ${message.value} at ${ballotLabel(message.ballot)}.`);
    } else {
      enqueuePaxos("NACK", acceptor.id, message.from, message.ballot, {
        promisedBallot: acceptor.promised && [...acceptor.promised],
      });
      paxosEvent(`${acceptor.id} refused ACCEPT; it promised ${ballotLabel(acceptor.promised)}.`);
    }
    return;
  }

  if (message.type === "ACCEPTED" && proposer) {
    if (proposer.phase !== "accepting" || compareBallots(message.ballot, proposer.ballot) !== 0) return;
    if (!proposer.accepts.includes(message.from)) proposer.accepts.push(message.from);
    if (proposer.accepts.length >= paxosState.majority) {
      proposer.phase = "learned";
      paxosState.learned = { proposer: proposer.id, ballot: [...proposer.ballot], value: message.value };
      paxosEvent(`${proposer.id} learned ${message.value} from ${proposer.accepts.length} acceptors.`);
      document.querySelector("#paxos-status").textContent = "A value has been learned by a majority.";
    }
    return;
  }

  if (message.type === "NACK" && proposer && proposer.phase !== "learned") {
    if (message.promisedBallot && compareBallots(message.promisedBallot, proposer.ballot) > 0) {
      proposer.phase = "preempted";
      paxosEvent(`${proposer.id} was preempted by ${ballotLabel(message.promisedBallot)}.`);
    }
  }
}

function deliverPaxos(drop = false) {
  const message = paxosState.queue.shift();
  if (!message) return;
  if (drop) {
    paxosEvent(`Dropped ${message.type}: ${message.from} → ${message.to}.`);
    document.querySelector("#paxos-status").textContent = `${message.type} was lost; no response will arrive.`;
  } else {
    paxosEvent(`Delivered ${message.type}: ${message.from} → ${message.to}.`);
    receivePaxos(message);
    document.querySelector("#paxos-status").textContent = `Delivered ${message.type} from ${message.from} to ${message.to}.`;
  }
  renderPaxos();
}

function renderPaxos() {
  document.querySelector("#paxos-majority").textContent = `${paxosState.majority} OF ${paxosState.acceptors.length}`;
  document.querySelector("#paxos-acceptors").innerHTML = paxosState.acceptors.map((acceptor) =>
    `<article class="sim-replica"><div><strong>${acceptor.id}</strong><span>ACCEPTOR</span></div>` +
    `<small>PROMISED ${ballotLabel(acceptor.promised)}</small>` +
    `<small>ACCEPTED ${acceptor.acceptedValue === null ? "—" : `${escapeProtocolText(acceptor.acceptedValue)} @ ${ballotLabel(acceptor.acceptedBallot)}`}</small></article>`,
  ).join("");
  document.querySelector("#paxos-proposers").innerHTML = paxosState.proposers.map((proposer) =>
    `<article class="sim-replica"><div><strong>${proposer.id} · ${ballotLabel(proposer.ballot)}</strong><span class="sim-phase ${proposer.phase}">${proposer.phase.toUpperCase()}</span></div>` +
    `<small>VALUE ${escapeProtocolText(proposer.value)}</small>` +
    `<small>PROMISES ${proposer.promises.length}/${paxosState.majority} · ACCEPTED ${proposer.accepts.length}/${paxosState.majority}</small></article>`,
  ).join("");
  document.querySelector("#paxos-queue").innerHTML = paxosState.queue.length
    ? paxosState.queue.slice(0, 10).map((message, index) =>
      `<li class="message-item${index === 0 ? " is-next" : ""}"><span class="message-type">${message.type}</span><span>${message.from} → ${message.to}</span><code>${ballotLabel(message.ballot)}${message.value ? ` · ${escapeProtocolText(message.value)}` : ""}</code></li>`,
    ).join("")
    : '<li class="queue-empty">Message queue is empty.</li>';
  const outcome = document.querySelector("#paxos-outcome");
  outcome.classList.toggle("is-success", Boolean(paxosState.learned));
  outcome.textContent = paxosState.learned
    ? `LEARNED · ${paxosState.learned.value} · ballot ${ballotLabel(paxosState.learned.ballot)} · proposer ${paxosState.learned.proposer}`
    : "No value learned yet.";
  document.querySelector("#paxos-event-count").textContent = String(paxosState.events.length).padStart(2, "0");
  document.querySelector("#paxos-events").innerHTML = paxosState.events.length
    ? paxosState.events.map((event) => `<li><time>${event.time}</time><span>${escapeProtocolText(event.message)}</span></li>`).join("")
    : '<li class="event-empty">No protocol messages yet.</li>';
  document.querySelector("#paxos-next").disabled = paxosState.queue.length === 0;
  document.querySelector("#paxos-drop").disabled = paxosState.queue.length === 0;
}

document.querySelector("#paxos-start-p1").addEventListener("click", () => startPaxos("P1"));
document.querySelector("#paxos-start-p2").addEventListener("click", () => startPaxos("P2"));
document.querySelector("#paxos-next").addEventListener("click", () => deliverPaxos());
document.querySelector("#paxos-drop").addEventListener("click", () => deliverPaxos(true));
document.querySelector("#paxos-reset").addEventListener("click", resetPaxos);

function primaryFor(view, replicaCount) {
  return `R${view % replicaCount}`;
}

let pbftState;

function pbftLimit() {
  return Math.floor((pbftState.count - 1) / 3);
}

function pbftQuorum() {
  return 2 * pbftLimit() + 1;
}

function pbftFaultyReplica() {
  if (pbftState.faultMode === "primary") return "R0";
  if (pbftState.faultMode === "backup") return pbftState.liarId;
  return null;
}

function resetPbft() {
  const count = Number(document.querySelector("#pbft-count").value);
  const faultMode = document.querySelector("#pbft-fault").value;
  const liarSelect = document.querySelector("#pbft-liar");
  const backupIds = Array.from({ length: count - 1 }, (_item, index) => `R${index + 1}`);
  const selectedLiar = backupIds.includes(liarSelect.value) ? liarSelect.value : backupIds.at(-1);
  liarSelect.innerHTML = backupIds.map((id) => `<option value="${id}">${id}</option>`).join("");
  liarSelect.value = selectedLiar;
  pbftState = {
    count,
    faultMode,
    liarId: selectedLiar,
    request: document.querySelector("#pbft-request").value.trim() || "request",
    view: 0,
    queue: [],
    events: [],
    nextMessage: 1,
    phase: "idle",
    sequence: 1,
    viewChanges: [],
    newViewQueued: false,
    newViewStarted: false,
    replicas: Array.from({ length: count }, (_item, index) => ({
      id: `R${index}`,
      preprepared: null,
      prepareVotes: {},
      prepared: [],
      commitVotes: {},
      committed: [],
      sentCommit: [],
    })),
  };
  document.querySelector(".pbft-liar-wrap").hidden = faultMode !== "backup";
  document.querySelector("#pbft-status").textContent = "Ready for a request.";
  renderPbft();
}

function pbftEvent(message) {
  pbftState.events.unshift({
    time: new Date().toLocaleTimeString("en-GB", { hour12: false }),
    message,
  });
  pbftState.events.splice(16);
}

function enqueuePbft(type, from, to, details = {}) {
  pbftState.queue.push({ id: pbftState.nextMessage++, type, from, to, view: pbftState.view, ...details });
  pbftEvent(`Queued ${type}: ${from} → ${to}${details.value ? ` · ${details.value}` : ""}.`);
}

function startPbftRequest(primary = primaryFor(pbftState.view, pbftState.count), value = pbftState.request) {
  pbftState.phase = "pre-prepare";
  pbftState.request = document.querySelector("#pbft-request").value.trim() || pbftState.request;
  const isLyingPrimary = primary === pbftFaultyReplica();
  const proposedValue = isLyingPrimary ? `${pbftState.request} [tampered]` : value;
  pbftState.replicas.forEach((replica) => {
    enqueuePbft("PRE-PREPARE", primary, replica.id, {
      value: proposedValue,
      sequence: pbftState.sequence,
    });
  });
  pbftEvent(`${primary} proposed ${proposedValue}${isLyingPrimary ? " instead of the client-signed request" : ""}.`);
  renderPbft();
}

function enqueuePrepare(replicaId, value) {
  pbftState.replicas.forEach((replica) => {
    enqueuePbft("PREPARE", replicaId, replica.id, { value, sequence: pbftState.sequence });
  });
}

function enqueueCommit(replicaId, value) {
  pbftState.replicas.forEach((replica) => {
    enqueuePbft("COMMIT", replicaId, replica.id, { value, sequence: pbftState.sequence });
  });
}

function maybePreparePbft(replica, value) {
  const votes = replica.prepareVotes[value] || [];
  const faulty = replica.id === pbftFaultyReplica();
  if (votes.length >= pbftQuorum() && (faulty || replica.preprepared === value)
    && !replica.prepared.includes(value)) {
    replica.prepared.push(value);
    enqueueCommit(replica.id, value);
    pbftEvent(`${replica.id} prepared ${value} with ${votes.length} matching votes, including the primary PRE-PREPARE.`);
  }
}

function receivePbft(message) {
  const replica = pbftState.replicas.find((item) => item.id === message.to);
  if (!replica) return;
  const faulty = pbftFaultyReplica();
  const isFaulty = replica.id === faulty;

  if (message.type === "PRE-PREPARE") {
    if (message.view !== pbftState.view) return;
    if (message.value !== pbftState.request && !isFaulty) {
      enqueuePbft("REJECT", replica.id, message.from, { value: message.value, reason: "request digest mismatch" });
      pbftEvent(`${replica.id} rejected a PRE-PREPARE with an invalid client-request digest.`);
      return;
    }
    replica.preprepared = message.value;
    const primary = primaryFor(message.view, pbftState.count);
    replica.prepareVotes[message.value] = [primary];
    if (replica.id !== primary) {
      const prepareValue = replica.id === faulty && replica.id !== primary
        ? `${pbftState.request} [forged digest]`
        : message.value;
      enqueuePrepare(replica.id, prepareValue);
      pbftEvent(`${replica.id} accepted PRE-PREPARE and broadcast PREPARE.`);
    } else {
      pbftEvent(`${replica.id} established the primary PRE-PREPARE.`);
    }
    maybePreparePbft(replica, message.value);
    return;
  }

  if (message.type === "PREPARE") {
    if (message.view !== pbftState.view) return;
    if (!isFaulty && replica.preprepared !== message.value) return;
    const votes = replica.prepareVotes[message.value] || (replica.prepareVotes[message.value] = []);
    if (!votes.includes(message.from)) votes.push(message.from);
    maybePreparePbft(replica, message.value);
    return;
  }

  if (message.type === "COMMIT") {
    if (message.view !== pbftState.view) return;
    if (!isFaulty && !replica.prepared.includes(message.value)) return;
    const votes = replica.commitVotes[message.value] || (replica.commitVotes[message.value] = []);
    if (!votes.includes(message.from)) votes.push(message.from);
    if (votes.length >= pbftQuorum() && (isFaulty || replica.prepared.includes(message.value))
      && !replica.committed.includes(message.value)) {
      replica.committed.push(message.value);
      pbftEvent(`${replica.id} committed ${message.value} with ${votes.length} matching COMMIT votes.`);
    }
    return;
  }

  if (message.type === "VIEW-CHANGE") {
    if (!pbftState.viewChanges.includes(message.from)) pbftState.viewChanges.push(message.from);
    if (pbftState.viewChanges.length >= pbftQuorum() && !pbftState.newViewQueued) {
      pbftState.newViewQueued = true;
      const newPrimary = primaryFor(pbftState.view, pbftState.count);
      pbftEvent(`${pbftState.viewChanges.length} VIEW-CHANGE messages reached ${newPrimary}; quorum formed.`);
      pbftState.replicas.forEach((item) => enqueuePbft("NEW-VIEW", newPrimary, item.id, { certificate: [...pbftState.viewChanges] }));
    }
    return;
  }

  if (message.type === "NEW-VIEW" && !pbftState.newViewStarted) {
    pbftState.newViewStarted = true;
    pbftEvent(`${message.from} installed view ${pbftState.view}; replaying the original client request.`);
    startPbftRequest(message.from, pbftState.request);
    return;
  }

  if (message.type === "REJECT") {
    pbftEvent(`${message.from} rejected the request from ${message.to}: ${message.reason}.`);
  }
}

function deliverPbft(drop = false) {
  const message = pbftState.queue.shift();
  if (!message) return;
  if (drop) {
    pbftEvent(`Dropped ${message.type}: ${message.from} → ${message.to}.`);
    document.querySelector("#pbft-status").textContent = `${message.type} was lost; no response will arrive.`;
  } else {
    pbftEvent(`Delivered ${message.type}: ${message.from} → ${message.to}.`);
    receivePbft(message);
    document.querySelector("#pbft-status").textContent = `Delivered ${message.type} from ${message.from} to ${message.to}.`;
  }
  renderPbft();
}

function triggerPbftViewChange() {
  pbftState.queue = [];
  pbftState.view += 1;
  pbftState.phase = "view-change";
  pbftState.viewChanges = [];
  pbftState.newViewQueued = false;
  pbftState.newViewStarted = false;
  const nextPrimary = primaryFor(pbftState.view, pbftState.count);
  pbftEvent(`View timeout: replicas request view ${pbftState.view}; ${nextPrimary} becomes the next primary.`);
  pbftState.replicas
    .filter((replica) => replica.id !== pbftFaultyReplica())
    .forEach((replica) => enqueuePbft("VIEW-CHANGE", replica.id, nextPrimary, { newView: pbftState.view }));
  document.querySelector("#pbft-status").textContent = `Waiting for ${pbftQuorum()} VIEW-CHANGE messages.`;
  renderPbft();
}

function renderPbft() {
  const primary = primaryFor(pbftState.view, pbftState.count);
  const faulty = pbftFaultyReplica();
  const f = pbftLimit();
  const quorum = pbftQuorum();
  const faultExceeded = faulty && f < 1;
  document.querySelector("#pbft-view-readout").textContent = `${pbftState.view} / ${primary}`;
  document.querySelector("#pbft-bound").textContent = `f = ${f} / ${quorum} votes`;
  const warning = document.querySelector("#pbft-warning");
  warning.classList.toggle("is-danger", Boolean(faultExceeded));
  warning.textContent = faultExceeded
    ? `n = ${pbftState.count} tolerates f = 0. One injected Byzantine replica exceeds the PBFT bound; this run has no safety guarantee.`
    : `n = ${pbftState.count} supports f = ${f}; this run requires ${quorum} matching votes.`;
  document.querySelector("#pbft-replicas").innerHTML = pbftState.replicas.map((replica) => {
    const role = replica.id === primary ? "PRIMARY" : "BACKUP";
    const badges = [role, replica.id === faulty ? "BYZANTINE" : "HONEST"];
    const status = replica.committed.length
      ? `COMMITTED ${replica.committed.join(" / ")}`
      : replica.prepared.length
        ? `PREPARED ${replica.prepared.join(" / ")}`
        : "WAITING";
    return `<article class="sim-replica"><div><strong>${replica.id}</strong><span class="replica-badges">${badges.map((badge) => `<i class="${badge.toLowerCase()}">${badge}</i>`).join("")}</span></div><small>${escapeProtocolText(status)}</small><small>VIEW ${pbftState.view} · PREPARES ${Object.values(replica.prepareVotes).reduce((sum, votes) => sum + votes.length, 0)}</small></article>`;
  }).join("");
  document.querySelector("#pbft-queue").innerHTML = pbftState.queue.length
    ? pbftState.queue.slice(0, 12).map((message, index) =>
      `<li class="message-item${index === 0 ? " is-next" : ""}"><span class="message-type ${message.type.toLowerCase()}">${message.type}</span><span>${message.from} → ${message.to}</span><code>v${message.view}${message.value ? ` · ${escapeProtocolText(message.value)}` : ""}</code></li>`,
    ).join("")
    : '<li class="queue-empty">Message queue is empty.</li>';
  const honestReplicas = pbftState.replicas.filter((replica) => replica.id !== faulty);
  const honestDecisions = [...new Set(honestReplicas.flatMap((replica) => replica.committed))];
  const outcome = document.querySelector("#pbft-outcome");
  outcome.classList.toggle("is-success", honestDecisions.length === 1);
  outcome.classList.toggle("is-danger", honestDecisions.length > 1 || Boolean(faultExceeded));
  outcome.textContent = honestDecisions.length > 1
    ? `SAFETY ALERT · honest replicas committed conflicting values: ${honestDecisions.join(" / ")}`
    : honestDecisions.length === 1
      ? `COMMITTED BY HONEST REPLICAS · ${honestDecisions[0]} · ${honestReplicas.filter((replica) => replica.committed.includes(honestDecisions[0])).length} replica(s)`
      : faultExceeded
        ? "OUTSIDE FAULT BOUND · no safe commit claim can be made."
        : "No request committed.";
  document.querySelector("#pbft-event-count").textContent = String(pbftState.events.length).padStart(2, "0");
  document.querySelector("#pbft-events").innerHTML = pbftState.events.length
    ? pbftState.events.map((event) => `<li><time>${event.time}</time><span>${escapeProtocolText(event.message)}</span></li>`).join("")
    : '<li class="event-empty">No protocol messages yet.</li>';
  document.querySelector("#pbft-next").disabled = pbftState.queue.length === 0;
  document.querySelector("#pbft-drop").disabled = pbftState.queue.length === 0;
}

document.querySelector("#pbft-count").addEventListener("change", resetPbft);
document.querySelector("#pbft-fault").addEventListener("change", resetPbft);
document.querySelector("#pbft-liar").addEventListener("change", resetPbft);
document.querySelector("#pbft-start").addEventListener("click", () => startPbftRequest());
document.querySelector("#pbft-next").addEventListener("click", () => deliverPbft());
document.querySelector("#pbft-drop").addEventListener("click", () => deliverPbft(true));
document.querySelector("#pbft-view").addEventListener("click", triggerPbftViewChange);
document.querySelector("#pbft-reset").addEventListener("click", resetPbft);

resetPaxos();
resetPbft();