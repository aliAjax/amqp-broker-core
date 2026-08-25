const byId = (id) => document.getElementById(id);

async function request(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { "content-type": "application/json", ...(options.headers || {}) },
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`${response.status} ${detail || response.statusText}`);
  }
  return response.json();
}

function emptyRow(columns, message) {
  return `<tr><td colspan="${columns}" class="empty">${message}</td></tr>`;
}

async function loadAddresses() {
  const data = await request("/v1/addresses");
  const rows = await Promise.all((data.items || []).map(async (item) => {
    let depth = "-";
    try { depth = (await request(`/v1/addresses/${encodeURIComponent(item.name)}/depth`)).depth; } catch (_) {}
    return `<tr><td>${item.name}</td><td>${item.kind}</td><td>${Boolean(item.durable)}</td><td>${Boolean(item.paused)}</td><td>${depth}</td></tr>`;
  }));
  byId("addresses").innerHTML = rows.join("") || emptyRow(5, "No addresses configured");
  byId("address-count").textContent = String(data.count || 0);
}

async function refresh() {
  byId("error").hidden = true;
  try {
    const [status, connections, leases] = await Promise.all([
      request("/v1/status"), request("/v1/connections"), request("/v1/cluster/leases"), loadAddresses(),
    ]);
    byId("node-id").textContent = status.node_id || "-";
    byId("storage").textContent = status.storage || "-";
    byId("connection-count").textContent = String(connections.count || 0);
    byId("lease-count").textContent = String(leases.count || 0);
    byId("connections").innerHTML = (connections.items || []).map((item) => `<tr><td>${item.id}</td><td>${item.remote}</td><td>${item.state}</td><td>${item.sessions}</td></tr>`).join("") || emptyRow(4, "No active connections");
    byId("leases").innerHTML = (leases.items || []).map((item) => `<tr><td>${item.shard}</td><td>${item.node_id}</td><td>${item.epoch}</td><td>${new Date(item.expires_at).toLocaleTimeString()}</td></tr>`).join("") || emptyRow(4, "No shard leases");
    byId("connection-state").textContent = "Ready";
  } catch (error) {
    byId("connection-state").textContent = "Unavailable";
    byId("error").textContent = error.message;
    byId("error").hidden = false;
  }
}

byId("refresh").addEventListener("click", refresh);
byId("create-address").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  try {
    await request("/v1/addresses", { method: "POST", body: JSON.stringify({ name: form.get("name"), kind: form.get("kind"), durable: true }) });
    event.currentTarget.reset();
    await refresh();
  } catch (error) {
    byId("error").textContent = error.message;
    byId("error").hidden = false;
  }
});

refresh();
setInterval(refresh, 15000);
