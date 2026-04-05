const API_BASE =
  window.location.origin && window.location.origin !== "null"
    ? `${window.location.origin}/api`
    : "http://127.0.0.1:5000/api";

async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `HTTP ${res.status}`);
  }
  return await res.json();
}

function asQuery(params = {}) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === null || value === undefined || value === "") {
      return;
    }
    query.set(key, String(value));
  });
  return query.toString();
}

export async function getOptions() {
  return await fetchJson(`${API_BASE}/options`);
}

export async function getGraphSummary(filters) {
  return await fetchJson(`${API_BASE}/graph?${asQuery({ ...filters, view: "summary", limit: 70 })}`);
}

export async function getGraphDetail(filters) {
  return await fetchJson(`${API_BASE}/graph?${asQuery({ ...filters, view: "detail", limit: 260 })}`);
}

export async function getPrediction(filters) {
  return await fetchJson(`${API_BASE}/predict`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ granularity: "month", horizon: 6, filters }),
  });
}

export async function getWorkflow(filters) {
  return await fetchJson(`${API_BASE}/workflow?${asQuery(filters)}`);
}

export async function getFactorInsights(filters, topK, minAbsImpact) {
  return await fetchJson(
    `${API_BASE}/insights/factors?${asQuery({
      ...filters,
      top_k: topK,
      min_abs_impact: minAbsImpact,
    })}`
  );
}

export async function getFactorEvidence(filters, factor, page = 1, size = 20) {
  return await fetchJson(
    `${API_BASE}/insights/evidence?${asQuery({
      ...filters,
      factor,
      page,
      size,
    })}`
  );
}
