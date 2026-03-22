const API_BASE =
  window.location.origin && window.location.origin !== "null"
    ? `${window.location.origin}/api`
    : "http://127.0.0.1:5000/api";

const trendChart = echarts.init(document.getElementById("trendChart"));
const graphEl = document.getElementById("graphChart");
const workflowEl = document.getElementById("workflowChart");
const explainSummaryEl = document.getElementById("explainSummary");
const driverCardsEl = document.getElementById("driverCards");
const workflowMetricsEl = document.getElementById("workflowMetrics");
let cy = null;

function buildQuery(extra = {}) {
  const start = document.getElementById("start").value;
  const end = document.getElementById("end").value;
  const shopId = document.getElementById("shopId").value.trim();
  const dish = document.getElementById("dish").value.trim();
  const params = new URLSearchParams();
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  if (shopId) params.set("shop_id", shopId);
  if (dish) params.set("dish", dish);
  Object.entries(extra).forEach(([k, v]) => params.set(k, String(v)));
  return params.toString();
}

async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `HTTP ${res.status}`);
  }
  return await res.json();
}

function renderExplanation(explanation) {
  explainSummaryEl.textContent = explanation?.summary || "暂无解释数据。";

  const positive = explanation?.top_positive_dishes || [];
  const negative = explanation?.top_negative_dishes || [];
  const attrs = explanation?.top_attributes || [];

  driverCardsEl.innerHTML = "";
  const cards = [
    {
      title: "正向驱动菜品",
      items: positive.map((x) => `${x.dish}: Δ${x.delta}, 影响${x.impact}`),
    },
    {
      title: "负向驱动菜品",
      items: negative.map((x) => `${x.dish}: Δ${x.delta}, 影响${x.impact}`),
    },
    {
      title: "高频属性关注",
      items: attrs.map((x) => `${x.attribute}: ${x.count}次`),
    },
  ];

  cards.forEach((card) => {
    const div = document.createElement("div");
    div.className = "driver-card";
    div.innerHTML = `<h3>${card.title}</h3>`;
    if (!card.items.length) {
      const item = document.createElement("div");
      item.className = "driver-item";
      item.textContent = "无";
      div.appendChild(item);
    } else {
      card.items.forEach((text) => {
        const item = document.createElement("div");
        item.className = "driver-item";
        item.textContent = text;
        div.appendChild(item);
      });
    }
    driverCardsEl.appendChild(div);
  });
}

async function loadTrend(baseQuery) {
  const filters = Object.fromEntries(new URLSearchParams(baseQuery).entries());
  const data = await fetchJson(`${API_BASE}/predict`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ granularity: "month", horizon: 6, filters }),
  });

  const historyX = data.history.map((x) => x.time);
  const historyY = data.history.map((x) => Number(x.rating).toFixed(2));
  const forecastX = data.forecast.map((x) => x.time);
  const forecastY = data.forecast.map((x) => Number(x.rating).toFixed(2));

  trendChart.setOption({
    tooltip: { trigger: "axis" },
    legend: { data: ["历史评分", "预测评分"] },
    xAxis: { type: "category", data: [...historyX, ...forecastX] },
    yAxis: { type: "value", min: 1, max: 5 },
    series: [
      { name: "历史评分", type: "line", smooth: true, data: historyY },
      {
        name: "预测评分",
        type: "line",
        smooth: true,
        lineStyle: { type: "dashed" },
        data: [...Array(historyY.length).fill(null), ...forecastY],
      },
    ],
  });
  renderExplanation(data.explanation || {});
}

function nodeColorByType(type) {
  if (type === "Shop") return "#136f63";
  if (type === "Dish") return "#1f8a70";
  if (type === "Attribute") return "#6ea78f";
  return "#7f9fb5";
}

async function loadGraph(baseQuery) {
  const query = buildQuery({
    ...Object.fromEntries(new URLSearchParams(baseQuery)),
    view: "summary",
    limit: 24,
  });
  const data = await fetchJson(`${API_BASE}/graph?${query}`);
  const elements = [
    ...data.nodes.map((n) => ({
      data: { id: n.id, label: `${n.type}:${n.label}`, type: n.type, score: n.score || 0 },
    })),
    ...data.edges.map((e, idx) => ({
      data: {
        id: `e${idx}`,
        source: e.source,
        target: e.target,
        label: e.weight ? `${e.type} (${e.weight})` : e.type,
        weight: e.weight || 1,
      },
    })),
  ];

  if (cy) cy.destroy();
  cy = cytoscape({
    container: graphEl,
    elements,
    style: [
      {
        selector: "node",
        style: {
          label: "data(label)",
          "background-color": (ele) => nodeColorByType(ele.data("type")),
          color: "#0a1620",
          "font-size": 10,
          "text-valign": "center",
          "text-halign": "center",
          width: 36,
          height: 36,
        },
      },
      {
        selector: "edge",
        style: {
          label: "data(label)",
          width: (ele) => Math.min(6, 1 + Math.log2(Number(ele.data("weight") || 1))),
          "line-color": "#9bb4c6",
          "target-arrow-shape": "triangle",
          "target-arrow-color": "#9bb4c6",
          "curve-style": "bezier",
          "font-size": 8,
          color: "#4c6576",
        },
      },
    ],
    layout: { name: "cose", animate: true, fit: true, padding: 30, nodeRepulsion: 8000 },
  });
}

function renderWorkflowSteps(workflow) {
  workflowEl.innerHTML = "";
  const row = document.createElement("div");
  row.className = "workflow-row";
  (workflow.nodes || []).forEach((n, i) => {
    const node = document.createElement("div");
    node.className = "wf-node";
    node.textContent = n.label;
    row.appendChild(node);
    if (i < workflow.nodes.length - 1) {
      const arrow = document.createElement("div");
      arrow.className = "wf-arrow";
      arrow.textContent = "→";
      row.appendChild(arrow);
    }
  });
  workflowEl.appendChild(row);
}

function renderWorkflowMetrics(metrics) {
  workflowMetricsEl.innerHTML = "";
  const sections = [
    {
      title: "数据导入",
      lines: [
        `评论数: ${metrics.data_import.total_reviews}`,
        `门店数: ${metrics.data_import.shops}`,
        `菜品数: ${metrics.data_import.dishes}`,
        `时间范围: ${metrics.data_import.time_range}`,
      ],
    },
    {
      title: "NLP处理",
      lines: [
        `平均情感: ${metrics.nlp.avg_sentiment}`,
        `标签覆盖率: ${(metrics.nlp.tag_coverage * 100).toFixed(1)}%`,
        `高频标签: ${(metrics.nlp.top_tags || []).map((x) => x.tag).join("、") || "无"}`,
      ],
    },
    {
      title: "图谱构建",
      lines: [
        `店铺节点: ${metrics.graph.shops}`,
        `菜品节点: ${metrics.graph.dishes}`,
        `用户节点: ${metrics.graph.users}`,
        `评分关系: ${metrics.graph.rated_edges}`,
      ],
    },
    {
      title: "趋势预测",
      lines: [
        `MAE: ${metrics.predict.mae}`,
        `RMSE: ${metrics.predict.rmse}`,
        `下一期变化: ${metrics.predict.predicted_change >= 0 ? "+" : ""}${metrics.predict.predicted_change.toFixed(3)}`,
      ],
    },
  ];

  sections.forEach((section) => {
    const card = document.createElement("div");
    card.className = "metric-card";
    card.innerHTML = `<h3>${section.title}</h3>`;
    section.lines.forEach((line) => {
      const item = document.createElement("div");
      item.className = "metric-item";
      item.textContent = line;
      card.appendChild(item);
    });
    workflowMetricsEl.appendChild(card);
  });
}

async function loadWorkflow(baseQuery) {
  const data = await fetchJson(`${API_BASE}/workflow?${baseQuery}`);
  renderWorkflowSteps(data.workflow || { nodes: [], edges: [] });
  renderWorkflowMetrics(
    data.metrics || {
      data_import: { total_reviews: 0, shops: 0, dishes: 0, time_range: "-" },
      nlp: { avg_sentiment: 0, tag_coverage: 0, top_tags: [] },
      graph: { shops: 0, dishes: 0, users: 0, rated_edges: 0 },
      predict: { mae: 0, rmse: 0, predicted_change: 0 },
    },
  );
}

async function refreshAll() {
  const baseQuery = buildQuery();
  await Promise.all([loadTrend(baseQuery), loadGraph(baseQuery), loadWorkflow(baseQuery)]);
}

document.getElementById("refreshBtn").addEventListener("click", refreshAll);
window.addEventListener("resize", () => {
  trendChart.resize();
  if (cy) cy.resize();
});

refreshAll().catch((err) => {
  trendChart.setOption({ title: { text: "加载失败，请先启动后端并导入数据" } });
  console.error(err);
});
