const API_BASE =
  window.location.origin && window.location.origin !== "null"
    ? `${window.location.origin}/api`
    : "http://127.0.0.1:5000/api";

const els = {
  start: document.getElementById("start"),
  end: document.getElementById("end"),
  shopId: document.getElementById("shopId"),
  refreshBtn: document.getElementById("refreshBtn"),
  statusText: document.getElementById("statusText"),
  explainSummary: document.getElementById("explainSummary"),
  driverCards: document.getElementById("driverCards"),
  workflow: document.getElementById("workflowChart"),
  workflowMetrics: document.getElementById("workflowMetrics"),
  graph: document.getElementById("graphChart"),
};

const trendChart = echarts.init(document.getElementById("trendChart"));
const attributeImpactChart = echarts.init(document.getElementById("attributeImpactChart"));
let cy = null;

function setStatus(text) {
  els.statusText.textContent = text || "";
}

function buildQuery(extra = {}) {
  const params = new URLSearchParams();
  if (els.start.value) params.set("start", els.start.value);
  if (els.end.value) params.set("end", els.end.value);
  if (els.shopId.value) params.set("shop_id", els.shopId.value);
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

async function loadOptions() {
  const data = await fetchJson(`${API_BASE}/options`);
  const shops = data.shops || [];
  els.shopId.innerHTML = `<option value="">全部商家</option>`;
  shops.forEach((s) => {
    const op = document.createElement("option");
    op.value = s.shop_id;
    op.textContent = `${s.shop_id}（${s.review_count}条）`;
    els.shopId.appendChild(op);
  });
  if (shops.length > 0) {
    els.shopId.value = shops[0].shop_id;
  }
}

function renderTrend(data) {
  const history = data.history || [];
  const forecast = data.forecast || [];
  if (!history.length && !forecast.length) {
    trendChart.clear();
    trendChart.setOption({ title: { text: "当前筛选下无趋势数据" } });
    return;
  }

  const historyX = history.map((x) => x.time);
  const historyY = history.map((x) => Number(x.rating).toFixed(2));
  const forecastX = forecast.map((x) => x.time);
  const forecastY = forecast.map((x) => Number(x.rating).toFixed(2));

  trendChart.setOption({
    title: { text: "" },
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
}

function renderAttributeImpactChart(explanation = {}) {
  const positive = (explanation.top_positive_attributes || []).map((x) => ({
    name: x.attribute,
    value: Number(x.impact || 0),
  }));
  const negative = (explanation.top_negative_attributes || []).map((x) => ({
    name: x.attribute,
    value: Number(x.impact || 0),
  }));
  const merged = [...positive, ...negative];
  if (!merged.length) {
    attributeImpactChart.clear();
    attributeImpactChart.setOption({ title: { text: "当前筛选下无属性影响数据" } });
    return;
  }

  const map = new Map();
  merged.forEach((item) => map.set(item.name, (map.get(item.name) || 0) + item.value));
  const rows = Array.from(map.entries())
    .map(([name, value]) => ({ name, value }))
    .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
    .slice(0, 10);

  attributeImpactChart.setOption({
    title: { text: "" },
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      formatter: (items) => {
        const it = items[0];
        const direction = Number(it.value) >= 0 ? "正向" : "负向";
        return `${it.name}<br/>${direction}影响: ${Number(it.value).toFixed(3)}`;
      },
    },
    grid: { left: 120, right: 20, top: 18, bottom: 18 },
    xAxis: { type: "value", axisLabel: { formatter: (v) => Number(v).toFixed(2) } },
    yAxis: { type: "category", data: rows.map((r) => r.name) },
    series: [
      {
        type: "bar",
        data: rows.map((r) => r.value),
        itemStyle: { color: (p) => (Number(p.value) >= 0 ? "#3fa46a" : "#d65f5f") },
        label: { show: true, position: "right", formatter: ({ value }) => Number(value).toFixed(2) },
      },
    ],
  });
}

function renderExplanation(explanation = {}) {
  els.explainSummary.textContent = explanation.summary || "当前筛选下暂无解释。";

  const positive = explanation.top_positive_factors || [];
  const negative = explanation.top_negative_factors || [];
  const attrs = explanation.top_attributes || [];
  const bridge = explanation.feature_bridge || null;

  const cards = [
    {
      title: "正向驱动因子",
      items: positive.map((x) => `${x.factor}（Δ${x.delta}，影响${x.impact}）`),
    },
    {
      title: "负向驱动因子",
      items: negative.map((x) => `${x.factor}（Δ${x.delta}，影响${x.impact}）`),
    },
    {
      title: "高频关注属性",
      items: attrs.map((x) => `${x.attribute}（${x.count}次）`),
    },
    {
      title: "模型输入侧信号",
      items: bridge
        ? [
            `情感趋势: ${Number(bridge.sentiment_trend || 0).toFixed(3)}`,
            `评论量趋势: ${Number(bridge.volume_trend || 0).toFixed(3)}`,
            `综合信号: ${bridge.model_side_signal || "中性"}`,
          ]
        : [],
    },
  ];

  els.driverCards.innerHTML = "";
  cards.forEach((card) => {
    const node = document.createElement("div");
    node.className = "driver-card";
    node.innerHTML = `<h3>${card.title}</h3>`;
    if (!card.items.length) {
      const item = document.createElement("div");
      item.className = "driver-item";
      item.textContent = "无";
      node.appendChild(item);
    } else {
      card.items.forEach((txt) => {
        const item = document.createElement("div");
        item.className = "driver-item";
        item.textContent = txt;
        node.appendChild(item);
      });
    }
    els.driverCards.appendChild(node);
  });

  renderAttributeImpactChart(explanation);
}

function nodeColorByType(type) {
  if (type === "Shop") return "#176f63";
  if (type === "Factor") return "#2f8f7f";
  if (type === "Review") return "#8fa6b9";
  return "#7f9fb5";
}

function edgeColorByImpact(edge) {
  const impact = Number(edge.data("impact") || 0);
  if (impact > 0.05) return "#3fa46a";
  if (impact < -0.05) return "#d65f5f";
  return "#9bb4c6";
}

function renderGraph(graphData) {
  const nodes = graphData.nodes || [];
  const edges = graphData.edges || [];
  if (!nodes.length) {
    if (cy) {
      cy.destroy();
      cy = null;
    }
    els.graph.innerHTML = "<div style='padding:16px;color:#6f8192;'>当前筛选下无图谱数据</div>";
    return;
  }

  els.graph.innerHTML = "";
  const elements = [
    ...nodes.map((n) => ({
      data: {
        id: n.id,
        label: `${n.type}:${n.label}`,
        type: n.type,
        category: n.category || "",
      },
    })),
    ...edges.map((e, idx) => ({
      data: {
        id: `e${idx}`,
        source: e.source,
        target: e.target,
        type: e.type,
        impact: Number(e.impact || 0),
        label:
          e.type === "AFFECTED_BY"
            ? `${e.type}（频次${e.weight || 1}, 影响${Number(e.impact || 0).toFixed(2)}）`
            : `${e.type} (${e.weight || 1})`,
        weight: Number(e.weight || 1),
      },
    })),
  ];

  if (cy) cy.destroy();
  cy = cytoscape({
    container: els.graph,
    elements,
    style: [
      {
        selector: "node",
        style: {
          label: "data(label)",
          "background-color": (ele) => nodeColorByType(ele.data("type")),
          color: "#0a1620",
          "font-size": 10,
          "text-wrap": "wrap",
          "text-max-width": 92,
          "text-valign": "center",
          "text-halign": "center",
          width: 40,
          height: 40,
        },
      },
      {
        selector: "edge",
        style: {
          label: "data(label)",
          width: (ele) => Math.min(7, 1 + Math.log2(Number(ele.data("weight") || 1))),
          "line-color": (ele) => edgeColorByImpact(ele),
          "target-arrow-shape": "triangle",
          "target-arrow-color": (ele) => edgeColorByImpact(ele),
          "curve-style": "bezier",
          "font-size": 8,
          color: "#4c6576",
        },
      },
    ],
    layout: { name: "cose", animate: true, fit: true, padding: 24, nodeRepulsion: 9000 },
  });
}

function renderWorkflowSteps(workflow = { nodes: [] }) {
  els.workflow.innerHTML = "";
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
  els.workflow.appendChild(row);
}

function renderWorkflowMetrics(metrics = null) {
  const m = metrics || {
    data_import: { total_reviews: 0, shops: 0, dishes: 0, time_range: "-" },
    nlp: { avg_sentiment: 0, tag_coverage: 0, top_tags: [] },
    graph: { shops: 0, dishes: 0, users: 0, rated_edges: 0 },
    predict: { mae: 0, rmse: 0, predicted_change: 0 },
  };

  const sections = [
    {
      title: "数据导入",
      lines: [
        `评论数: ${m.data_import.total_reviews}`,
        `商家数: ${m.data_import.shops}`,
        `时间范围: ${m.data_import.time_range}`,
      ],
    },
    {
      title: "文本分析",
      lines: [
        `平均情感: ${m.nlp.avg_sentiment}`,
        `标签覆盖率: ${(m.nlp.tag_coverage * 100).toFixed(1)}%`,
        `高频标签: ${(m.nlp.top_tags || []).map((x) => x.tag).join("、") || "无"}`,
      ],
    },
    {
      title: "图谱统计",
      lines: [
        `店铺节点: ${m.graph.shops}`,
        `因子节点: ${m.graph.dishes}`,
        `评论节点: ${m.graph.users}`,
        `因子关系: ${m.graph.rated_edges}`,
      ],
    },
    {
      title: "趋势预测",
      lines: [
        `MAE: ${m.predict.mae}`,
        `RMSE: ${m.predict.rmse}`,
        `下一期变化: ${m.predict.predicted_change >= 0 ? "+" : ""}${Number(m.predict.predicted_change).toFixed(3)}`,
      ],
    },
  ];

  els.workflowMetrics.innerHTML = "";
  sections.forEach((s) => {
    const card = document.createElement("div");
    card.className = "metric-card";
    card.innerHTML = `<h3>${s.title}</h3>`;
    s.lines.forEach((line) => {
      const item = document.createElement("div");
      item.className = "metric-item";
      item.textContent = line;
      card.appendChild(item);
    });
    els.workflowMetrics.appendChild(card);
  });
}

async function refreshAll() {
  const baseQuery = buildQuery();
  setStatus("正在刷新数据...");
  try {
    const filters = Object.fromEntries(new URLSearchParams(baseQuery).entries());
    const [predictData, graphData, workflowData] = await Promise.all([
      fetchJson(`${API_BASE}/predict`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ granularity: "month", horizon: 6, filters }),
      }),
      fetchJson(`${API_BASE}/graph?${buildQuery({ view: "summary", limit: 24 })}`),
      fetchJson(`${API_BASE}/workflow?${baseQuery}`),
    ]);

    renderTrend(predictData);
    renderExplanation(predictData.explanation || {});
    renderGraph(graphData);
    renderWorkflowSteps(workflowData.workflow || { nodes: [] });
    renderWorkflowMetrics(workflowData.metrics);
    setStatus("刷新完成");
  } catch (err) {
    console.error(err);
    setStatus(`加载失败：${String(err.message || err)}`);
    trendChart.clear();
    trendChart.setOption({ title: { text: "加载失败，请检查后端和数据" } });
    attributeImpactChart.clear();
    attributeImpactChart.setOption({ title: { text: "加载失败，暂无属性影响数据" } });
    renderExplanation({});
    renderWorkflowMetrics(null);
  }
}

async function bootstrap() {
  try {
    setStatus("正在加载商家列表...");
    await loadOptions();
    await refreshAll();
  } catch (err) {
    console.error(err);
    setStatus(`初始化失败：${String(err.message || err)}`);
  }
}

els.refreshBtn.addEventListener("click", refreshAll);
els.shopId.addEventListener("change", refreshAll);

window.addEventListener("resize", () => {
  trendChart.resize();
  attributeImpactChart.resize();
  if (cy) cy.resize();
});

bootstrap();
