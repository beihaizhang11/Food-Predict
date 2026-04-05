import { createStore } from "./state/store.js";
import {
  getFactorEvidence,
  getFactorInsights,
  getGraphDetail,
  getGraphSummary,
  getOptions,
  getPrediction,
  getWorkflow,
} from "./services/api.js";
import {
  buildGraphViewModel,
  detectAnomalyBucket,
  toFactorMap,
  toImpactChartModel,
  toSnapshotModel,
} from "./adapters/researchAdapter.js";
import { createGraphView } from "./views/graphView.js";
import { createInsightView } from "./views/insightView.js";
import { createDashboardView } from "./views/dashboardView.js";

const refs = {
  startDate: document.getElementById("startDate"),
  endDate: document.getElementById("endDate"),
  shopSelect: document.getElementById("shopSelect"),
  minImpactRange: document.getElementById("minImpactRange"),
  minImpactValue: document.getElementById("minImpactValue"),
  refreshBtn: document.getElementById("refreshBtn"),
  anomalyBtn: document.getElementById("anomalyBtn"),
  categoryFilter: document.getElementById("categoryFilter"),
  statusText: document.getElementById("statusText"),
  snapshotBar: document.getElementById("snapshotBar"),
  graphHint: document.getElementById("graphHint"),
  modeButtons: Array.from(document.querySelectorAll(".mode-btn")),
};

const graphView = createGraphView(document.getElementById("graphCanvas"), (factor) => {
  onFactorSelected(factor);
});

const insightView = createInsightView({
  headlineEl: document.getElementById("factorHeadline"),
  statsEl: document.getElementById("factorStats"),
  timelineEl: document.getElementById("factorTimeline"),
  evidenceEl: document.getElementById("evidenceList"),
});

const dashboardView = createDashboardView({
  snapshotEl: document.getElementById("snapshotBar"),
  impactEl: document.getElementById("impactChart"),
  trendEl: document.getElementById("trendChart"),
  predictionNoteEl: document.getElementById("predictionNote"),
  workflowEl: document.getElementById("workflowChart"),
  workflowMetricsEl: document.getElementById("workflowMetrics"),
  onTrendBucketClick(bucket) {
    store.setState((prev) => ({
      ui: { ...prev.ui, focusBucket: bucket },
    }));
    render();
  },
});

const store = createStore({
  options: { shops: [] },
  filters: {
    start: "",
    end: "",
    shop_id: "",
    minImpact: 0.1,
    categories: new Set(),
  },
  ui: {
    mode: "overview",
    anomalyMode: false,
    anomaly: null,
    focusBucket: "",
    selectedFactor: "",
  },
  data: {
    prediction: null,
    summaryGraph: null,
    detailGraph: null,
    factorsPayload: null,
    factorMap: new Map(),
    evidencePayload: null,
    workflowPayload: null,
  },
  loading: false,
  error: "",
});

function setStatus(text) {
  refs.statusText.textContent = text || "";
}

function readFilterInputs() {
  return {
    start: refs.startDate.value || "",
    end: refs.endDate.value || "",
    shop_id: refs.shopSelect.value || "",
  };
}

function syncFilterInputs(filters) {
  refs.minImpactValue.textContent = Number(filters.minImpact || 0).toFixed(2);
}

function renderCategoryChips() {
  const state = store.getState();
  const categories = state.data.factorsPayload?.categories || [];
  const selected = state.filters.categories;
  refs.categoryFilter.innerHTML = "";

  if (!categories.length) {
    refs.categoryFilter.innerHTML = `<span class="empty">暂无可筛选分类</span>`;
    return;
  }

  categories.forEach((row) => {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = `chip${selected.size === 0 || selected.has(row.category) ? " is-active" : ""}`;
    chip.textContent = `${row.category} (${row.mention_count})`;
    chip.addEventListener("click", () => {
      store.setState((prev) => {
        const next = new Set(prev.filters.categories);
        if (next.has(row.category)) {
          next.delete(row.category);
        } else {
          next.add(row.category);
        }
        return { filters: { ...prev.filters, categories: next } };
      });
      render();
      renderCategoryChips();
    });
    refs.categoryFilter.appendChild(chip);
  });
}

function renderModeButtons() {
  const mode = store.getState().ui.mode;
  refs.modeButtons.forEach((btn) => {
    btn.classList.toggle("is-active", btn.dataset.mode === mode);
  });
}

function render() {
  const state = store.getState();
  const { prediction, summaryGraph, detailGraph, factorsPayload, factorMap, evidencePayload, workflowPayload } =
    state.data;
  const { minImpact, categories } = state.filters;
  const { mode, selectedFactor, anomalyMode, anomaly, focusBucket } = state.ui;

  syncFilterInputs(state.filters);
  renderModeButtons();

  if (!prediction || !factorsPayload || !summaryGraph || !detailGraph) {
    return;
  }

  const effectiveBucket = anomalyMode && anomaly?.bucket ? anomaly.bucket : focusBucket;
  const snapshot = toSnapshotModel({
    prediction,
    factors: factorsPayload,
    filters: state.filters,
    anomalyMode,
  });
  dashboardView.renderSnapshot(snapshot);

  const impactRows = toImpactChartModel(factorsPayload, categories, effectiveBucket);
  dashboardView.renderImpact(impactRows, effectiveBucket);
  dashboardView.renderTrend(prediction);
  dashboardView.renderWorkflow(workflowPayload || {});

  const graphModel = buildGraphViewModel({
    summaryGraph,
    detailGraph,
    factorMap,
    minImpact,
    selectedCategories: categories,
    focusBucket: effectiveBucket,
    mode,
    selectedFactor,
  });
  graphView.render(graphModel, { selectedFactor });

  const selectedFactorRow = selectedFactor ? factorMap.get(selectedFactor) : null;
  insightView.renderFactor(selectedFactorRow, anomalyMode ? anomaly : null);
  insightView.renderEvidence(evidencePayload || { items: [] });

  refs.graphHint.textContent =
    mode === "overview"
      ? "图谱总览：边颜色表示影响方向，边宽表示影响强度，透明度表示证据置信。"
      : mode === "evidence"
        ? `证据子图：当前聚焦因子 ${selectedFactor || "未选择"}，可追溯到评论证据节点。`
        : `时间片视图：聚焦时间窗口 ${effectiveBucket || "未指定"} 的因子贡献。`;
}

async function loadEvidence(factor) {
  const state = store.getState();
  const filters = readFilterInputs();
  const payload = await getFactorEvidence(filters, factor, 1, 20);
  store.setState((prev) => ({
    ui: { ...prev.ui, selectedFactor: factor || "" },
    data: { ...prev.data, evidencePayload: payload },
  }));
  render();
}

async function onFactorSelected(factor) {
  if (!factor) return;
  setStatus(`加载因子 ${factor} 的证据中...`);
  try {
    await loadEvidence(factor);
    setStatus(`已聚焦因子 ${factor}`);
  } catch (error) {
    setStatus(`加载证据失败: ${String(error.message || error)}`);
  }
}

async function refresh() {
  const filters = readFilterInputs();
  const state = store.getState();
  setStatus("正在刷新研究视图...");
  store.setState({ loading: true, error: "", filters: { ...state.filters, ...filters } });

  try {
    const [prediction, summaryGraph, detailGraph, factorsPayload, workflowPayload] = await Promise.all([
      getPrediction(filters),
      getGraphSummary(filters),
      getGraphDetail(filters),
      getFactorInsights(filters, 24, state.filters.minImpact),
      getWorkflow(filters),
    ]);

    const factorMap = toFactorMap(factorsPayload);
    let selectedFactor = store.getState().ui.selectedFactor;
    if (!selectedFactor || !factorMap.has(selectedFactor)) {
      selectedFactor = factorsPayload.factors?.[0]?.factor || "";
    }

    let anomaly = null;
    if (store.getState().ui.anomalyMode) {
      anomaly = detectAnomalyBucket(prediction.history || []);
    }

    store.setState((prev) => ({
      loading: false,
      data: {
        ...prev.data,
        prediction,
        summaryGraph,
        detailGraph,
        factorsPayload,
        factorMap,
        workflowPayload,
      },
      ui: {
        ...prev.ui,
        selectedFactor,
        anomaly,
        focusBucket: prev.ui.focusBucket || anomaly?.bucket || "",
      },
    }));

    renderCategoryChips();
    if (selectedFactor) {
      await loadEvidence(selectedFactor);
    } else {
      store.setState((prev) => ({
        data: { ...prev.data, evidencePayload: { items: [] } },
      }));
      render();
    }
    setStatus("研究视图已刷新");
  } catch (error) {
    console.error(error);
    store.setState({ loading: false, error: String(error.message || error) });
    setStatus(`加载失败: ${String(error.message || error)}`);
  }
}

async function bootstrap() {
  try {
    setStatus("正在加载商家列表...");
    const payload = await getOptions();
    const shops = payload.shops || [];

    refs.shopSelect.innerHTML = `<option value="">全部商家</option>`;
    shops.forEach((shop) => {
      const option = document.createElement("option");
      option.value = shop.shop_id;
      option.textContent = `${shop.display_name || shop.shop_id} (${shop.review_count}条)`;
      refs.shopSelect.appendChild(option);
    });
    if (shops.length > 0) refs.shopSelect.value = shops[0].shop_id;

    store.setState((prev) => ({
      options: { shops },
      filters: { ...prev.filters, shop_id: refs.shopSelect.value || "" },
    }));
    await refresh();
  } catch (error) {
    console.error(error);
    setStatus(`初始化失败: ${String(error.message || error)}`);
  }
}

refs.refreshBtn.addEventListener("click", refresh);
refs.shopSelect.addEventListener("change", refresh);
refs.startDate.addEventListener("change", refresh);
refs.endDate.addEventListener("change", refresh);
refs.minImpactRange.addEventListener("input", (event) => {
  const value = Number(event.target.value || 0);
  store.setState((prev) => ({
    filters: { ...prev.filters, minImpact: value },
  }));
  render();
});
refs.anomalyBtn.addEventListener("click", () => {
  store.setState((prev) => {
    const enabled = !prev.ui.anomalyMode;
    const anomaly = enabled ? detectAnomalyBucket(prev.data.prediction?.history || []) : null;
    return {
      ui: {
        ...prev.ui,
        anomalyMode: enabled,
        anomaly,
        focusBucket: enabled && anomaly?.bucket ? anomaly.bucket : prev.ui.focusBucket,
      },
    };
  });
  refs.anomalyBtn.classList.toggle("is-active");
  render();
});

refs.modeButtons.forEach((button) => {
  button.addEventListener("click", () => {
    store.setState((prev) => ({
      ui: { ...prev.ui, mode: button.dataset.mode || "overview" },
    }));
    render();
  });
});

window.addEventListener("resize", () => {
  graphView.resize();
  insightView.resize();
  dashboardView.resize();
});

bootstrap();
