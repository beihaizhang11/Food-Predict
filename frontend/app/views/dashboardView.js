export function createDashboardView({
  snapshotEl,
  impactEl,
  trendEl,
  predictionNoteEl,
  workflowEl,
  workflowMetricsEl,
  onTrendBucketClick,
}) {
  const impactChart = window.echarts.init(impactEl);
  const trendChart = window.echarts.init(trendEl);

  trendChart.on("click", (params) => {
    const bucket = String(params?.name || "").slice(0, 7);
    if (bucket) onTrendBucketClick(bucket);
  });

  function renderSnapshot(cards = []) {
    snapshotEl.innerHTML = cards
      .map(
        (card) => `
      <article class="snapshot-card">
        <h3>${card.title}</h3>
        <p>${card.value}</p>
        <small>${card.sub || ""}</small>
      </article>
    `
      )
      .join("");
  }

  function renderImpact(rows = [], anomalyBucket = "") {
    if (!rows.length) {
      impactChart.clear();
      impactChart.setOption({ title: { text: "暂无因子影响数据" } });
      return;
    }
    const categories = rows.map((x) => x.factor).reverse();
    const values = rows.map((x) => Number(x.netImpact)).reverse();
    impactChart.setOption({
      animationDuration: 500,
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        formatter(items) {
          const item = items?.[0];
          if (!item) return "";
          const row = rows.find((x) => x.factor === item.name);
          return [
            `${item.name} (${row?.category || ""})`,
            `净影响: ${Number(item.value).toFixed(3)}`,
            `提及: ${row?.mentionCount || 0}`,
            `置信: ${(Number(row?.confidence || 0) * 100).toFixed(1)}%`,
            anomalyBucket ? `${anomalyBucket} 窗口影响: ${Number(row?.bucketImpact || 0).toFixed(3)}` : "",
          ]
            .filter(Boolean)
            .join("<br/>");
        },
      },
      grid: { left: 110, right: 24, top: 18, bottom: 30 },
      xAxis: {
        type: "value",
        splitLine: { lineStyle: { color: "#dfe8ef" } },
        axisLabel: { color: "#567084" },
      },
      yAxis: {
        type: "category",
        data: categories,
        axisLabel: { color: "#314c61" },
      },
      series: [
        {
          type: "bar",
          data: values,
          itemStyle: {
            color(params) {
              return Number(params.value) >= 0 ? "#2f9b67" : "#c85f5f";
            },
          },
          label: {
            show: true,
            position: "right",
            color: "#2a445a",
            formatter: ({ value }) => Number(value).toFixed(2),
          },
        },
      ],
    });
  }

  function renderTrend(prediction = {}) {
    const history = prediction.history || [];
    const forecast = prediction.forecast || [];
    const historyX = history.map((x) => x.time);
    const historyY = history.map((x) => Number(x.rating || 0).toFixed(3));
    const forecastX = forecast.map((x) => x.time);
    const forecastY = forecast.map((x) => Number(x.rating || 0).toFixed(3));

    trendChart.setOption({
      animationDuration: 420,
      tooltip: { trigger: "axis" },
      legend: {
        data: ["历史评分", "预测评分"],
        top: 2,
        textStyle: { color: "#4f6980", fontSize: 11 },
      },
      grid: { left: 36, right: 18, top: 30, bottom: 30 },
      xAxis: {
        type: "category",
        data: [...historyX, ...forecastX],
        axisLabel: { color: "#5a7388", fontSize: 11 },
      },
      yAxis: {
        type: "value",
        min: 1,
        max: 5,
        splitLine: { lineStyle: { color: "#e2ebf2" } },
        axisLabel: { color: "#5a7388", fontSize: 11 },
      },
      series: [
        {
          name: "历史评分",
          type: "line",
          smooth: true,
          symbolSize: 4,
          data: historyY,
          lineStyle: { color: "#3f6eb5", width: 2 },
          itemStyle: { color: "#3f6eb5" },
        },
        {
          name: "预测评分",
          type: "line",
          smooth: true,
          symbolSize: 4,
          data: [...Array(historyY.length).fill(null), ...forecastY],
          lineStyle: { color: "#63a872", width: 2, type: "dashed" },
          itemStyle: { color: "#63a872" },
        },
      ],
    });

    predictionNoteEl.textContent = prediction.explanation?.summary || "暂无预测解释。";
  }

  function renderWorkflow(workflowPayload = {}) {
    const workflow = workflowPayload.workflow || { nodes: [] };
    const metrics = workflowPayload.metrics || {};
    const nodes = workflow.nodes || [];
    workflowEl.innerHTML = `
      <div class="wf-row">
        ${nodes
          .map((node, index) => {
            const arrow = index < nodes.length - 1 ? `<span class="wf-arrow">→</span>` : "";
            return `<div class="wf-node">${node.label}</div>${arrow}`;
          })
          .join("")}
      </div>
    `;

    const cards = [
      {
        title: "数据层",
        lines: [
          `评论: ${metrics?.data_import?.total_reviews || 0}`,
          `商家: ${metrics?.data_import?.shops || 0}`,
          `范围: ${metrics?.data_import?.time_range || "-"}`,
        ],
      },
      {
        title: "NLP层",
        lines: [
          `平均情感: ${metrics?.nlp?.avg_sentiment || 0}`,
          `标签覆盖: ${((metrics?.nlp?.tag_coverage || 0) * 100).toFixed(1)}%`,
          `高频标签: ${(metrics?.nlp?.top_tags || []).map((x) => x.tag).join(" / ") || "-"}`,
        ],
      },
      {
        title: "图谱层",
        lines: [
          `商家节点: ${metrics?.graph?.shops || 0}`,
          `因子节点: ${metrics?.graph?.dishes || 0}`,
          `证据节点: ${metrics?.graph?.users || 0}`,
          `影响边: ${metrics?.graph?.rated_edges || 0}`,
        ],
      },
      {
        title: "预测层",
        lines: [
          `MAE: ${metrics?.predict?.mae || 0}`,
          `RMSE: ${metrics?.predict?.rmse || 0}`,
          `下一期变化: ${Number(metrics?.predict?.predicted_change || 0).toFixed(3)}`,
        ],
      },
    ];

    workflowMetricsEl.innerHTML = cards
      .map(
        (card) => `
        <article class="metric-card">
          <h4>${card.title}</h4>
          ${card.lines.map((line) => `<div class="metric-item">${line}</div>`).join("")}
        </article>
      `
      )
      .join("");
  }

  function resize() {
    impactChart.resize();
    trendChart.resize();
  }

  return { renderSnapshot, renderImpact, renderTrend, renderWorkflow, resize };
}
