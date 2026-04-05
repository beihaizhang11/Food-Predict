export function createInsightView({ headlineEl, statsEl, timelineEl, evidenceEl }) {
  const timelineChart = window.echarts.init(timelineEl);

  function renderFactor(factorRow, anomaly = null) {
    if (!factorRow) {
      headlineEl.innerHTML = `<div class="empty">请选择一个因子节点查看影响解释。</div>`;
      statsEl.innerHTML = "";
      timelineChart.clear();
      return;
    }

    const direction = Number(factorRow.net_impact || 0) >= 0 ? "正向" : "负向";
    const anomalyText =
      anomaly && anomaly.bucket
        ? `异常点窗口 ${anomaly.bucket}（变化 ${Number(anomaly.delta).toFixed(3)}）`
        : "未启用异常点模式";
    headlineEl.innerHTML = `
      <strong>${factorRow.factor}</strong>（${factorRow.category}）<br/>
      净影响 ${Number(factorRow.net_impact).toFixed(3)}，方向 ${direction}，证据置信 ${(
        Number(factorRow.confidence || 0) * 100
      ).toFixed(1)}%。<br/>
      ${anomalyText}
    `;

    statsEl.innerHTML = `
      <div class="stat"><h4>提及频次</h4><p>${factorRow.mention_count}</p></div>
      <div class="stat"><h4>平均影响</h4><p>${Number(factorRow.avg_impact || 0).toFixed(3)}</p></div>
      <div class="stat"><h4>平均情感</h4><p>${Number(factorRow.avg_sentiment || 0).toFixed(3)}</p></div>
      <div class="stat"><h4>平均极性</h4><p>${Number(factorRow.avg_polarity || 0).toFixed(3)}</p></div>
    `;

    const timeline = factorRow.timeline || [];
    timelineChart.setOption({
      animationDuration: 300,
      tooltip: { trigger: "axis" },
      grid: { left: 36, right: 16, top: 20, bottom: 28 },
      xAxis: {
        type: "category",
        data: timeline.map((x) => x.bucket),
        axisLabel: { color: "#5a7388", fontSize: 11 },
      },
      yAxis: {
        type: "value",
        axisLabel: { color: "#5a7388", fontSize: 11 },
        splitLine: { lineStyle: { color: "#e0eaf2" } },
      },
      series: [
        {
          type: "line",
          smooth: true,
          symbolSize: 5,
          data: timeline.map((x) => Number(x.impact || 0)),
          lineStyle: { color: "#2f77b4", width: 2 },
          itemStyle: { color: "#2f77b4" },
          areaStyle: { color: "rgba(47,119,180,0.12)" },
        },
      ],
    });
  }

  function renderEvidence(payload) {
    const rows = payload?.items || [];
    if (!rows.length) {
      evidenceEl.innerHTML = `<div class="empty">当前因子暂无可展示证据。</div>`;
      return;
    }
    evidenceEl.innerHTML = rows
      .map(
        (row) => `
        <article class="evidence-item">
          <div class="evidence-meta">
            <span>R${row.review_id} | ${row.time_bucket}</span>
            <span>impact ${Number(row.impact).toFixed(3)} | c ${Number(row.confidence).toFixed(2)}</span>
          </div>
          <div class="evidence-meta">
            <span>rating ${Number(row.rating).toFixed(2)}</span>
            <span>sentiment ${Number(row.sentiment).toFixed(3)}</span>
          </div>
          <div class="evidence-text">${row.snippet}</div>
        </article>
      `
      )
      .join("");
  }

  function resize() {
    timelineChart.resize();
  }

  return { renderFactor, renderEvidence, resize };
}
