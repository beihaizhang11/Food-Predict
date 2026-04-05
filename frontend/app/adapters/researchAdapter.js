function clamp01(value) {
  return Math.max(0, Math.min(1, Number(value || 0)));
}

export function toSnapshotModel({ prediction, factors, filters, anomalyMode }) {
  const factorRows = factors?.factors || [];
  const top = factorRows[0];
  const predictedDelta = Number(prediction?.explanation?.predicted_change || 0);
  const trendDirection = predictedDelta >= 0 ? "上升" : "下降";
  const confidence = top ? clamp01(top.confidence) : 0;

  return [
    {
      title: "样本规模",
      value: String(factors?.filters?.review_count || 0),
      sub: `${factors?.factor_count || 0} 个因子`,
    },
    {
      title: "时间窗口",
      value: filters.start && filters.end ? `${filters.start} ~ ${filters.end}` : "全时段",
      sub: anomalyMode ? "异常点模式已启用" : "常规模式",
    },
    {
      title: "预测方向",
      value: `${trendDirection} ${predictedDelta >= 0 ? "+" : ""}${predictedDelta.toFixed(3)}`,
      sub: prediction?.explanation?.feature_bridge?.signal_alignment || "neutral",
    },
    {
      title: "主导因子",
      value: top ? top.factor : "无",
      sub: top ? `${top.category} | 影响 ${Number(top.net_impact).toFixed(3)}` : "暂无",
    },
    {
      title: "结论置信",
      value: `${(confidence * 100).toFixed(1)}%`,
      sub: "因子频次与一致性估计",
    },
  ];
}

export function toImpactChartModel(factorsPayload, selectedCategories, anomalyBucket = "") {
  const factors = (factorsPayload?.factors || []).filter((row) =>
    selectedCategories.size ? selectedCategories.has(row.category) : true
  );

  return factors.slice(0, 12).map((row) => {
    const timelineRow = (row.timeline || []).find((t) => t.bucket === anomalyBucket);
    return {
      factor: row.factor,
      category: row.category,
      netImpact: Number(row.net_impact || 0),
      mentionCount: Number(row.mention_count || 0),
      confidence: Number(row.confidence || 0),
      bucketImpact: Number(timelineRow?.impact || 0),
    };
  });
}

export function toFactorMap(factorsPayload) {
  const map = new Map();
  (factorsPayload?.factors || []).forEach((row) => map.set(row.factor, row));
  return map;
}

export function detectAnomalyBucket(history = []) {
  if (!history.length || history.length < 3) {
    return null;
  }
  let best = null;
  for (let i = 1; i < history.length; i += 1) {
    const current = Number(history[i].rating || 0);
    const prev = Number(history[i - 1].rating || 0);
    const delta = current - prev;
    if (!best || Math.abs(delta) > Math.abs(best.delta)) {
      best = {
        bucket: String(history[i].time || "").slice(0, 7),
        delta,
        at: history[i].time,
      };
    }
  }
  return best;
}

export function buildGraphViewModel({
  summaryGraph,
  detailGraph,
  factorMap,
  minImpact,
  selectedCategories,
  focusBucket,
  mode,
  selectedFactor,
}) {
  const summaryNodes = summaryGraph?.nodes || [];
  const summaryEdges = summaryGraph?.edges || [];
  const detailNodes = detailGraph?.nodes || [];
  const detailEdges = detailGraph?.edges || [];

  const categoryAllowed = (category) => (selectedCategories.size ? selectedCategories.has(category) : true);
  const edgeAllowed = (edge) => {
    if (edge.type === "HAS_FACTOR") {
      return categoryAllowed(edge.category || "");
    }
    if (edge.type === "AFFECTED_BY" || edge.type === "MENTIONS_FACTOR") {
      return Math.abs(Number(edge.impact || 0)) >= minImpact && categoryAllowed(edge.category || "");
    }
    return true;
  };

  const factorBucketImpact = {};
  factorMap.forEach((row, factorName) => {
    const t = (row.timeline || []).find((x) => x.bucket === focusBucket);
    factorBucketImpact[factorName] = Number(t?.impact || 0);
  });

  let nodes = summaryNodes;
  let edges = summaryEdges.filter(edgeAllowed);

  if (mode === "evidence") {
    nodes = detailNodes;
    edges = detailEdges.filter((edge) => {
      if (!edgeAllowed(edge)) return false;
      if (!selectedFactor) return true;
      if (edge.target === `factor:${selectedFactor}` || edge.source === `factor:${selectedFactor}`) return true;
      if (edge.type === "HAS_FACTOR" && edge.target === `factor:${selectedFactor}`) return true;
      if (edge.type === "MENTIONS_FACTOR") {
        return edge.target === `factor:${selectedFactor}`;
      }
      if (edge.type === "HAS_EVIDENCE") {
        return detailEdges.some(
          (e) =>
            e.type === "MENTIONS_FACTOR" &&
            e.source === edge.target &&
            e.target === `factor:${selectedFactor}`
        );
      }
      return false;
    });
  }

  if (mode === "timeline" && focusBucket) {
    edges = edges.filter((edge) => {
      if (edge.type === "AFFECTED_BY") {
        const factorName = String(edge.target || "").replace("factor:", "");
        return Math.abs(Number(factorBucketImpact[factorName] || 0)) >= minImpact;
      }
      return edge.type === "HAS_FACTOR";
    });
  }

  const nodeIds = new Set();
  edges.forEach((edge) => {
    nodeIds.add(edge.source);
    nodeIds.add(edge.target);
  });
  nodes = nodes.filter((node) => nodeIds.has(node.id) || node.type === "Shop");

  return { nodes, edges, factorBucketImpact };
}
