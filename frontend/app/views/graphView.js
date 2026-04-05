function nodeColor(nodeType) {
  if (nodeType === "shop" || nodeType === "Shop") return "#0f6b66";
  if (nodeType === "factor_category" || nodeType === "FactorCategory") return "#b6a6f8";
  if (nodeType === "factor" || nodeType === "Factor") return "#4eaac4";
  if (nodeType === "evidence" || nodeType === "Evidence" || nodeType === "Review") return "#90a6b8";
  return "#7f9fb1";
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function nodeSize(nodeType, importance = 0, selected = false) {
  const score = Number(importance || 0);
  if (nodeType === "shop" || nodeType === "Shop") {
    return clamp(26 + score * 2.4 + (selected ? 5 : 0), 28, 44);
  }
  if (nodeType === "factor_category" || nodeType === "FactorCategory") {
    return clamp(22 + score * 2.1 + (selected ? 4 : 0), 24, 40);
  }
  if (nodeType === "factor" || nodeType === "Factor") {
    return clamp(18 + score * 2.8 + (selected ? 5 : 0), 20, 42);
  }
  if (nodeType === "evidence" || nodeType === "Evidence" || nodeType === "Review") {
    return clamp(10 + score * 1.3, 10, 18);
  }
  return clamp(16 + score * 1.6, 16, 30);
}

function edgeColor(impact) {
  const value = Number(impact || 0);
  if (value > 0.03) return "#2f9b67";
  if (value < -0.03) return "#c85f5f";
  return "#8ea6b7";
}

function edgeOpacity(confidence) {
  return 0.25 + Number(confidence || 0) * 0.75;
}

export function createGraphView(container, onFactorSelected) {
  let cy = null;

  function render(model, options = {}) {
    const nodes = model?.nodes || [];
    const edges = model?.edges || [];
    const selectedFactor = options.selectedFactor || "";

    if (!nodes.length || !edges.length) {
      if (cy) {
        cy.destroy();
        cy = null;
      }
      container.innerHTML = `<div class="empty">当前筛选下没有可视化图谱数据。</div>`;
      return;
    }

    const importanceMap = new Map();
    const addImportance = (id, delta) => {
      importanceMap.set(id, Number(importanceMap.get(id) || 0) + Number(delta || 0));
    };

    edges.forEach((edge) => {
      const weightScore = Math.log2(Number(edge.weight || 1) + 1);
      const impactScore = Math.abs(Number(edge.impact || 0)) * 4;
      const score = weightScore + impactScore;
      addImportance(edge.source, score * 0.55);
      addImportance(edge.target, score);
    });

    const elements = [
      ...nodes.map((node) => ({
        data: {
          id: node.id,
          label: node.label,
          category: node.category || "",
          nodeType: node.node_type || node.type || "node",
          importance: Number(importanceMap.get(node.id) || 0),
        },
      })),
      ...edges.map((edge, index) => ({
        data: {
          id: `edge-${index}`,
          source: edge.source,
          target: edge.target,
          type: edge.type,
          weight: Number(edge.weight || 1),
          impact: Number(edge.impact || 0),
          confidence: Number(edge.confidence || 0),
          timeBucket: edge.time_bucket || "",
          category: edge.category || "",
        },
      })),
    ];

    if (cy) {
      cy.destroy();
      cy = null;
    }
    container.innerHTML = "";

    cy = window.cytoscape({
      container,
      elements,
      style: [
        {
          selector: "node",
          style: {
            label: "data(label)",
            "background-color": (ele) => nodeColor(ele.data("nodeType")),
            color: "#102434",
            "font-size": (ele) => {
              const type = ele.data("nodeType");
              if (type === "evidence" || type === "Evidence" || type === "Review") return 8;
              return 9;
            },
            "text-wrap": "wrap",
            "text-max-width": 88,
            "text-valign": "center",
            "text-halign": "center",
            width: (ele) =>
              nodeSize(
                ele.data("nodeType"),
                ele.data("importance"),
                selectedFactor && String(ele.id()) === `factor:${selectedFactor}`
              ),
            height: (ele) =>
              nodeSize(
                ele.data("nodeType"),
                ele.data("importance"),
                selectedFactor && String(ele.id()) === `factor:${selectedFactor}`
              ),
            "border-width": (ele) => {
              const id = String(ele.id());
              if (selectedFactor && id === `factor:${selectedFactor}`) return 2.4;
              return 1;
            },
            "border-color": (ele) => {
              const id = String(ele.id());
              if (selectedFactor && id === `factor:${selectedFactor}`) return "#f4a428";
              return "#e8eef4";
            },
          },
        },
        {
          selector: "edge",
          style: {
            width: (ele) => Math.min(5, 0.8 + Math.log2(Number(ele.data("weight") || 1))),
            "line-color": (ele) => edgeColor(ele.data("impact")),
            "line-opacity": (ele) => edgeOpacity(ele.data("confidence")),
            "target-arrow-shape": "triangle",
            "target-arrow-color": (ele) => edgeColor(ele.data("impact")),
            "curve-style": "bezier",
            label: (ele) => {
              const type = ele.data("type");
              if (type === "AFFECTED_BY") {
                return `${Number(ele.data("impact")).toFixed(2)}`;
              }
              if (type === "MENTIONS_FACTOR" && Number(ele.data("impact")) !== 0) {
                return `${Number(ele.data("impact")).toFixed(2)}`;
              }
              return "";
            },
            "font-size": 7,
            color: "#3f5d74",
            "text-background-opacity": 0,
          },
        },
      ],
      layout: {
        name: "cose",
        fit: true,
        animate: false,
        randomize: false,
        padding: 92,
        componentSpacing: 300,
        nodeRepulsion: 92000,
        nodeOverlap: 1800,
        idealEdgeLength: 250,
        edgeElasticity: 0.03,
        gravity: 0.08,
        gravityRange: 2.8,
        nestingFactor: 0.08,
        numIter: 2800,
      },
    });

    cy.on("tap", "node", (event) => {
      const node = event.target.data();
      if (node.nodeType === "factor" || node.nodeType === "Factor") {
        onFactorSelected(String(node.id).replace("factor:", ""));
      }
    });
  }

  function resize() {
    if (cy) cy.resize();
  }

  return { render, resize };
}
