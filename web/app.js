document.addEventListener('DOMContentLoaded', () => {
  const odateInput = document.getElementById('odateInput');
  const btnRefresh = document.getElementById('btnRefresh');
  const btnOrderPlan = document.getElementById('btnOrderPlan');
  const agentCountText = document.getElementById('agentCountText');
  const runsTableBody = document.getElementById('runsTableBody');

  // Header & Search
  const globalSearch = document.getElementById('globalSearch');
  const btnViewFullDag = document.getElementById('btnViewFullDag');
  const btnViewSplit = document.getElementById('btnViewSplit');
  const btnViewFullTable = document.getElementById('btnViewFullTable');
  const mainWorkspace = document.getElementById('mainWorkspace');

  // Status Ribbon Chips
  const ribbonChips = document.querySelectorAll('.ribbon-chip');
  const ribbonFilterInfo = document.getElementById('ribbonFilterInfo');
  const activeFilterText = document.getElementById('activeFilterText');
  const btnClearStateFilter = document.getElementById('btnClearStateFilter');

  // Tab Navigation Elements
  const tabButtons = document.querySelectorAll('.tab-btn');
  const tabPanes = document.querySelectorAll('.tab-content-pane');
  const tabBadgeRuns = document.getElementById('tabBadgeRuns');
  const tabBadgeDefs = document.getElementById('tabBadgeDefs');
  const tabBadgeConds = document.getElementById('tabBadgeConds');
  const tabBadgeAudits = document.getElementById('tabBadgeAudits');
  const tabBadgeAgents = document.getElementById('tabBadgeAgents');

  // Tab Containers
  const containerRuns = document.getElementById('containerRuns');
  const containerDefs = document.getElementById('containerDefs');
  const containerConds = document.getElementById('containerConds');
  const containerAudits = document.getElementById('containerAudits');
  const containerAgents = document.getElementById('containerAgents');

  // Sub Tables & Form Controls
  const defsTableBody = document.getElementById('defsTableBody');
  const condsTableBody = document.getElementById('condsTableBody');
  const inputNewCondName = document.getElementById('inputNewCondName');
  const btnAddCondition = document.getElementById('btnAddCondition');
  const auditsTableBody = document.getElementById('auditsTableBody');
  const agentsTableBody = document.getElementById('agentsTableBody');

  // Canvas & DAG Elements
  const dagCanvasViewport = document.getElementById('dagCanvasViewport');
  const dagTransformContainer = document.getElementById('dagTransformContainer');
  const dagSvgLayer = document.getElementById('dagSvgLayer');
  const dagEdgesGroup = document.getElementById('dagEdgesGroup');
  const dagNodesLayer = document.getElementById('dagNodesLayer');
  const filterGroup = document.getElementById('filterGroup');
  const filterState = document.getElementById('filterState');
  const btnFitCanvas = document.getElementById('btnFitCanvas');
  const btnZoomIn = document.getElementById('btnZoomIn');
  const btnZoomOut = document.getElementById('btnZoomOut');
  const btnZoomReset = document.getElementById('btnZoomReset');

  // Dependency Inspector Drawer
  const depInspectorDrawer = document.getElementById('depInspectorDrawer');
  const btnCloseInspector = document.getElementById('btnCloseInspector');
  const inspectorBadge = document.getElementById('inspectorBadge');
  const inspectorJobName = document.getElementById('inspectorJobName');
  const inspectorDefID = document.getElementById('inspectorDefID');
  const inspectorRunSelect = document.getElementById('inspectorRunSelect');
  const inspectorGroup = document.getElementById('inspectorGroup');
  const inspectorCron = document.getElementById('inspectorCron');
  const inspectorAgent = document.getElementById('inspectorAgent');
  const inspectorExitCode = document.getElementById('inspectorExitCode');
  const inspectorCommand = document.getElementById('inspectorCommand');
  const inCondCounter = document.getElementById('inCondCounter');
  const inspectorInCondList = document.getElementById('inspectorInCondList');
  const outCondCounter = document.getElementById('outCondCounter');
  const inspectorOutCondList = document.getElementById('inspectorOutCondList');
  const downstreamJobsCounter = document.getElementById('downstreamJobsCounter');
  const inspectorDownstreamJobsList = document.getElementById('inspectorDownstreamJobsList');
  const btnInspectorTrigger = document.getElementById('btnInspectorTrigger');
  const btnInspectorRerun = document.getElementById('btnInspectorRerun');
  const btnInspectorSetOK = document.getElementById('btnInspectorSetOK');
  const btnInspectorBypass = document.getElementById('btnInspectorBypass');
  const btnInspectorEdit = document.getElementById('btnInspectorEdit');
  const btnInspectorLog = document.getElementById('btnInspectorLog');

  // Modals
  const logModal = document.getElementById('logModal');
  const btnCloseLogModal = document.getElementById('btnCloseLogModal');
  const logTerminal = document.getElementById('logTerminal');
  const logModalTitle = document.getElementById('logModalTitle');

  const actionModal = document.getElementById('actionModal');
  const btnCloseActionModal = document.getElementById('btnCloseActionModal');
  const btnCancelAction = document.getElementById('btnCancelAction');
  const btnConfirmAction = document.getElementById('btnConfirmAction');
  const actionModalTitle = document.getElementById('actionModalTitle');
  const actionModalDesc = document.getElementById('actionModalDesc');
  const actionTargetRunID = document.getElementById('actionTargetRunID');
  const actionType = document.getElementById('actionType');
  const operatorIDInput = document.getElementById('operatorIDInput');
  const actionReasonInput = document.getElementById('actionReasonInput');

  // Global State
  let activeLogWs = null;
  let allRuns = [];
  let allDefs = [];
  let allConditions = [];
  let allAudits = [];
  let allAgents = [];
  let activeConditionsSet = new Set();
  let selectedNode = null;
  let currentGroupFilter = '';
  let currentStateFilter = '';
  let currentSearchKeyword = '';
  let activeTabId = 'containerRuns';

  // DAG Pan & Zoom State
  let dagTransform = { scale: 1, x: 24, y: 24 };
  let isDraggingCanvas = false;
  let dragStart = { x: 0, y: 0 };

  // Local calendar date (toISOString would give the UTC date, i.e. yesterday before 09:00 KST)
  const now = new Date();
  const today = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}`;
  odateInput.value = today;

  // Apply Transform
  function applyTransform() {
    dagTransformContainer.style.transform = `translate(${dagTransform.x}px, ${dagTransform.y}px) scale(${dagTransform.scale})`;
  }
  applyTransform();

  // Split an args field into arguments: whitespace separates, "double quotes" group.
  function parseArgs(raw) {
    const match = raw.trim().match(/(?:[^\s"]+|"[^"]*")+/g);
    return match ? match.map(m => m.replace(/^"|"$/g, '')) : [];
  }

  // Inverse of parseArgs for display: arguments containing whitespace are wrapped in double quotes.
  function formatArgs(args) {
    return (args || []).map(a => (/\s/.test(a) || a === '' ? `"${a}"` : a)).join(' ');
  }

  // Load Dashboard Data (Runs, DAG, Conditions)
  async function loadDashboard() {
    const odate = odateInput.value.trim() || today;

    // 1. Fetch Agents
    try {
      const agentRes = await fetch('/api/v1/agents');
      const agentData = await agentRes.json();
      const count = agentData.active_agent_count || (agentData.agents ? agentData.agents.length : 0);
      agentCountText.textContent = `Agents: ${count} Active`;
      if (tabBadgeAgents) tabBadgeAgents.textContent = count;
    } catch (e) {
      agentCountText.textContent = 'Agents: Offline';
    }

    // 2. Fetch Job Definitions, Runs, and Conditions in parallel
    try {
      const [defsRes, runsRes, condsRes] = await Promise.all([
        fetch('/api/v1/jobs'),
        fetch(`/api/v1/runs?date=${odate}`),
        fetch(`/api/v1/conditions?date=${odate}`)
      ]);

      allDefs = (await defsRes.json()) || [];
      allRuns = (await runsRes.json()) || [];
      allConditions = (await condsRes.json()) || [];
      activeConditionsSet = new Set(allConditions.map(c => c.name));

      if (tabBadgeRuns) tabBadgeRuns.textContent = allRuns.length;
      if (tabBadgeDefs) tabBadgeDefs.textContent = allDefs.length;
      if (tabBadgeConds) tabBadgeConds.textContent = allConditions.length;

      updateGroupFilterOptions(allDefs);
      renderMetrics(allRuns);
      renderFilteredTable();
      renderDAG();

      // Refresh inspector if open
      if (selectedNode) {
        const freshRun = allRuns.find(r => (r.job_def_id || r.run_id) === selectedNode.id || r.job_name === selectedNode.name);
        const freshDef = allDefs.find(d => d.id === selectedNode.def.id);
        if (freshRun || freshDef) {
          selectedNode.run = freshRun || selectedNode.run;
          selectedNode.def = freshDef || selectedNode.def;
          openInspector(selectedNode, false);
        }
      }
    } catch (e) {
      runsTableBody.innerHTML = `<tr><td colspan="6" class="loading-cell">데이터를 불러올 수 없습니다: ${e.message}</td></tr>`;
    }
  }

  function updateGroupFilterOptions(defs) {
    const groups = new Set();
    defs.forEach(d => {
      if (d.group) groups.add(d.group);
    });

    const currentVal = filterGroup.value;
    const existingOptions = Array.from(filterGroup.options).map(o => o.value);
    
    groups.forEach(g => {
      if (!existingOptions.includes(g)) {
        const opt = document.createElement('option');
        opt.value = g;
        opt.textContent = g;
        filterGroup.appendChild(opt);
      }
    });

    if (currentVal && groups.has(currentVal)) {
      filterGroup.value = currentVal;
    }
  }

  function renderMetrics(runs) {
    let total = runs.length;
    let success = 0, running = 0, ready = 0, wait = 0, failed = 0, bypass = 0;

    runs.forEach(r => {
      switch (r.state) {
        case 'SUCCESS': success++; break;
        case 'RUNNING': running++; break;
        case 'READY':
        case 'ASSIGNED': ready++; break;
        case 'WAIT': wait++; break;
        case 'FAILED': failed++; break;
        case 'BYPASS': bypass++; break;
      }
    });

    const elTotal = document.getElementById('statTotal');
    const elSuccess = document.getElementById('statSuccess');
    const elRunning = document.getElementById('statRunning');
    const elReady = document.getElementById('statReady');
    const elWait = document.getElementById('statWait');
    const elFailed = document.getElementById('statFailed');
    const elBypass = document.getElementById('statBypass');

    if (elTotal) elTotal.textContent = total;
    if (elSuccess) elSuccess.textContent = success;
    if (elRunning) elRunning.textContent = running;
    if (elReady) elReady.textContent = ready;
    if (elWait) elWait.textContent = wait;
    if (elFailed) elFailed.textContent = failed;
    if (elBypass) elBypass.textContent = bypass;
  }

  function renderFilteredTable() {
    const prevScroll = containerRuns ? containerRuns.scrollTop : 0;

    let filtered = allRuns;
    if (currentStateFilter) {
      if (currentStateFilter === 'READY') {
        filtered = filtered.filter(r => r.state === 'READY' || r.state === 'ASSIGNED');
      } else {
        filtered = filtered.filter(r => r.state === currentStateFilter);
      }
    }
    if (currentGroupFilter) {
      const groupDefs = new Set(allDefs.filter(d => d.group === currentGroupFilter).map(d => d.id));
      filtered = filtered.filter(r => groupDefs.has(r.job_def_id));
    }
    if (currentSearchKeyword) {
      filtered = filtered.filter(r => {
        const name = (r.job_name || '').toLowerCase();
        const id = (r.job_def_id || '').toLowerCase();
        const run = (r.run_id || '').toLowerCase();
        return name.includes(currentSearchKeyword) || id.includes(currentSearchKeyword) || run.includes(currentSearchKeyword);
      });
    }

    renderTable(filtered);
    if (containerRuns) containerRuns.scrollTop = prevScroll;
  }

  function renderTable(runs) {
    if (runs.length === 0) {
      runsTableBody.innerHTML = `<tr><td colspan="6" class="loading-cell">금일 등록된 배치 실행 이력이 없습니다.</td></tr>`;
      return;
    }

    runsTableBody.innerHTML = runs.map(r => {
      const exitCodeStr = r.exit_code !== undefined ? r.exit_code : '-';
      const scheduledStr = r.scheduled_at ? r.scheduled_at.slice(11, 19) : '-';

      return `
        <tr>
          <td><span class="node-badge ${r.state}">${r.state}</span></td>
          <td><strong style="cursor: pointer; color: #38bdf8;" class="link-job-detail" data-defid="${r.job_def_id}" title="클릭하여 작업 상세 및 수정">${r.job_name}</strong></td>
          <td><code>${r.run_id}</code></td>
          <td><code>${exitCodeStr}</code></td>
          <td>${scheduledStr}</td>
          <td>
            <button class="btn-action btn-log" data-id="${r.run_id}" data-name="${r.job_name}">Log</button>
            <button class="btn-action btn-detail" data-defid="${r.job_def_id}">수정</button>
            <button class="btn-action btn-rerun" data-id="${r.run_id}">Rerun</button>
            ${r.state !== 'SUCCESS' ? `<button class="btn-action btn-setok" data-id="${r.run_id}">Set OK</button>` : ''}
            ${r.state === 'WAIT' || r.state === 'READY' ? `<button class="btn-action btn-bypass" data-id="${r.run_id}">Bypass</button>` : ''}
          </td>
        </tr>
      `;
    }).join('');

    // Attach button events
    document.querySelectorAll('.btn-log').forEach(b => b.addEventListener('click', () => openLogModal(b.dataset.id, b.dataset.name)));
    document.querySelectorAll('.btn-detail').forEach(b => b.addEventListener('click', () => openJobDetailModal(b.dataset.defid)));
    document.querySelectorAll('.link-job-detail').forEach(b => b.addEventListener('click', () => openJobDetailModal(b.dataset.defid)));
    document.querySelectorAll('.btn-rerun').forEach(b => b.addEventListener('click', () => openActionModal('RERUN', b.dataset.id)));
    document.querySelectorAll('.btn-setok').forEach(b => b.addEventListener('click', () => openActionModal('SET_OK', b.dataset.id)));
    document.querySelectorAll('.btn-bypass').forEach(b => b.addEventListener('click', () => openActionModal('BYPASS', b.dataset.id)));
  }

  // ==========================================
  // TOPOLOGICAL DAG RENDERER
  // ==========================================
  function renderDAG() {
    dagEdgesGroup.innerHTML = '';
    dagNodesLayer.innerHTML = '';

    // 1. Build definition index
    const defsById = new Map();
    const defsByName = new Map();
    const producerDefsByCond = new Map(); // condName -> [def]

    allDefs.forEach(def => {
      defsById.set(def.id, def);
      defsByName.set(def.name, def);
      (def.out_conditions || []).forEach(cond => {
        if (!producerDefsByCond.has(cond)) {
          producerDefsByCond.set(cond, []);
        }
        producerDefsByCond.get(cond).push(def);
      });
    });

    // 2. Form graph nodes from runs or definitions (allRuns is sorted DESC)
    let graphNodes = [];
    const runMap = new Map();
    allRuns.forEach(r => {
      const keyId = r.job_def_id || r.run_id;
      if (!runMap.has(keyId)) {
        runMap.set(keyId, r);
      }
      if (!runMap.has(r.job_name)) {
        runMap.set(r.job_name, r);
      }
    });

    // Use all defined jobs or today's runs
    allDefs.forEach(def => {
      const run = runMap.get(def.id) || runMap.get(def.name) || {
        run_id: '-',
        job_def_id: def.id,
        job_name: def.name,
        state: def.enabled ? 'UNSCHEDULED' : 'DISABLED', // no run for this ODATE (DISABLED: definition switched off)
        agent_id: '-',
        exit_code: '-'
      };

      // Filter by Group
      if (currentGroupFilter && def.group !== currentGroupFilter) {
        return;
      }
      // Filter by State
      if (currentStateFilter) {
        if (currentStateFilter === 'READY') {
          if (run.state !== 'READY' && run.state !== 'ASSIGNED') return;
        } else if (run.state !== currentStateFilter) {
          return;
        }
      }
      // Filter by Search Keyword
      if (currentSearchKeyword) {
        const name = (def.name || '').toLowerCase();
        const id = (def.id || '').toLowerCase();
        const runId = (run.run_id || '').toLowerCase();
        if (!name.includes(currentSearchKeyword) && !id.includes(currentSearchKeyword) && !runId.includes(currentSearchKeyword)) {
          return;
        }
      }

      graphNodes.push({
        id: def.id,
        name: def.name,
        def: def,
        run: run,
        inConditions: def.in_conditions || [],
        outConditions: def.out_conditions || [],
        level: 0,
        x: 0,
        y: 0
      });
    });

    if (graphNodes.length === 0) {
      dagNodesLayer.innerHTML = '<div style="color:#64748b; padding: 2rem; text-align: center;">조건에 일치하는 워크플로우 작업이 없습니다.</div>';
      return;
    }

    const nodeById = new Map();
    const nodeByName = new Map();
    graphNodes.forEach(n => {
      nodeById.set(n.id, n);
      nodeByName.set(n.name, n);
    });

    // 3. Build directed edges
    const edges = [];
    graphNodes.forEach(targetNode => {
      targetNode.inConditions.forEach(condName => {
        // Direct matching: producer def has outCond
        let producers = producerDefsByCond.get(condName) || [];
        // If condition name is directly the name of a job
        if (producers.length === 0 && defsByName.has(condName)) {
          producers = [defsByName.get(condName)];
        }

        producers.forEach(pDef => {
          const sourceNode = nodeById.get(pDef.id);
          if (sourceNode && sourceNode.id !== targetNode.id) {
            const isSatisfied = activeConditionsSet.has(condName) || sourceNode.run.state === 'SUCCESS';
            edges.push({
              source: sourceNode,
              target: targetNode,
              condition: condName,
              satisfied: isSatisfied
            });
          }
        });
      });
    });

    // 4. Compute Topological Levels (Kahn's / Longest Path)
    // In-degree from current visible edges
    const incomingEdges = new Map();
    const outgoingEdges = new Map();
    graphNodes.forEach(n => {
      incomingEdges.set(n.id, []);
      outgoingEdges.set(n.id, []);
    });

    edges.forEach(e => {
      incomingEdges.get(e.target.id).push(e);
      outgoingEdges.get(e.source.id).push(e);
    });

    // Compute longest path from roots
    let changed = true;
    let iteration = 0;
    const maxIterations = graphNodes.length + 2;

    while (changed && iteration < maxIterations) {
      changed = false;
      iteration++;
      edges.forEach(e => {
        if (e.target.level < e.source.level + 1) {
          e.target.level = e.source.level + 1;
          changed = true;
        }
      });
    }

    // 5. Calculate (x, y) Coordinates
    // Group by level
    const columns = [];
    graphNodes.forEach(n => {
      const lvl = n.level;
      if (!columns[lvl]) columns[lvl] = [];
      columns[lvl].push(n);
    });

    const nodeWidth = 210;
    const nodeHeight = 86;
    const colSpacing = 140;
    const rowSpacing = 36;
    const padX = 40;
    const padY = 40;

    let maxCanvasWidth = 700;
    let maxCanvasHeight = 600;

    columns.forEach((colNodes, lvl) => {
      if (!colNodes) return;
      colNodes.forEach((n, idx) => {
        n.x = padX + lvl * (nodeWidth + colSpacing);
        n.y = padY + idx * (nodeHeight + rowSpacing);

        if (n.x + nodeWidth + 120 > maxCanvasWidth) {
          maxCanvasWidth = n.x + nodeWidth + 120;
        }
        if (n.y + nodeHeight + 120 > maxCanvasHeight) {
          maxCanvasHeight = n.y + nodeHeight + 120;
        }
      });
    });

    dagSvgLayer.setAttribute('width', maxCanvasWidth);
    dagSvgLayer.setAttribute('height', maxCanvasHeight);
    dagTransformContainer.style.width = `${maxCanvasWidth}px`;
    dagTransformContainer.style.height = `${maxCanvasHeight}px`;

    // 6. Render SVG Directed Edges
    let edgesSvgHtml = '';
    edges.forEach(e => {
      const x1 = e.source.x + nodeWidth;
      const y1 = e.source.y + nodeHeight / 2;
      const x2 = e.target.x;
      const y2 = e.target.y + nodeHeight / 2;

      const dx = Math.max(40, (x2 - x1) * 0.45);
      const cx1 = x1 + dx;
      const cy1 = y1;
      const cx2 = x2 - dx;
      const cy2 = y2;

      const d = `M ${x1} ${y1} C ${cx1} ${cy1}, ${cx2} ${cy2}, ${x2} ${y2}`;
      const markerType = e.satisfied ? 'arrow-satisfied' : 'arrow-pending';
      const edgeClass = e.satisfied ? 'edge-satisfied' : 'edge-pending';

      edgesSvgHtml += `
        <path fill="none"
              class="dag-edge ${edgeClass}" 
              d="${d}" 
              marker-end="url(#${markerType})"
              data-source="${e.source.id}"
              data-target="${e.target.id}"
              data-condition="${e.condition}"
              data-satisfied="${e.satisfied}">
          <title>선후행 연결: ${e.source.name} ➡ ${e.target.name}\n조건: ${e.condition} (${e.satisfied ? '충족됨' : '미충족 대기중'})</title>
        </path>
      `;
    });
    dagEdgesGroup.innerHTML = edgesSvgHtml;

    // 7. Render HTML Node Cards
    let nodesHtml = '';
    graphNodes.forEach(node => {
      const r = node.run;
      const inCount = node.inConditions.length;
      let satisfiedInCount = 0;
      node.inConditions.forEach(c => {
        if (activeConditionsSet.has(c)) satisfiedInCount++;
      });

      const inPortClass = inCount === 0 ? '' : (satisfiedInCount === inCount ? 'satisfied' : 'pending');
      const inPortTitle = inCount === 0 ? '선행 조건 없음 (루트 작업)' : `선행 조건 ${satisfiedInCount}/${inCount} 충족`;

      const exitStr = r.exit_code !== undefined && r.exit_code !== '-' ? `Exit: ${r.exit_code}` : (r.agent_id ? `Agent: ${r.agent_id}` : 'Unassigned');

      nodesHtml += `
        <div class="dag-node state-${r.state}" id="node-${node.id}" data-id="${node.id}" style="left: ${node.x}px; top: ${node.y}px;">
          ${inCount > 0 ? `<div class="node-port port-in ${inPortClass}" title="${inPortTitle}"></div>` : ''}
          ${node.outConditions.length > 0 ? `<div class="node-port port-out" title="완료 시 ${node.outConditions.length}개 후행 조건 발행"></div>` : ''}

          <div class="node-header">
            <span class="node-title" title="${node.name}">${node.name}</span>
            <span class="node-badge ${r.state}">${r.state}</span>
          </div>

          <div class="node-details">
            <span>RunID: ${r.run_id && r.run_id !== '-' ? r.run_id.slice(-14) : (r.state === 'DISABLED' ? '비활성' : '미발주')}</span>
            <span>${exitStr}</span>
          </div>

          <div class="node-cond-summary">
            <span class="cond-pill ${inCount > 0 ? (satisfiedInCount === inCount ? 'in-ok' : 'in-wait') : ''}">
              ${inCount > 0 ? `In: ${satisfiedInCount}/${inCount}` : 'Root Job'}
            </span>
            <span class="cond-pill ${node.outConditions.length > 0 ? 'out-ready' : ''}">
              ${node.outConditions.length > 0 ? `Out: ${node.outConditions.length}` : 'Leaf'}
            </span>
          </div>
        </div>
      `;
    });
    dagNodesLayer.innerHTML = nodesHtml;

    // 8. Attach Node Events (Trace & Highlight, Click to Inspect)
    graphNodes.forEach(node => {
      const el = document.getElementById(`node-${node.id}`);
      if (!el) return;

      // Click: Open Inspector Drawer & Highlight dependencies
      el.addEventListener('click', (e) => {
        e.stopPropagation();
        selectNode(node, incomingEdges, outgoingEdges);
      });

      // Double-click: Directly open full Job Definition & Edit Modal
      el.addEventListener('dblclick', (e) => {
        e.stopPropagation();
        openJobDetailModal(node.def.id);
      });

      // Hover: Highlight Upstream & Downstream paths
      el.addEventListener('mouseenter', () => {
        if (!selectedNode) {
          highlightDependencies(node.id, incomingEdges, outgoingEdges);
        }
      });

      el.addEventListener('mouseleave', () => {
        if (!selectedNode) {
          clearHighlights();
        }
      });
    });

    // Click on canvas background deselects and closes drawer
    dagCanvasViewport.addEventListener('click', () => {
      selectedNode = null;
      clearHighlights();
      depInspectorDrawer.classList.remove('active');
      depInspectorDrawer.classList.remove('open');
    });
  }

  // ==========================================
  // DEPENDENCY TRACE & HIGHLIGHT
  // ==========================================
  function highlightDependencies(targetNodeId, incomingEdges, outgoingEdges) {
    // 1. Traverse Ancestors (Upstream)
    const upstreamNodeIds = new Set();
    const upstreamEdgeKeys = new Set();

    function findUpstream(currId) {
      const inEdges = incomingEdges.get(currId) || [];
      inEdges.forEach(edge => {
        upstreamEdgeKeys.add(`${edge.source.id}->${edge.target.id}`);
        if (!upstreamNodeIds.has(edge.source.id)) {
          upstreamNodeIds.add(edge.source.id);
          findUpstream(edge.source.id);
        }
      });
    }
    findUpstream(targetNodeId);

    // 2. Traverse Descendants (Downstream)
    const downstreamNodeIds = new Set();
    const downstreamEdgeKeys = new Set();

    function findDownstream(currId) {
      const outEdges = outgoingEdges.get(currId) || [];
      outEdges.forEach(edge => {
        downstreamEdgeKeys.add(`${edge.source.id}->${edge.target.id}`);
        if (!downstreamNodeIds.has(edge.target.id)) {
          downstreamNodeIds.add(edge.target.id);
          findDownstream(edge.target.id);
        }
      });
    }
    findDownstream(targetNodeId);

    // 3. Apply CSS classes to Nodes
    document.querySelectorAll('.dag-node').forEach(nodeEl => {
      const id = nodeEl.dataset.id;
      if (id === targetNodeId) {
        nodeEl.classList.add('highlight-selected');
        nodeEl.classList.remove('node-dimmed', 'highlight-upstream', 'highlight-downstream');
      } else if (upstreamNodeIds.has(id)) {
        nodeEl.classList.add('highlight-upstream');
        nodeEl.classList.remove('node-dimmed', 'highlight-selected', 'highlight-downstream');
      } else if (downstreamNodeIds.has(id)) {
        nodeEl.classList.add('highlight-downstream');
        nodeEl.classList.remove('node-dimmed', 'highlight-selected', 'highlight-upstream');
      } else {
        nodeEl.classList.add('node-dimmed');
        nodeEl.classList.remove('highlight-selected', 'highlight-upstream', 'highlight-downstream');
      }
    });

    // 4. Apply CSS classes to Edges
    document.querySelectorAll('.dag-edge').forEach(edgeEl => {
      const src = edgeEl.dataset.source;
      const tgt = edgeEl.dataset.target;
      const key = `${src}->${tgt}`;

      if (upstreamEdgeKeys.has(key)) {
        edgeEl.classList.add('edge-upstream-highlight');
        edgeEl.classList.remove('edge-dimmed', 'edge-downstream-highlight');
        edgeEl.setAttribute('marker-end', 'url(#arrow-upstream)');
      } else if (downstreamEdgeKeys.has(key)) {
        edgeEl.classList.add('edge-downstream-highlight');
        edgeEl.classList.remove('edge-dimmed', 'edge-upstream-highlight');
        edgeEl.setAttribute('marker-end', 'url(#arrow-downstream)');
      } else {
        edgeEl.classList.add('edge-dimmed');
        edgeEl.classList.remove('edge-upstream-highlight', 'edge-downstream-highlight');
      }
    });
  }

  function clearHighlights() {
    document.querySelectorAll('.dag-node').forEach(n => {
      n.classList.remove('highlight-selected', 'highlight-upstream', 'highlight-downstream', 'node-dimmed');
    });
    document.querySelectorAll('.dag-edge').forEach(e => {
      e.classList.remove('edge-upstream-highlight', 'edge-downstream-highlight', 'edge-dimmed');
      const isSat = e.dataset.satisfied === 'true';
      e.setAttribute('marker-end', `url(#arrow-${isSat ? 'satisfied' : 'pending'})`);
    });
  }

  // ==========================================
  // DEPENDENCY INSPECTOR DRAWER
  // ==========================================
  function selectNode(node, incomingEdges, outgoingEdges) {
    selectedNode = node;
    highlightDependencies(node.id, incomingEdges, outgoingEdges);
    openInspector(node, true);
  }

  function openInspector(node, slideIn) {
    const def = node.def;

    // Find all runs for this job definition (allRuns is sorted DESC, latest first)
    const jobRuns = allRuns.filter(r => (r.job_def_id || r.run_id) === def.id || r.job_name === def.name);
    let currentTargetRun = node.run;
    if (jobRuns.length > 0 && (!currentTargetRun || currentTargetRun.run_id === '-')) {
      currentTargetRun = jobRuns[0];
      node.run = currentTargetRun;
    }

    function updateInspectorRunView(r) {
      if (!r) return;
      inspectorBadge.className = `drawer-badge node-badge ${r.state}`;
      inspectorBadge.textContent = { UNSCHEDULED: '미발주 (UNSCHEDULED)', DISABLED: '비활성 (DISABLED)' }[r.state] || r.state;
      inspectorJobName.textContent = node.name;
      inspectorDefID.textContent = def.id || '-';
      if (inspectorGroup) inspectorGroup.textContent = def.group || 'DEFAULT';
      if (inspectorCron) inspectorCron.textContent = def.cron_expr || '-';
      inspectorAgent.textContent = r.agent_id || 'unassigned';
      inspectorExitCode.textContent = r.exit_code !== undefined && r.exit_code !== -1 ? r.exit_code : '-';
      if (inspectorCommand) {
        const fullCmd = `${def.command || ''} ${(def.args || []).join(' ')}`.trim() || '-';
        inspectorCommand.textContent = fullCmd;
        inspectorCommand.title = fullCmd;
      }

      // Dynamic button action binding to the specifically selected Run
      btnInspectorRerun.onclick = () => {
        if (r.run_id && r.run_id !== '-') {
          openActionModal('RERUN', r.run_id);
        } else {
          triggerJobImmediately(def.id);
        }
      };

      btnInspectorSetOK.onclick = () => {
        if (r.run_id && r.run_id !== '-') {
          openActionModal('SET_OK', r.run_id);
        } else {
          alert('실행 인스턴스가 존재하지 않아 Set OK를 수행할 수 없습니다.');
        }
      };

      btnInspectorBypass.onclick = () => {
        if (r.run_id && r.run_id !== '-') {
          openActionModal('BYPASS', r.run_id);
        } else {
          alert('실행 인스턴스가 존재하지 않습니다.');
        }
      };

      btnInspectorLog.onclick = () => {
        if (r.run_id && r.run_id !== '-') {
          openLogModal(r.run_id, node.name);
        } else {
          alert('실행 이력이 없어 로그를 조회할 수 없습니다.');
        }
      };
    }

    // Populate Run History Dropdown
    if (inspectorRunSelect) {
      if (jobRuns.length === 0) {
        inspectorRunSelect.innerHTML = '<option value="">(실행 이력 없음)</option>';
      } else {
        inspectorRunSelect.innerHTML = jobRuns.map((jr, idx) => {
          const prefix = idx === 0 ? '★최신 ' : `${jobRuns.length - idx}차: `;
          const shortId = jr.run_id.length > 15 ? '...' + jr.run_id.slice(-12) : jr.run_id;
          return `<option value="${jr.run_id}">${prefix}[${jr.state}] ${shortId}</option>`;
        }).join('');

        if (currentTargetRun && currentTargetRun.run_id && currentTargetRun.run_id !== '-') {
          inspectorRunSelect.value = currentTargetRun.run_id;
        }

        inspectorRunSelect.onchange = () => {
          const selId = inspectorRunSelect.value;
          const matched = jobRuns.find(jr => jr.run_id === selId);
          if (matched) {
            currentTargetRun = matched;
            node.run = matched;
            updateInspectorRunView(matched);
          }
        };
      }
    }

    updateInspectorRunView(currentTargetRun || node.run);

    // 1. In-Conditions List
    const inConds = def.in_conditions || [];
    inCondCounter.textContent = inConds.length;
    if (inConds.length === 0) {
      inspectorInCondList.innerHTML = '<div style="color: #64748b; font-size: 0.75rem;">선행 조건이 없습니다. (루트 시작 작업)</div>';
    } else {
      inspectorInCondList.innerHTML = inConds.map(cond => {
        const isSat = activeConditionsSet.has(cond);
        return `
          <div class="inspector-item">
            <div class="item-left">
              <span class="item-title">${cond}</span>
              <span class="item-sub">필요 조건명</span>
            </div>
            <div style="display: flex; align-items: center;">
              <span class="${isSat ? 'cond-badge-satisfied' : 'cond-badge-pending'}">
                ${isSat ? '✓ 충족됨' : '⏳ 미충족'}
              </span>
              ${!isSat ? `<button class="btn-mini-emit" data-cond="${cond}" title="관리자 권한으로 이 조건을 즉시 충족(발행)시킵니다">+ 수동충족</button>` : ''}
            </div>
          </div>
        `;
      }).join('');

      inspectorInCondList.querySelectorAll('.btn-mini-emit').forEach(btn => {
        btn.addEventListener('click', async (e) => {
          e.stopPropagation();
          const condName = btn.dataset.cond;
          const odate = odateInput.value.trim() || today;
          try {
            const res = await fetch('/api/v1/conditions', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ name: condName, odate: odate })
            });
            if (!res.ok) throw new Error(await res.text());
            await loadDashboard();
          } catch (err) {
            alert(`조건 수동 발행 실패: ${err.message}`);
          }
        });
      });
    }

    // 2. Out-Conditions List
    const outConds = def.out_conditions || [];
    outCondCounter.textContent = outConds.length;
    if (outConds.length === 0) {
      inspectorOutCondList.innerHTML = '<div style="color: #64748b; font-size: 0.75rem;">발행하는 후행 조건이 없습니다.</div>';
    } else {
      inspectorOutCondList.innerHTML = outConds.map(cond => {
        const isEmitted = activeConditionsSet.has(cond);
        return `
          <div class="inspector-item">
            <div class="item-left">
              <span class="item-title">${cond}</span>
              <span class="item-sub">완료 시 자동 발행</span>
            </div>
            <div>
              <span class="${isEmitted ? 'cond-badge-satisfied' : 'cond-badge-pending'}">
                ${isEmitted ? '✓ 발행 완료' : '대기중'}
              </span>
            </div>
          </div>
        `;
      }).join('');
    }

    // 3. Dependent Downstream Jobs
    const dependentJobs = [];
    allDefs.forEach(otherDef => {
      if (otherDef.id === def.id) return;
      const inList = otherDef.in_conditions || [];
      const hasDependency = outConds.some(oc => inList.includes(oc)) || inList.includes(def.name);
      if (hasDependency) {
        const otherRun = allRuns.find(r => r.job_def_id === otherDef.id || r.job_name === otherDef.name);
        dependentJobs.push({
          def: otherDef,
          run: otherRun
        });
      }
    });

    downstreamJobsCounter.textContent = dependentJobs.length;
    if (dependentJobs.length === 0) {
      inspectorDownstreamJobsList.innerHTML = '<div style="color: #64748b; font-size: 0.75rem;">이 작업에 의존하는 후행 작업이 없습니다.</div>';
    } else {
      inspectorDownstreamJobsList.innerHTML = dependentJobs.map(d => {
        const dState = d.run ? d.run.state : (d.def.enabled ? 'UNSCHEDULED' : 'DISABLED');
        return `
          <div class="inspector-item" style="cursor: pointer;" data-defid="${d.def.id}">
            <div class="item-left">
              <span class="item-title">${d.def.name}</span>
              <span class="item-sub">Group: ${d.def.group || 'DEFAULT'}</span>
            </div>
            <div>
              <span class="node-badge ${dState}">${dState}</span>
            </div>
          </div>
        `;
      }).join('');

      inspectorDownstreamJobsList.querySelectorAll('.inspector-item').forEach(item => {
        item.addEventListener('click', () => {
          const targetDefId = item.dataset.defid;
          const targetNode = allDefs.find(d => d.id === targetDefId);
          if (targetNode) {
            const targetEl = document.getElementById(`node-${targetDefId}`);
            if (targetEl) targetEl.click();
          }
        });
      });
    }

    // 4. Action Buttons Binding (Trigger and Edit def-level actions)
    btnInspectorTrigger.onclick = () => {
      triggerJobImmediately(def.id);
    };

    btnInspectorEdit.onclick = () => {
      openJobDetailModal(def.id);
    };

    if (slideIn) {
      depInspectorDrawer.classList.add('active');
      depInspectorDrawer.classList.add('open');
    }
  }

  function closeInspector() {
    depInspectorDrawer.classList.remove('active');
    depInspectorDrawer.classList.remove('open');
    selectedNode = null;
    clearHighlights();
  }

  btnCloseInspector.addEventListener('click', closeInspector);

  // ==========================================
  // CANVAS PAN & ZOOM & FIT ENGINE
  // ==========================================
  dagCanvasViewport.addEventListener('mousedown', (e) => {
    // Only drag on empty viewport space
    if (e.target.closest('.dag-node') || e.target.closest('.dependency-inspector-drawer')) {
      return;
    }
    isDraggingCanvas = true;
    dragStart = {
      x: e.clientX - dagTransform.x,
      y: e.clientY - dagTransform.y
    };
    dagCanvasViewport.classList.add('is-dragging');
  });

  window.addEventListener('mousemove', (e) => {
    if (!isDraggingCanvas) return;
    dagTransform.x = e.clientX - dragStart.x;
    dagTransform.y = e.clientY - dragStart.y;
    applyTransform();
  });

  window.addEventListener('mouseup', () => {
    if (isDraggingCanvas) {
      isDraggingCanvas = false;
      dagCanvasViewport.classList.remove('is-dragging');
    }
  });

  dagCanvasViewport.addEventListener('wheel', (e) => {
    e.preventDefault();
    const zoomFactor = e.deltaY < 0 ? 1.1 : 0.9;
    const newScale = Math.min(2.5, Math.max(0.3, dagTransform.scale * zoomFactor));

    // Zoom towards mouse pointer
    const rect = dagCanvasViewport.getBoundingClientRect();
    const mouseX = e.clientX - rect.left;
    const mouseY = e.clientY - rect.top;

    dagTransform.x = mouseX - (mouseX - dagTransform.x) * (newScale / dagTransform.scale);
    dagTransform.y = mouseY - (mouseY - dagTransform.y) * (newScale / dagTransform.scale);
    dagTransform.scale = newScale;

    applyTransform();
  }, { passive: false });

  btnZoomIn.addEventListener('click', () => {
    dagTransform.scale = Math.min(2.5, dagTransform.scale + 0.15);
    applyTransform();
  });

  btnZoomOut.addEventListener('click', () => {
    dagTransform.scale = Math.max(0.3, dagTransform.scale - 0.15);
    applyTransform();
  });

  btnZoomReset.addEventListener('click', () => {
    dagTransform = { scale: 1, x: 24, y: 24 };
    applyTransform();
  });

  btnFitCanvas.addEventListener('click', () => {
    const nodes = document.querySelectorAll('.dag-node');
    if (nodes.length === 0) return;

    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    nodes.forEach(n => {
      const left = parseFloat(n.style.left);
      const top = parseFloat(n.style.top);
      if (left < minX) minX = left;
      if (top < minY) minY = top;
      if (left + 240 > maxX) maxX = left + 240;
      if (top + 90 > maxY) maxY = top + 90;
    });

    const graphW = maxX - minX;
    const graphH = maxY - minY;
    const vpW = dagCanvasViewport.clientWidth - 60;
    const vpH = dagCanvasViewport.clientHeight - 60;

    const scaleX = vpW / graphW;
    const scaleY = vpH / graphH;
    const bestScale = Math.min(1.2, Math.max(0.4, Math.min(scaleX, scaleY)));

    dagTransform.scale = bestScale;
    dagTransform.x = (dagCanvasViewport.clientWidth - graphW * bestScale) / 2 - minX * bestScale;
    dagTransform.y = (dagCanvasViewport.clientHeight - graphH * bestScale) / 2 - minY * bestScale;
    applyTransform();
  });

  // Filter Event Listeners
  filterGroup.addEventListener('change', () => {
    currentGroupFilter = filterGroup.value;
    renderFilteredTable();
    renderDAG();
  });

  function setStateFilter(state) {
    currentStateFilter = state;
    if (filterState) filterState.value = state;

    ribbonChips.forEach(chip => {
      chip.classList.toggle('active', (chip.dataset.filter || '') === state);
    });

    if (state) {
      if (ribbonFilterInfo) ribbonFilterInfo.style.display = 'flex';
      if (activeFilterText) activeFilterText.textContent = `필터: ${state}`;
    } else {
      if (ribbonFilterInfo) ribbonFilterInfo.style.display = 'none';
    }

    renderFilteredTable();
    renderDAG();
  }

  filterState.addEventListener('change', () => {
    setStateFilter(filterState.value);
  });

  ribbonChips.forEach(chip => {
    chip.addEventListener('click', () => {
      const filter = chip.dataset.filter !== undefined ? chip.dataset.filter : '';
      setStateFilter(filter);
    });
  });

  if (btnClearStateFilter) {
    btnClearStateFilter.addEventListener('click', () => {
      setStateFilter('');
    });
  }

  // Global Inline Instant Search
  if (globalSearch) {
    globalSearch.addEventListener('input', (e) => {
      currentSearchKeyword = e.target.value.trim().toLowerCase();
      renderFilteredTable();
      renderDAG();
    });
  }

  // 3-Way Workspace Layout Switcher
  function setLayoutMode(mode) {
    if (!mainWorkspace) return;
    mainWorkspace.classList.remove('view-full-dag', 'view-split', 'view-full-table');
    mainWorkspace.classList.add(mode);

    if (btnViewFullDag) btnViewFullDag.classList.toggle('active', mode === 'view-full-dag');
    if (btnViewSplit) btnViewSplit.classList.toggle('active', mode === 'view-split');
    if (btnViewFullTable) btnViewFullTable.classList.toggle('active', mode === 'view-full-table');
  }

  if (btnViewFullDag) btnViewFullDag.addEventListener('click', () => setLayoutMode('view-full-dag'));
  if (btnViewSplit) btnViewSplit.addEventListener('click', () => setLayoutMode('view-split'));
  if (btnViewFullTable) btnViewFullTable.addEventListener('click', () => setLayoutMode('view-full-table'));

  // Global Keyboard Shortcuts
  window.addEventListener('keydown', (e) => {
    const tag = (e.target.tagName || '').toUpperCase();
    const isInput = tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';

    if (e.key === 'Escape') {
      if (isInput) {
        e.target.blur();
        if (e.target === globalSearch && globalSearch.value) {
          globalSearch.value = '';
          currentSearchKeyword = '';
          renderFilteredTable();
          renderDAG();
        }
        return;
      }
      if (jobDetailModal && jobDetailModal.classList.contains('active')) closeJobDetailModal();
      if (logModal && logModal.classList.contains('active')) closeLogModal();
      if (actionModal && actionModal.classList.contains('active')) closeActionModal();
      if (newJobModal && newJobModal.classList.contains('active')) closeNewJobModal();
      if (depInspectorDrawer && (depInspectorDrawer.classList.contains('open') || depInspectorDrawer.classList.contains('active'))) closeInspector();
      return;
    }

    if (isInput) return;

    if (e.key === '/') {
      e.preventDefault();
      if (globalSearch) {
        globalSearch.focus();
        globalSearch.select();
      }
    } else if (e.key === 'f' || e.key === 'F') {
      e.preventDefault();
      if (mainWorkspace && mainWorkspace.classList.contains('view-full-dag')) {
        setLayoutMode('view-split');
      } else {
        setLayoutMode('view-full-dag');
      }
    } else if (e.key === 't' || e.key === 'T') {
      e.preventDefault();
      if (mainWorkspace && mainWorkspace.classList.contains('view-full-table')) {
        setLayoutMode('view-split');
      } else {
        setLayoutMode('view-full-table');
      }
    } else if (e.key === 'r' || e.key === 'R') {
      e.preventDefault();
      refreshActiveTab();
    }
  });

  // ==========================================
  // LOG MODAL
  // ==========================================
  async function openLogModal(runID, jobName) {
    logModalTitle.textContent = `[${jobName}] Sysout Log (${runID})`;
    logTerminal.innerHTML = '<div class="terminal-loader">로그 로딩 중...</div>';
    logModal.classList.add('active');

    if (activeLogWs) {
      activeLogWs.close();
      activeLogWs = null;
    }

    try {
      const res = await fetch(`/api/v1/runs/logs?run_id=${runID}`);
      const data = await res.json();
      logTerminal.innerHTML = '';

      const headerLine = document.createElement('div');
      headerLine.className = 'terminal-line';
      headerLine.style.color = '#94a3b8';
      headerLine.style.marginBottom = '8px';
      headerLine.style.borderBottom = '1px solid #1e293b';
      headerLine.style.paddingBottom = '4px';
      headerLine.textContent = `>>> [FreeJobScheduler Sysout] Job: ${jobName} | RunID: ${runID} | State: ${data.state} | ExitCode: ${data.exit_code !== undefined ? data.exit_code : '-'}`;
      logTerminal.appendChild(headerLine);

      // Failure reason from the agent (timeout, exit status) or the operator's Set OK / Bypass note
      if (data.error_message) {
        const reasonLine = document.createElement('div');
        reasonLine.className = 'terminal-line';
        reasonLine.style.color = data.state === 'FAILED' ? '#f87171' : '#c084fc';
        reasonLine.style.marginBottom = '8px';
        reasonLine.textContent = `사유: ${data.error_message}`;
        logTerminal.appendChild(reasonLine);
      }

      if (data.command) {
        const cmdLine = document.createElement('div');
        cmdLine.className = 'terminal-line';
        cmdLine.style.color = '#e2e8f0';
        cmdLine.style.marginBottom = '8px';
        cmdLine.textContent = `$ ${data.command} ${(data.args || []).join(' ')}`;
        logTerminal.appendChild(cmdLine);
      }

      if (data.logs && data.logs.length > 0) {
        data.logs.forEach(chunk => {
          const line = document.createElement('div');
          line.className = `terminal-line ${chunk.stream === 'STDERR' ? 'stderr' : ''}`;
          line.textContent = `[${chunk.stream}][${chunk.sequence}] ${chunk.content}`;
          logTerminal.appendChild(line);
        });
      } else if (data.state === 'SUCCESS' || data.state === 'FAILED') {
        const noLog = document.createElement('div');
        noLog.className = 'terminal-line';
        noLog.style.color = '#94a3b8';
        noLog.textContent = '(표준 출력 내용 없음)';
        logTerminal.appendChild(noLog);
      } else {
        const waiting = document.createElement('div');
        waiting.className = 'terminal-loader';
        waiting.textContent = '실시간 로그 수신 대기 중...';
        logTerminal.appendChild(waiting);
      }

      if (data.state === 'SUCCESS' || data.state === 'FAILED') {
        const finLine = document.createElement('div');
        finLine.className = 'terminal-line';
        finLine.style.color = data.state === 'SUCCESS' ? '#10b981' : '#ef4444';
        finLine.style.marginTop = '8px';
        finLine.textContent = `\n--- [COMPLETED] 작업이 종료되었습니다 (State: ${data.state}, ExitCode: ${data.exit_code}) ---`;
        logTerminal.appendChild(finLine);
        logTerminal.scrollTop = logTerminal.scrollHeight;
        return;
      }
    } catch (e) {
      console.error('Failed to load historical logs:', e);
    }

    const loc = window.location;
    const wsProto = loc.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProto}//${loc.host}/ws/logs?run_id=${runID}`;

    activeLogWs = new WebSocket(wsUrl);
    activeLogWs.onmessage = (event) => {
      const loader = logTerminal.querySelector('.terminal-loader');
      if (loader) loader.remove();

      try {
        const chunk = JSON.parse(event.data);
        const line = document.createElement('div');
        line.className = `terminal-line ${chunk.stream === 'STDERR' ? 'stderr' : ''}`;
        line.textContent = `[${chunk.stream}][${chunk.sequence}] ${chunk.content}`;
        logTerminal.appendChild(line);
        logTerminal.scrollTop = logTerminal.scrollHeight;
      } catch (e) {
        console.error('Failed to parse log chunk:', e);
      }
    };
  }

  btnCloseLogModal.addEventListener('click', () => {
    logModal.classList.remove('active');
    if (activeLogWs) {
      activeLogWs.close();
      activeLogWs = null;
    }
  });

  // ==========================================
  // ACTION MODAL (RERUN / SET OK / BYPASS)
  // ==========================================
  function openActionModal(type, runID) {
    actionType.value = type;
    actionTargetRunID.value = runID;
    operatorIDInput.value = '';
    actionReasonInput.value = '';

    actionModalTitle.textContent = `긴급 조치: ${type}`;
    actionModalDesc.innerHTML = `대상 실행 인스턴스: <code>${runID}</code><br/>이 조치는 감사 기록(Audit Trail)에 영구 저장됩니다.`;
    actionModal.classList.add('active');
  }

  btnCloseActionModal.addEventListener('click', () => actionModal.classList.remove('active'));
  btnCancelAction.addEventListener('click', () => actionModal.classList.remove('active'));

  btnConfirmAction.addEventListener('click', async () => {
    const type = actionType.value;
    const runID = actionTargetRunID.value;
    const operatorID = operatorIDInput.value.trim();
    const reason = actionReasonInput.value.trim();

    if (!operatorID || !reason) {
      alert('작업자 ID와 조치 사유는 필수 입력 사항입니다.');
      return;
    }

    try {
      const res = await fetch('/api/v1/runs/action', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          action: type,
          run_id: runID,
          operator_id: operatorID,
          reason: reason
        })
      });

      if (!res.ok) {
        const errText = await res.text();
        throw new Error(errText);
      }

      actionModal.classList.remove('active');
      loadDashboard();
    } catch (e) {
      alert(`조치 실행 실패: ${e.message}`);
    }
  });

  btnRefresh.addEventListener('click', loadDashboard);
  odateInput.addEventListener('change', loadDashboard);

  if (btnOrderPlan) {
    btnOrderPlan.addEventListener('click', async () => {
      const odate = odateInput.value.trim() || today;
      if (!confirm(`[ODATE: ${odate}] 일자의 전체 배치 계획을 일괄 발주하시겠습니까?\n\n미발주 작업들이 READY/WAIT 상태의 실행 인스턴스로 등록되어, 선행 조건 충족 시 자동으로 연쇄 실행됩니다.`)) {
        return;
      }
      try {
        btnOrderPlan.disabled = true;
        btnOrderPlan.textContent = '⏳ 발주 처리 중...';
        const res = await fetch('/api/v1/runs/order', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ odate: odate })
        });
        if (!res.ok) {
          const errText = await res.text();
          throw new Error(errText);
        }
        const data = await res.json();
        alert(`[일괄 발주 성공]\n- 신규 발주: ${data.ordered_count}건 (즉시실행 READY: ${data.ready_count}, 대기 WAIT: ${data.wait_count})\n- 기존 발주 완료: ${data.skipped_count}건`);
        await loadDashboard();
      } catch (err) {
        alert(`일괄 발주 실패: ${err.message}`);
      } finally {
        btnOrderPlan.disabled = false;
        btnOrderPlan.textContent = '📅 당일 계획 발주';
      }
    });
  }

  // ==========================================
  // NEW JOB MODAL
  // ==========================================
  const btnNewJob = document.getElementById('btnNewJob');
  const newJobModal = document.getElementById('newJobModal');
  const btnCloseNewJobModal = document.getElementById('btnCloseNewJobModal');
  const btnCancelNewJob = document.getElementById('btnCancelNewJob');
  const btnSubmitNewJob = document.getElementById('btnSubmitNewJob');

  const newJobName = document.getElementById('newJobName');
  const newJobGroup = document.getElementById('newJobGroup');
  const newJobCommand = document.getElementById('newJobCommand');
  const newJobArgs = document.getElementById('newJobArgs');
  const newJobCron = document.getElementById('newJobCron');
  const newJobLabels = document.getElementById('newJobLabels');
  const newJobInConds = document.getElementById('newJobInConds');
  const newJobOutConds = document.getElementById('newJobOutConds');
  const newJobRunNow = document.getElementById('newJobRunNow');

  btnNewJob.addEventListener('click', () => {
    // Start from an empty form every time, with Run Now off, so nothing is ordered by accident.
    [newJobName, newJobGroup, newJobCommand, newJobArgs, newJobCron, newJobLabels, newJobInConds, newJobOutConds]
      .forEach(el => { el.value = ''; });
    newJobRunNow.checked = false;
    newJobModal.classList.add('active');
    newJobName.focus();
  });

  const closeNewJobModal = () => newJobModal.classList.remove('active');
  btnCloseNewJobModal.addEventListener('click', closeNewJobModal);
  btnCancelNewJob.addEventListener('click', closeNewJobModal);

  btnSubmitNewJob.addEventListener('click', async () => {
    const name = newJobName.value.trim();
    const command = newJobCommand.value.trim();
    if (!name || !command) {
      alert('작업명과 명령어는 필수 입력 항목입니다.');
      return;
    }

    const group = newJobGroup.value.trim(); // empty: the server assigns DEFAULT
    const cron = newJobCron.value.trim();
    const labels = newJobLabels.value.split(',').map(s => s.trim()).filter(Boolean);
    const inConds = newJobInConds.value.split(',').map(s => s.trim()).filter(Boolean);
    const outConds = newJobOutConds.value.split(',').map(s => s.trim()).filter(Boolean);
    const runNow = newJobRunNow.checked;
    const odate = odateInput.value.trim() || today;

    const args = parseArgs(newJobArgs.value);

    const payload = {
      name: name,
      group: group,
      command: command,
      args: args,
      cron_expr: cron,
      agent_labels: labels,
      in_conditions: inConds,
      out_conditions: outConds,
      run_now: runNow,
      odate: odate
    };

    try {
      const res = await fetch('/api/v1/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!res.ok) {
        const errText = await res.text();
        throw new Error(errText);
      }

      closeNewJobModal();
      await loadDashboard();
    } catch (e) {
      alert(`배치 등록 실패: ${e.message}`);
    }
  });

  // ==========================================
  // UNIFIED 5-TAB SWITCHING & DATA LOADERS
  // ==========================================
  function switchTab(targetId) {
    activeTabId = targetId;
    tabButtons.forEach(btn => {
      btn.classList.toggle('active', btn.dataset.target === targetId);
    });
    tabPanes.forEach(pane => {
      pane.classList.toggle('active', pane.id === targetId);
    });
    refreshActiveTab();
  }

  tabButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      const targetId = btn.dataset.target;
      if (targetId) switchTab(targetId);
    });
  });

  function refreshActiveTab() {
    switch (activeTabId) {
      case 'containerRuns':
        loadDashboard();
        break;
      case 'containerDefs':
        loadDefsTable();
        break;
      case 'containerConds':
        loadCondsTable();
        break;
      case 'containerAudits':
        loadAuditsTable();
        break;
      case 'containerAgents':
        loadAgentsTable();
        break;
    }
  }

  async function loadDefsTable() {
    defsTableBody.innerHTML = '<tr><td colspan="6" class="loading-cell">정의 목록 불러오는 중...</td></tr>';
    try {
      const res = await fetch('/api/v1/jobs');
      const defs = (await res.json()) || [];
      if (tabBadgeDefs) tabBadgeDefs.textContent = defs.length;

      if (defs.length === 0) {
        defsTableBody.innerHTML = '<tr><td colspan="6" class="loading-cell">등록된 배치 정의가 없습니다.</td></tr>';
        return;
      }

      defsTableBody.innerHTML = defs.map(d => {
        const cmdStr = `${d.command || ''} ${(d.args || []).join(' ')}`.trim();
        return `
          <tr>
            <td><code>${d.id}</code></td>
            <td><strong style="cursor:pointer; color:#38bdf8;" class="btn-def-detail" data-id="${d.id}">${d.name}</strong></td>
            <td><span class="badge" style="background:#1e293b; padding:2px 6px; border-radius:4px;">${d.group || 'DEFAULT'}</span></td>
            <td><code>${d.cron_expr || '-'}</code></td>
            <td><code title="${cmdStr}">${cmdStr.length > 30 ? cmdStr.slice(0, 30) + '...' : cmdStr}</code></td>
            <td>
              <button class="btn-action btn-def-detail" data-id="${d.id}">수정/상세</button>
              <button class="btn-action btn-def-trigger" data-id="${d.id}" style="background:#0284c7;">⚡실행</button>
              <button class="btn-action btn-def-delete" data-id="${d.id}" style="background:#ef4444;">삭제</button>
            </td>
          </tr>
        `;
      }).join('');

      document.querySelectorAll('.btn-def-detail').forEach(b => b.addEventListener('click', () => openJobDetailModal(b.dataset.id)));
      document.querySelectorAll('.btn-def-trigger').forEach(b => b.addEventListener('click', () => triggerJobImmediately(b.dataset.id)));
      document.querySelectorAll('.btn-def-delete').forEach(b => b.addEventListener('click', () => deleteJobDefinition(b.dataset.id)));
    } catch (e) {
      defsTableBody.innerHTML = `<tr><td colspan="6" class="loading-cell">조회 실패: ${e.message}</td></tr>`;
    }
  }

  // Conditions Tab Loader
  async function loadCondsTable() {
    if (!condsTableBody) return;
    const odate = odateInput.value.trim() || today;
    condsTableBody.innerHTML = '<tr><td colspan="4" class="loading-cell">조건 목록 불러오는 중...</td></tr>';
    try {
      const res = await fetch(`/api/v1/conditions?date=${odate}`);
      const conds = (await res.json()) || [];
      if (tabBadgeConds) tabBadgeConds.textContent = conds.length;

      if (conds.length === 0) {
        condsTableBody.innerHTML = '<tr><td colspan="4" class="loading-cell">금일 등록된 조건(Condition) 신호가 없습니다.</td></tr>';
        return;
      }

      condsTableBody.innerHTML = conds.map(c => {
        const timeStr = c.created_at ? c.created_at.slice(11, 19) : '-';
        return `
          <tr>
            <td><strong style="color:#10b981; font-family:'JetBrains Mono',monospace;">${c.name}</strong></td>
            <td><code>${c.odate || odate}</code></td>
            <td>${timeStr}</td>
            <td>
              <button class="btn-action btn-cond-delete" data-name="${c.name}" data-odate="${c.odate || odate}" style="background:#ef4444;" title="조건 강제 소거/삭제">삭제</button>
            </td>
          </tr>
        `;
      }).join('');

      document.querySelectorAll('.btn-cond-delete').forEach(b => {
        b.addEventListener('click', async () => {
          if (!confirm(`조건 [${b.dataset.name}]을 삭제하시겠습니까?`)) return;
          try {
            const delRes = await fetch(`/api/v1/conditions?name=${encodeURIComponent(b.dataset.name)}&date=${b.dataset.odate}`, {
              method: 'DELETE'
            });
            if (!delRes.ok) throw new Error(await delRes.text());
            loadCondsTable();
            loadDashboard();
          } catch (e) {
            alert(`조건 삭제 실패: ${e.message}`);
          }
        });
      });
    } catch (e) {
      condsTableBody.innerHTML = `<tr><td colspan="4" class="loading-cell">조건 조회 실패: ${e.message}</td></tr>`;
    }
  }

  // Manual Condition Add
  if (btnAddCondition && inputNewCondName) {
    btnAddCondition.addEventListener('click', async () => {
      const name = inputNewCondName.value.trim().toUpperCase();
      if (!name) {
        alert('발행할 조건명을 입력해주세요. (예: MANUAL_APPROVE_OK)');
        return;
      }
      const odate = odateInput.value.trim() || today;
      try {
        const res = await fetch('/api/v1/conditions', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: name, odate: odate })
        });
        if (!res.ok) throw new Error(await res.text());
        inputNewCondName.value = '';
        loadCondsTable();
        loadDashboard();
      } catch (e) {
        alert(`조건 발행 실패: ${e.message}`);
      }
    });
    inputNewCondName.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') btnAddCondition.click();
    });
  }

  // Audit Trail Tab Loader
  async function loadAuditsTable() {
    if (!auditsTableBody) return;
    auditsTableBody.innerHTML = '<tr><td colspan="5" class="loading-cell">감사 로그 불러오는 중...</td></tr>';
    try {
      const res = await fetch('/api/v1/audits');
      const audits = (await res.json()) || [];
      if (tabBadgeAudits) tabBadgeAudits.textContent = audits.length;

      if (audits.length === 0) {
        auditsTableBody.innerHTML = '<tr><td colspan="5" class="loading-cell">기록된 운영자 조치 감사 로그가 없습니다.</td></tr>';
        return;
      }

      auditsTableBody.innerHTML = audits.map(a => {
        const timeStr = a.created_at ? a.created_at.slice(0, 19).replace('T', ' ') : '-';
        let actionBadgeColor = '#3b82f6';
        if (a.action === 'SET_OK') actionBadgeColor = '#10b981';
        else if (a.action === 'RERUN') actionBadgeColor = '#f59e0b';
        else if (a.action === 'BYPASS') actionBadgeColor = '#8b5cf6';

        return `
          <tr>
            <td><code>${a.operator_id}</code></td>
            <td><span class="badge" style="background:${actionBadgeColor}; color:#fff; font-weight:600; padding:2px 6px; border-radius:4px;">${a.action}</span></td>
            <td><code>${a.target_id}</code></td>
            <td style="color:#e2e8f0;">${a.reason || '-'}</td>
            <td>${timeStr}</td>
          </tr>
        `;
      }).join('');
    } catch (e) {
      auditsTableBody.innerHTML = `<tr><td colspan="5" class="loading-cell">감사 로그 조회 실패: ${e.message}</td></tr>`;
    }
  }

  // Agents Pool Tab Loader
  async function loadAgentsTable() {
    if (!agentsTableBody) return;
    agentsTableBody.innerHTML = '<tr><td colspan="6" class="loading-cell">에이전트 현황 불러오는 중...</td></tr>';
    try {
      const res = await fetch('/api/v1/agents');
      const data = await res.json();
      const agents = data.agents || [];
      if (tabBadgeAgents) tabBadgeAgents.textContent = agents.length || data.active_agent_count || 0;

      if (agents.length === 0) {
        agentsTableBody.innerHTML = '<tr><td colspan="6" class="loading-cell">현재 연결된 워커 노드 에이전트가 없습니다.</td></tr>';
        return;
      }

      agentsTableBody.innerHTML = agents.map(ag => {
        const labelsHtml = (ag.labels || []).map(l => `<span class="badge" style="background:#1e293b; color:#94a3b8; margin-right:4px; padding:1px 5px; border-radius:3px;">${l}</span>`).join('') || '<span style="color:#64748b;">(None)</span>';
        const activeTasks = ag.active_tasks || 0;
        const maxConc = ag.max_concurrency || 10;
        const percent = Math.min(100, Math.round((activeTasks / maxConc) * 100));

        return `
          <tr>
            <td><strong style="color:#38bdf8;">${ag.agent_id}</strong></td>
            <td><code>${ag.hostname || '-'}</code></td>
            <td><span class="badge" style="background:#334155; color:#cbd5e1; padding:2px 6px; border-radius:4px;">${ag.os || 'linux'}</span></td>
            <td>${labelsHtml}</td>
            <td>
              <div style="display:flex; align-items:center; gap:8px;">
                <div style="flex:1; background:#1e293b; height:6px; border-radius:3px; overflow:hidden;">
                  <div style="width:${percent}%; background:#0284c7; height:100%; border-radius:3px;"></div>
                </div>
                <code style="font-size:11px;">${activeTasks}/${maxConc}</code>
              </div>
            </td>
            <td><span class="badge" style="background:#065f46; color:#34d399; font-weight:600; padding:2px 6px; border-radius:4px;">ONLINE</span></td>
          </tr>
        `;
      }).join('');
    } catch (e) {
      agentsTableBody.innerHTML = `<tr><td colspan="6" class="loading-cell">에이전트 조회 실패: ${e.message}</td></tr>`;
    }
  }

  // ==========================================
  // JOB DETAIL & EDIT MODAL
  // ==========================================
  const jobDetailModal = document.getElementById('jobDetailModal');
  const btnCloseJobDetailModal = document.getElementById('btnCloseJobDetailModal');
  const btnCancelEditJob = document.getElementById('btnCancelEditJob');
  const btnSaveJob = document.getElementById('btnSaveJob');
  const btnTriggerJob = document.getElementById('btnTriggerJob');
  const btnDeleteJob = document.getElementById('btnDeleteJob');

  const editJobID = document.getElementById('editJobID');
  const editJobName = document.getElementById('editJobName');
  const editJobGroup = document.getElementById('editJobGroup');
  const editJobCommand = document.getElementById('editJobCommand');
  const editJobArgs = document.getElementById('editJobArgs');
  const editJobCron = document.getElementById('editJobCron');
  const editJobLabels = document.getElementById('editJobLabels');
  const editJobInConds = document.getElementById('editJobInConds');
  const editJobOutConds = document.getElementById('editJobOutConds');
  const editJobEnabled = document.getElementById('editJobEnabled');

  const closeJobDetailModal = () => jobDetailModal.classList.remove('active');
  btnCloseJobDetailModal.addEventListener('click', closeJobDetailModal);
  btnCancelEditJob.addEventListener('click', closeJobDetailModal);

  // Definition as loaded into the edit modal. PUT replaces the whole definition, so fields the
  // modal does not show (timeout_sec, env) are sent back from here instead of being reset.
  let editingDef = null;

  async function openJobDetailModal(jobID) {
    try {
      const res = await fetch(`/api/v1/jobs/detail?id=${jobID}`);
      if (!res.ok) {
        throw new Error('배치 정의 정보를 찾을 수 없습니다.');
      }
      const def = await res.json();
      editingDef = def;

      editJobID.value = def.id || '';
      editJobName.value = def.name || '';
      editJobGroup.value = def.group || 'DEFAULT';
      editJobCommand.value = def.command || '';
      editJobArgs.value = formatArgs(def.args);
      editJobCron.value = def.cron_expr || '';
      editJobLabels.value = (def.agent_labels || []).join(', ');
      editJobInConds.value = (def.in_conditions || []).join(', ');
      editJobOutConds.value = (def.out_conditions || []).join(', ');
      editJobEnabled.checked = def.enabled !== false;

      jobDetailModal.classList.add('active');
    } catch (e) {
      alert(e.message);
    }
  }

  btnSaveJob.addEventListener('click', async () => {
    const id = editJobID.value.trim();
    const name = editJobName.value.trim();
    const command = editJobCommand.value.trim();

    if (!id || !name || !command) {
      alert('작업명과 명령어는 필수 입력 사항입니다.');
      return;
    }

    // Keep the original args untouched when the field was not edited (they may contain quotes
    // that the field cannot represent).
    const original = editingDef && editingDef.id === id ? editingDef : {};
    const args = editJobArgs.value === formatArgs(original.args) ? (original.args || []) : parseArgs(editJobArgs.value);

    const payload = {
      ...original,
      id: id,
      name: name,
      group: editJobGroup.value.trim() || 'DEFAULT',
      command: command,
      args: args,
      cron_expr: editJobCron.value.trim(),
      agent_labels: editJobLabels.value.split(',').map(s => s.trim()).filter(Boolean),
      in_conditions: editJobInConds.value.split(',').map(s => s.trim()).filter(Boolean),
      out_conditions: editJobOutConds.value.split(',').map(s => s.trim()).filter(Boolean),
      enabled: editJobEnabled.checked
    };

    try {
      const res = await fetch('/api/v1/jobs', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!res.ok) {
        const errText = await res.text();
        throw new Error(errText);
      }

      alert('배치 작업 정의가 성공적으로 수정되었습니다.');
      closeJobDetailModal();
      loadDashboard();
      if (containerDefs.style.display === 'block') {
        loadDefsTable();
      }
    } catch (e) {
      alert(`수정 실패: ${e.message}`);
    }
  });

  async function triggerJobImmediately(jobID) {
    const odate = odateInput.value.trim() || today;
    try {
      const res = await fetch('/api/v1/jobs/trigger', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job_id: jobID, odate: odate })
      });

      if (!res.ok) {
        const errText = await res.text();
        throw new Error(errText);
      }

      switchTab('containerRuns');
    } catch (e) {
      alert(`즉시 실행 발주 실패: ${e.message}`);
    }
  }

  btnTriggerJob.addEventListener('click', () => {
    const id = editJobID.value.trim();
    if (id) {
      closeJobDetailModal();
      triggerJobImmediately(id);
    }
  });

  async function deleteJobDefinition(jobID) {
    if (!confirm(`정말 배치 정의 [${jobID}]를 삭제하시겠습니까?`)) {
      return;
    }

    try {
      const res = await fetch(`/api/v1/jobs?id=${jobID}`, {
        method: 'DELETE'
      });

      if (!res.ok) {
        const errText = await res.text();
        throw new Error(errText);
      }

      alert('배치 정의가 삭제되었습니다.');
      loadDefsTable();
      loadDashboard();
    } catch (e) {
      alert(`삭제 실패: ${e.message}`);
    }
  }

  btnDeleteJob.addEventListener('click', () => {
    const id = editJobID.value.trim();
    if (id) {
      closeJobDetailModal();
      deleteJobDefinition(id);
    }
  });

  // Initial Load & Auto-refresh every 3s
  loadDashboard();
  setInterval(() => {
    if (isDraggingCanvas) return;
    if (jobDetailModal && jobDetailModal.classList.contains('active')) return;
    if (actionModal && actionModal.classList.contains('active')) return;
    if (logModal && logModal.classList.contains('active')) return;
    if (newJobModal && newJobModal.classList.contains('active')) return;

    if (activeTabId === 'containerRuns') {
      loadDashboard();
    } else if (activeTabId === 'containerConds') {
      loadCondsTable();
    } else if (activeTabId === 'containerAudits') {
      loadAuditsTable();
    } else if (activeTabId === 'containerAgents') {
      loadAgentsTable();
    }
  }, 3000);
});
