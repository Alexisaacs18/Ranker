// Medical Provider Risk Database - Frontend Application
// Updated for qui tam medical fraud data

import { POWER_ALIASES } from "./power_aliases.js";

// FIXED: Update file paths to match your output
const DATA_URL = "/data/results/qui_tam_ranked.jsonl";
const CHUNK_MANIFEST_URL = "/data/chunks.json";
const DEFAULT_CHUNK_SIZE = 1000;

// Since you're not using chunking (chunk-size=0), these paths won't be used,
// but keeping them for future compatibility
const CHUNK_DIR = "/contrib";

const elements = {
  scoreFilter: document.getElementById("scoreFilter"),
  scoreValue: document.getElementById("scoreValue"),
  leadFilter: document.getElementById("leadTypeFilter"),
  powerFilter: document.getElementById("powerFilter"),
  searchInput: document.getElementById("searchInput"),
  limitInput: document.getElementById("limitInput"),
  resetFilters: document.getElementById("resetFilters"),
  countStat: document.getElementById("countStat"),
  avgStat: document.getElementById("avgStat"),
  leadStat: document.getElementById("leadStat"),
  updatedStat: document.getElementById("updatedStat"),
  detailDrawer: document.getElementById("detailDrawer"),
  detailTitle: document.getElementById("detailTitle"),
  detailReason: document.getElementById("detailReason"),
  detailInsights: document.getElementById("detailInsights"),
  detailLeadTypes: document.getElementById("detailLeadTypes"),
  detailPower: document.getElementById("detailPower"),
  detailAgencies: document.getElementById("detailAgencies"),
  detailTags: document.getElementById("detailTags"),
  detailModel: document.getElementById("detailModel"),
  detailText: document.getElementById("detailText"),
  detailTextPreview: document.getElementById("detailTextPreview"),
  detailTextToggle: document.getElementById("detailTextToggle"),
  detailClose: document.getElementById("detailClose"),
  loadingOverlay: document.getElementById("loadingOverlay"),
  loadingTitle: document.getElementById("loadingTitle"),
  loadingSubtitle: document.getElementById("loadingSubtitle"),
  loadingProgress: document.getElementById("loadingProgress"),
  inlineLoader: document.getElementById("inlineLoader"),
  inlineLoaderText: document.getElementById("inlineLoaderText"),
  processedCount: document.getElementById("processedCount"),
  scriptOutputModal: document.getElementById("scriptOutputModal"),
  scriptOutputTitle: document.getElementById("scriptOutputTitle"),
  scriptOutputClose: document.getElementById("scriptOutputClose"),
  scriptOutputStop: document.getElementById("scriptOutputStop"),
  scriptOutputStopModal: document.getElementById("scriptOutputStopModal"),
  scriptOutputPre: document.getElementById("scriptOutputPre"),
  deleteDataBtn: document.getElementById("deleteDataBtn"),
  websiteUrl: document.getElementById("websiteUrl"),
  maxPages: document.getElementById("maxPages"),
  urlPattern: document.getElementById("urlPattern"),
  linkSelector: document.getElementById("linkSelector"),
  scrapeWebsiteBtn: document.getElementById("scrapeWebsiteBtn"),
};

const state = {
  raw: [],
  filtered: [],
  lastUpdated: null,
  manifestMetadata: null,
  gridApi: null,  // FIXED: Store grid API directly
  leadChart: null,
  scoreChart: null,
  powerChart: null,
  agencyChart: null,
  leadChoices: null,
  powerChoices: null,
  loading: {
    totalChunks: 0,
    loadedChunks: 0,
  },
  currentLoadId: 0,
  activeRowId: null,
  powerDisplayNames: {},
  filtersEnabled: false,
};

// Power alias functions remain the same
const powerAliasLookup = buildPowerAliasLookup(POWER_ALIASES);
const canonicalPowerList = buildCanonicalPowerList(powerAliasLookup);
const powerKeywordMap = buildPowerKeywordMap(POWER_ALIASES, powerAliasLookup);
const canonicalAliasKeyMap = buildCanonicalAliasKeyMap(POWER_ALIASES);

function buildPowerAliasLookup(aliasMap) {
  const lookup = new Map();
  Object.entries(aliasMap || {}).forEach(([canonical, aliases]) => {
    const canonicalKey = cleanPowerAlias(canonical);
    if (canonicalKey && !lookup.has(canonicalKey)) {
      lookup.set(canonicalKey, canonical);
    }
    (aliases || []).forEach((alias) => {
      const key = cleanPowerAlias(alias);
      if (!key) return;
      if (!lookup.has(key)) {
        lookup.set(key, canonical);
      }
    });
  });
  return lookup;
}

function buildCanonicalPowerList(lookup) {
  const list = [];
  const seen = new Set();
  lookup.forEach((canonical) => {
    const clean = cleanPowerAlias(canonical);
    if (!clean || seen.has(clean)) return;
    seen.add(clean);
    list.push({ canonical, clean });
  });
  return list;
}

function buildPowerKeywordMap(aliasMap) {
  const keywordMap = new Map();
  Object.entries(aliasMap || {}).forEach(([canonical, aliases]) => {
    const cleanCanonical = cleanPowerAlias(canonical);
    if (!cleanCanonical) return;
    const keywords = new Set();
    keywords.add(cleanCanonical);
    cleanCanonical.split(" ").forEach((token) => keywords.add(token));
    (aliases || []).forEach((alias) => {
      const cleanAlias = cleanPowerAlias(alias);
      if (!cleanAlias) return;
      keywords.add(cleanAlias);
      cleanAlias.split(" ").forEach((token) => keywords.add(token));
    });
    keywordMap.set(canonical, Array.from(keywords));
  });
  return keywordMap;
}

function buildCanonicalAliasKeyMap(aliasMap) {
  const map = new Map();
  Object.entries(aliasMap || {}).forEach(([canonical, aliases]) => {
    const canonicalKey = cleanPowerAlias(canonical);
    if (!canonicalKey) return;
    const keys = new Set();
    keys.add(canonicalKey);
    (aliases || []).forEach((alias) => {
      const key = cleanPowerAlias(alias);
      if (key) keys.add(key);
    });
    map.set(canonical, keys);
  });
  return map;
}

function canonicalizePowerSelection(value) {
  const candidates = generatePowerAliasCandidates(value);
  for (const key of candidates.keys) {
    const canonical = powerAliasLookup.get(key);
    if (canonical) return canonical;
  }
  const fallback = findCanonicalByToken(candidates.bestKey);
  return fallback || value;
}

function aliasKeysForCanonical(canonical) {
  const canonicalKey = cleanPowerAlias(canonical);
  if (!canonicalKey) return new Set();
  if (canonicalAliasKeyMap.has(canonical)) {
    return new Set(canonicalAliasKeyMap.get(canonical));
  }
  return new Set([canonicalKey]);
}

function cleanPowerAlias(name) {
  if (!name) return "";
  return String(name)
    .toLowerCase()
    .replace(/[–—]/g, "-")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePowerMentions(values) {
  const normalized = [];
  const seen = new Set();
  (values || []).forEach((originalName) => {
    const candidates = generatePowerAliasCandidates(originalName);
    let canonical = null;
    for (const key of candidates.keys) {
      canonical = powerAliasLookup.get(key);
      if (canonical) break;
    }
    if (!canonical) {
      const tokenFallback = findCanonicalByToken(candidates.bestKey);
      if (tokenFallback) canonical = tokenFallback;
    }
    if (!canonical) canonical = originalName;
    const display = typeof canonical === "string" ? canonical : originalName;
    const displayKey = cleanPowerAlias(display);
    if (displayKey && !seen.has(displayKey)) {
      seen.add(displayKey);
      normalized.push(display);
    }
  });
  return normalized;
}

function expandPowerSelection(values) {
  const canonicalKeys = new Set();
  const aliasKeys = new Set();
  (values || []).forEach((value) => {
    const selectedCanonical = canonicalizePowerSelection(value);
    const canonicalKey = cleanPowerAlias(selectedCanonical);
    if (!canonicalKey) return;
    canonicalKeys.add(canonicalKey);
    aliasKeysForCanonical(selectedCanonical).forEach((key) => aliasKeys.add(key));
  });
  return { canonicalKeys, aliasKeys };
}

function matchesPowerSelection(name, selection) {
  const canonical = canonicalizePowerSelection(name);
  const canonicalKey = cleanPowerAlias(canonical);
  if (!canonicalKey || !selection || selection.aliasKeys.size === 0) return false;
  if (selection.aliasKeys.has(canonicalKey)) return true;
  const rowAliasKeys = aliasKeysForCanonical(canonical);
  return Array.from(rowAliasKeys).some((key) => selection.aliasKeys.has(key));
}

function generatePowerAliasCandidates(name) {
  const result = { keys: [], bestKey: "" };
  if (!name) return result;
  const candidates = [];
  const trimmed = String(name).trim();
  candidates.push(trimmed);
  const flipped = flipCommaName(trimmed);
  if (flipped) candidates.push(flipped);
  for (const candidate of candidates) {
    const key = cleanPowerAlias(candidate);
    if (key) {
      result.keys.push(key);
      if (!result.bestKey) result.bestKey = key;
    }
  }
  return result;
}

function flipCommaName(name) {
  if (!name || !name.includes(",")) return null;
  const [last, rest] = name.split(",", 2).map((part) => part.trim());
  if (!last || !rest) return null;
  const lastIsSingleWord = /^[A-Za-z'.-]+$/.test(last);
  const restWordCount = rest.split(/\s+/).filter(Boolean).length;
  if (!lastIsSingleWord || restWordCount === 0 || restWordCount > 3) {
    return null;
  }
  return `${rest} ${last}`.trim();
}

function findCanonicalByToken(key) {
  if (!key || key.length < 3) return null;
  const matches = canonicalPowerList.filter((item) => {
    const words = item.clean.split(" ");
    return words.includes(key);
  });
  if (matches.length === 1) return matches[0].canonical;
  return null;
}

// FIXED: Update normalizeRow to handle correct field names
function normalizeRow(row) {
  const qui_tam = Number(row.qui_tam_score ?? 0);
  const arrays = (value) => (Array.isArray(value) ? value : []);
  
  const rawActors = arrays(row.implicated_actors)
    .map((p) => (typeof p === "string" ? p.trim() : String(p ?? "").trim()))
    .filter(Boolean);
  const normalizedActors = normalizePowerMentions(rawActors);
  
  const normalized = {
    filename: row.filename,
    source_row_index: row.metadata?.source_row_index ?? null,
    headline: row.headline || row.metadata?.original_row?.filename || "Untitled Provider",
    qui_tam_score: Number.isFinite(qui_tam) ? qui_tam : 0,
    reason: row.reason || "",
    key_facts: arrays(row.key_facts),
    statute_violations: arrays(row.statute_violations),
    implicated_actors_raw: rawActors,
    implicated_actors: normalizedActors.length > 0 ? normalizedActors : rawActors,
    federal_programs_involved: arrays(row.federal_programs_involved),
    fraud_type: row.fraud_type || "Unknown",
    metadata: row.metadata || {},
    original_text: row.metadata?.original_row?.text || "",
  };
  
  normalized.search_blob = [
    normalized.headline,
    normalized.reason,
    normalized.key_facts.join(" "),
    normalized.statute_violations.join(" "),
    normalized.implicated_actors.join(" "),
    normalized.fraud_type,
    normalized.original_text,
  ]
    .join(" ")
    .toLowerCase();
    
  return normalized;
}

function resetLoadingState(title = "Loading Medical Records…", subtitle = "Preparing data") {
  state.powerDisplayNames = {};
  state.loading.loadedChunks = 0;
  state.loading.totalChunks = 0;
  setFiltersEnabled(false);
  if (elements.loadingOverlay) {
    elements.loadingOverlay.classList.remove("hidden");
    elements.loadingTitle.textContent = title;
    elements.loadingSubtitle.textContent = subtitle;
  }
  if (elements.loadingProgress) {
    elements.loadingProgress.style.width = "0%";
  }
  hideInlineLoader();
}

function updateLoadingProgress(loaded, total, subtitle) {
  state.loading.loadedChunks = loaded;
  state.loading.totalChunks = total;
  const percent = total > 0 ? Math.min(100, Math.round((loaded / total) * 100)) : 0;

  if (elements.loadingProgress) {
    elements.loadingProgress.style.width = `${percent}%`;
  }

  if (elements.loadingSubtitle) {
    elements.loadingSubtitle.textContent =
      subtitle || (total > 0 ? `Loading ${loaded}/${total} files (${percent}%)` : "Loading data…");
  }

  if (elements.inlineLoader && !elements.inlineLoader.classList.contains("hidden")) {
    elements.inlineLoaderText.textContent =
      subtitle || (total > 0 ? `Loading remaining files (${percent}%)` : "Loading files…");
  }
}

function finishInitialLoadingUI() {
  if (elements.loadingOverlay) {
    elements.loadingOverlay.classList.add("hidden");
  }
}

function setFiltersEnabled(enabled, { triggerApply = false } = {}) {
  state.filtersEnabled = !!enabled;
  const controls = [
    elements.scoreFilter,
    elements.leadFilter,
    elements.powerFilter,
    elements.searchInput,
    elements.limitInput,
    elements.resetFilters,
  ];
  controls.forEach((el) => {
    if (el) el.disabled = !enabled;
  });
  if (state.leadChoices) {
    enabled ? state.leadChoices.enable() : state.leadChoices.disable();
  }
  if (state.powerChoices) {
    enabled ? state.powerChoices.enable() : state.powerChoices.disable();
  }
  const waitNote = document.getElementById("filterWaitNote");
  if (waitNote) {
    waitNote.classList.toggle("hidden", enabled);
  }
  if (enabled && triggerApply) {
    applyFilters({ force: true });
  }
}

function showInlineLoader(message) {
  if (!elements.inlineLoader) return;
  elements.inlineLoaderText.textContent = message;
  elements.inlineLoader.classList.remove("hidden");
}

function hideInlineLoader() {
  if (!elements.inlineLoader) return;
  elements.inlineLoader.classList.add("hidden");
  elements.inlineLoaderText.textContent = "";
}

function isMobileView() {
  return window.innerWidth <= 768;
}

function getGridColumnDefs() {
  const isMobile = isMobileView();
  return [
    {
      headerName: "Risk",
      field: "qui_tam_score",
      width: isMobile ? 65 : 80,
      minWidth: isMobile ? 65 : 80,
      maxWidth: isMobile ? 65 : 100,
      filter: "agNumberColumnFilter",
      cellClass: "score-cell",
      pinned: isMobile ? "left" : null,
      tooltipValueGetter: (params) => `Risk Score: ${params.value ?? 0}`,
    },
    {
      headerName: "Headline",
      field: "headline",
      flex: isMobile ? 1 : 6,
      minWidth: isMobile ? 200 : 400,
      cellRenderer: (params) => {
        const headline = params.value || "Untitled Provider";
        return `<strong class="cell-text">${headline}</strong>`;
      },
      tooltipValueGetter: (params) => params.data?.headline || "Untitled Provider",
    },
    {
      headerName: "File",
      field: "filename",
      flex: 1,
      minWidth: 120,
      hide: isMobile,
      cellRenderer: (params) => `<span class="cell-text">${params.value || ""}</span>`,
      tooltipValueGetter: (params) => params.data?.filename || "",
    },
    {
      headerName: "Providers",
      field: "implicated_actors",
      flex: 2,
      minWidth: 180,
      hide: isMobile,
      cellRenderer: (params) => `<span class="cell-text">${(params.value || []).join(", ")}</span>`,
      tooltipValueGetter: (params) => (params.data?.implicated_actors || []).join(", "),
    },
    {
      headerName: "Fraud Type",
      field: "fraud_type",
      flex: 1.2,
      minWidth: 150,
      hide: isMobile,
      cellRenderer: (params) => `<span class="cell-text">${params.value || ""}</span>`,
      tooltipValueGetter: (params) => params.data?.fraud_type || "",
    },
    {
      headerName: "Federal Programs",
      field: "federal_programs_involved",
      flex: 1.2,
      minWidth: 140,
      hide: isMobile,
      cellRenderer: (params) => `<span class="cell-text">${(params.value || []).join(", ")}</span>`,
      tooltipValueGetter: (params) => (params.data?.federal_programs_involved || []).join(", "),
    },
    {
      headerName: "Violations",
      field: "statute_violations",
      flex: 1.2,
      minWidth: 140,
      hide: isMobile,
      cellRenderer: (params) => `<span class="cell-text">${(params.value || []).join(", ")}</span>`,
      tooltipValueGetter: (params) => (params.data?.statute_violations || []).join(", "),
    },
  ];
}

// FIXED: Use new AG Grid v31+ API
function initGrid() {
  const gridElement = document.getElementById("grid");
  const isMobile = isMobileView();
  
  const gridOptions = {
    columnDefs: getGridColumnDefs(),
    defaultColDef: {
      resizable: true,
      sortable: true,
      filter: true,
      flex: 1,
      minWidth: isMobile ? 80 : 130,
      wrapText: false,
      autoHeight: false,
      tooltipComponentParams: { color: "#fff" },
    },
    suppressMovableColumns: true,
    animateRows: true,
    pagination: true,
    paginationPageSize: isMobile ? 15 : 25,
    paginationPageSizeSelector: [15, 25, 50, 100],  // FIXED: Added selector
    rowHeight: isMobile ? 50 : 58,
    onGridReady: (params) => {
      // FIXED: Use new API methods
      params.api.setGridOption('rowData', state.filtered);
      params.api.applyColumnState({
        state: [{ colId: "qui_tam_score", sort: "desc" }],
        defaultState: { sort: null },
      });
      const topRow = params.api.getDisplayedRowAtIndex(0)?.data;
      if (topRow) {
        params.api.ensureIndexVisible(0);
        renderDetail(topRow);
      } else if (state.filtered.length > 0) {
        renderDetail(state.filtered[0]);
      }
    },
    onRowClicked: (event) => renderDetail(event.data, { scrollToDetail: true }),
    getRowId: (params) => params.data.filename,
  };

  // FIXED: Use new createGrid API
  state.gridApi = agGrid.createGrid(gridElement, gridOptions);
}

function parseJsonl(text) {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch (err) {
        console.warn("Skipping malformed JSONL line", err);
        return null;
      }
    })
    .filter(Boolean);
}

function populateFilters(data, preserveSelection = false) {
  const prevLead = preserveSelection ? getSelectedValues(state.leadChoices) : [];
  const prevPower = preserveSelection ? getSelectedValues(state.powerChoices) : [];
  
  const leadCounts = new Map();
  data.forEach(row => {
    const fraudType = row.fraud_type || "Unknown";
    leadCounts.set(fraudType, (leadCounts.get(fraudType) || 0) + 1);
  });
  
  const powerCounts = buildCountMap(data, "implicated_actors");

  const sortedLeads = sortValuesByCount(Array.from(leadCounts.keys()), leadCounts);
  const sortedPowers = sortValuesByCount(Array.from(powerCounts.keys()), powerCounts);

  setChoiceOptions(state.leadChoices, sortedLeads, prevLead, null, leadCounts, leadCounts);
  setChoiceOptions(
    state.powerChoices,
    sortedPowers,
    prevPower,
    powerKeywordMap,
    powerCounts,
    powerCounts
  );
}

function setChoiceOptions(
  choiceInstance,
  values,
  previouslySelected = [],
  keywordMap = null,
  countMap = null,
  baseCountMap = null
) {
  if (!choiceInstance) return;
  
  const selectedSet = new Set(
    previouslySelected.length > 0 ? previouslySelected : getSelectedValues(choiceInstance)
  );
  
  const options = values.map((value) => {
    const customProps = {};
    if (keywordMap) {
      customProps.keywords = keywordMap.get(value) || [];
    }
    if (countMap) {
      customProps.count = countMap.get ? countMap.get(value) || 0 : countMap[value] || 0;
    }
    if (baseCountMap) {
      customProps.baseCount = baseCountMap.get
        ? baseCountMap.get(value) || 0
        : baseCountMap[value] || 0;
    }
    return {
      value,
      label: value,
      customProperties: Object.keys(customProps).length > 0 ? customProps : undefined,
      selected: selectedSet.has(value),
    };
  });
  
  refreshChoices(choiceInstance, options, Array.from(selectedSet));
}

function getSelectedValues(choiceInstance) {
  if (!choiceInstance) return [];
  const value = choiceInstance.getValue(true);
  if (Array.isArray(value)) return value;
  if (value) return [value];
  return [];
}

function applyFilters(options = {}) {
  const force = options.force || false;
  if (!state.filtersEnabled && !force) return;
  
  const minScore = Number(elements.scoreFilter.value) || 0;
  elements.scoreValue.textContent = minScore.toString();
  
  const leadSelected = new Set(getSelectedValues(state.leadChoices));
  const powerSelectedRaw = getSelectedValues(state.powerChoices);
  const powerSelection = expandPowerSelection(powerSelectedRaw);
  const limit = Number(elements.limitInput.value) || null;
  const term = elements.searchInput.value.trim().toLowerCase();

  let filtered = state.raw.filter((row) => row.qui_tam_score >= minScore);
  
  if (leadSelected.size > 0) {
    filtered = filtered.filter((row) => leadSelected.has(row.fraud_type));
  }
  
  if (powerSelection.aliasKeys.size > 0) {
    filtered = filtered.filter((row) =>
      row.implicated_actors.some((name) => matchesPowerSelection(name, powerSelection))
    );
  }
  
  if (term) {
    filtered = filtered.filter((row) => row.search_blob.includes(term));
  }
  
  if (limit && limit > 0) {
    filtered = filtered.slice(0, limit);
  }
  
  filtered.sort((a, b) => b.qui_tam_score - a.qui_tam_score);

  state.filtered = filtered;
  updateChoiceOrdering(filtered, leadSelected, powerSelectedRaw);
  
  // FIXED: Use new AG Grid API
  if (state.gridApi) {
    state.gridApi.setGridOption('rowData', filtered);
    state.gridApi.applyColumnState({
      state: [{ colId: "qui_tam_score", sort: "desc" }],
      defaultState: { sort: null },
    });
    
    let targetRow = null;
    if (state.activeRowId) {
      const rowNode = state.gridApi.getRowNode(state.activeRowId);
      if (rowNode?.data) {
        targetRow = rowNode.data;
        state.gridApi.ensureNodeVisible(rowNode);
      }
    }
    if (!targetRow && filtered.length > 0) {
      targetRow = filtered[0];
      state.gridApi.ensureIndexVisible(0);
    }
    targetRow ? renderDetail(targetRow) : clearDetail();
  } else {
    if (filtered.length > 0) {
      renderDetail(filtered[0]);
    } else {
      clearDetail();
    }
  }
  
  updateSummary();
  updateCharts();
}

function updateSummary() {
  const count = state.filtered.length;
  const average =
    count === 0
      ? 0
      : state.filtered.reduce((sum, row) => sum + row.qui_tam_score, 0) / count;
  
  const fraudCounts = new Map();
  state.filtered.forEach(row => {
    const type = row.fraud_type || "Unknown";
    fraudCounts.set(type, (fraudCounts.get(type) || 0) + 1);
  });
  const sortedFraud = Array.from(fraudCounts.entries()).sort((a, b) => b[1] - a[1]);
  const topLead = sortedFraud.length ? sortedFraud[0][0] : "None";

  const totalLoaded = state.raw.length;
  if (state.manifestMetadata && state.manifestMetadata.total_dataset_rows) {
    const totalDataset = state.manifestMetadata.total_dataset_rows;
    if (typeof totalDataset === 'number') {
      if (count === totalLoaded) {
        elements.countStat.textContent = `${count.toLocaleString()} of ${totalDataset.toLocaleString()} loaded`;
      } else {
        elements.countStat.textContent = `${count.toLocaleString()} of ${totalLoaded.toLocaleString()} loaded (${totalDataset.toLocaleString()} total)`;
      }
    } else {
      elements.countStat.textContent = `${count.toLocaleString()} (${totalLoaded.toLocaleString()} loaded)`;
    }
  } else {
    elements.countStat.textContent = `${count.toLocaleString()} (${totalLoaded.toLocaleString()} loaded)`;
  }

  elements.avgStat.textContent = average.toFixed(1);
  elements.leadStat.textContent = topLead;
  elements.updatedStat.textContent = state.lastUpdated
    ? state.lastUpdated.toLocaleTimeString()
    : "–";

  if (elements.processedCount) {
    elements.processedCount.textContent = totalLoaded.toLocaleString();
  }
}

function aggregateCounts(rows, field) {
  const counter = new Map();
  rows.forEach((row) => {
    (row[field] || []).forEach((value) => {
      counter.set(value, (counter.get(value) || 0) + 1);
    });
  });
  return Array.from(counter.entries())
    .map(([label, value]) => ({ label, value }))
    .sort((a, b) => b.value - a.value);
}

function updateCharts() {
  updateLeadChart();
  updateScoreChart();
  updatePowerChart();
  updateAgencyChart();
}

function buildCountMap(rows, field) {
  const map = new Map();
  rows.forEach((row) => {
    (row[field] || []).forEach((value) => {
      map.set(value, (map.get(value) || 0) + 1);
    });
  });
  return map;
}

function sortValuesByCount(values, primaryCounts, secondaryCounts = null) {
  return Array.from(values).sort((a, b) => {
    const aPrimary = primaryCounts?.get ? primaryCounts.get(a) || 0 : 0;
    const bPrimary = primaryCounts?.get ? primaryCounts.get(b) || 0 : 0;
    if (aPrimary !== bPrimary) return bPrimary - aPrimary;
    if (secondaryCounts) {
      const aSecondary = secondaryCounts.get ? secondaryCounts.get(a) || 0 : 0;
      const bSecondary = secondaryCounts.get ? secondaryCounts.get(b) || 0 : 0;
      if (aSecondary !== bSecondary) return bSecondary - aSecondary;
    }
    return a.localeCompare(b);
  });
}

function refreshChoices(choiceInstance, options, selectedValues = []) {
  if (!choiceInstance) return;
  const uniqueSelected = Array.from(new Set(selectedValues));
  choiceInstance.clearStore();
  choiceInstance.setChoices(options, "value", "label", true);
  if (uniqueSelected.length > 0) {
    choiceInstance.setChoiceByValue(uniqueSelected);
  }
}

function updateChoiceOrdering(filteredRows, leadSelectedSet, powerSelectedRaw) {
  // Build counts for both fraud_type (singular) and implicated_actors (array)
  const leadCountsAll = new Map();
  state.raw.forEach(row => {
    const type = row.fraud_type || "Unknown";
    leadCountsAll.set(type, (leadCountsAll.get(type) || 0) + 1);
  });
  
  const leadCountsFiltered = new Map();
  filteredRows.forEach(row => {
    const type = row.fraud_type || "Unknown";
    leadCountsFiltered.set(type, (leadCountsFiltered.get(type) || 0) + 1);
  });
  
  const powerCountsAll = buildCountMap(state.raw, "implicated_actors");
  const powerCountsFiltered = buildCountMap(filteredRows, "implicated_actors");

  const sortedLeads = sortValuesByCount(
    Array.from(leadCountsAll.keys()),
    leadCountsFiltered,
    leadCountsAll
  );
  const sortedPowers = sortValuesByCount(
    Array.from(powerCountsAll.keys()),
    powerCountsFiltered,
    powerCountsAll
  );

  setChoiceOptions(
    state.leadChoices,
    sortedLeads,
    Array.from(leadSelectedSet || []),
    null,
    leadCountsFiltered,
    leadCountsAll
  );
  setChoiceOptions(
    state.powerChoices,
    sortedPowers,
    powerSelectedRaw || [],
    powerKeywordMap,
    powerCountsFiltered,
    powerCountsAll
  );
}

function updateLeadChart() {
  const ctx = document.getElementById("leadChart").getContext("2d");
  
  const fraudCounts = new Map();
  state.filtered.forEach(row => {
    const type = row.fraud_type || "Unknown";
    fraudCounts.set(type, (fraudCounts.get(type) || 0) + 1);
  });
  const topTypes = Array.from(fraudCounts.entries())
    .map(([label, value]) => ({ label, value }))
    .sort((a, b) => b.value - a.value)
    .slice(0, 10);
  
  const labels = topTypes.map((item) => item.label);
  const values = topTypes.map((item) => item.value);

  if (state.leadChart) {
    state.leadChart.destroy();
  }

  state.leadChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Fraud cases",
          data: values,
          backgroundColor: "#5ad0ff",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: "y",
      layout: {
        padding: { top: 10, bottom: 20, left: 10, right: 10 },
      },
      plugins: {
        legend: { display: false },
      },
      scales: {
        x: { ticks: { color: "#c3d2e8" } },
        y: { ticks: { color: "#c3d2e8" } },
      },
    },
  });
}

function updateScoreChart() {
  const ctx = document.getElementById("scoreChart").getContext("2d");
  const buckets = Array(10).fill(0);
  state.filtered.forEach((row) => {
    const index = Math.min(9, Math.floor(row.qui_tam_score / 10));
    buckets[index] += 1;
  });
  const labels = buckets.map((_, idx) => `${idx * 10}-${idx * 10 + 9}`);
  labels[9] = "90-100";

  if (state.scoreChart) {
    state.scoreChart.destroy();
  }

  state.scoreChart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Providers",
          data: buckets,
          borderColor: "#ffb347",
          backgroundColor: "rgba(255, 179, 71, 0.25)",
          fill: true,
          tension: 0.3,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      layout: {
        padding: { top: 10, bottom: 20, left: 10, right: 10 },
      },
      plugins: {
        legend: { display: false },
      },
      scales: {
        x: { ticks: { color: "#c3d2e8" } },
        y: { ticks: { color: "#c3d2e8" } },
      },
    },
  });
}

function updatePowerChart() {
  const ctx = document.getElementById("powerChart").getContext("2d");
  const topPower = aggregateCounts(state.filtered, "implicated_actors").slice(0, 8);
  const labels = topPower.map((item) => item.label);
  const values = topPower.map((item) => item.value);

  if (state.powerChart) {
    state.powerChart.destroy();
  }

  state.powerChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Cases",
          data: values,
          backgroundColor: "rgba(255, 99, 132, 0.65)",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: "y",
      layout: {
        padding: { top: 10, bottom: 20, left: 10, right: 10 },
      },
      plugins: {
        legend: { display: false },
      },
      scales: {
        x: { ticks: { color: "#c3d2e8" } },
        y: { ticks: { color: "#c3d2e8" } },
      },
    },
  });
}

function updateAgencyChart() {
  const ctx = document.getElementById("agencyChart").getContext("2d");
  const topAgencies = aggregateCounts(state.filtered, "federal_programs_involved").slice(0, 8);
  const labels = topAgencies.map((item) => item.label);
  const values = topAgencies.map((item) => item.value);

  if (state.agencyChart) {
    state.agencyChart.destroy();
  }

  state.agencyChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Programs",
          data: values,
          backgroundColor: "rgba(153, 102, 255, 0.7)",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: "y",
      layout: {
        padding: { top: 10, bottom: 20, left: 10, right: 10 },
      },
      plugins: {
        legend: { display: false },
      },
      scales: {
        x: { ticks: { color: "#c3d2e8" } },
        y: { ticks: { color: "#c3d2e8" } },
      },
    },
  });
}

// FIXED: Update loadData to use correct file path
async function loadData() {
  const loadId = Date.now();
  state.currentLoadId = loadId;
  resetLoadingState("Loading Medical Records…", "Fetching data");
  
  try {
    // Try manifest first (for chunked mode)
    const manifest = await fetchManifest();
    if (manifest && manifest.chunks && manifest.chunks.length > 0) {
      await loadChunks(manifest, loadId);
      finishInitialLoadingUI();
      return;
    }
  } catch (err) {
    console.warn("Chunk manifest unavailable, trying single file.", err);
  }

  // Fallback to single file (your case with chunk-size=0)
  try {
    await loadSingleFile(loadId);
    finishInitialLoadingUI();
    setFiltersEnabled(true, { triggerApply: true });
  } catch (error) {
    console.error("Failed to load data", error);
    finishInitialLoadingUI();
    hideInlineLoader();
    alert(
      "Unable to load ranked outputs. Ensure data/results/qui_tam_ranked.jsonl exists.\n\n" +
      "Run: python gpt_ranker.py --chunk-size 0"
    );
  }
}

async function fetchManifest() {
  try {
    const response = await fetch(`${CHUNK_MANIFEST_URL}?t=${Date.now()}`);
    if (!response.ok) return null;
    const data = await response.json();

    if (Array.isArray(data)) {
      return { chunks: data, metadata: null };
    } else if (data.chunks && Array.isArray(data.chunks)) {
      return { chunks: data.chunks, metadata: data.metadata || null };
    }

    return null;
  } catch (error) {
    return null;
  }
}

async function loadChunks(manifestData, loadId, initialChunkCount = 2) {
  const chunks = manifestData.chunks || manifestData;
  const metadata = manifestData.metadata || null;

  if (!Array.isArray(chunks) || chunks.length === 0) {
    throw new Error("Chunk manifest contained no readable data.");
  }

  state.manifestMetadata = metadata;
  state.loading.totalChunks = chunks.length;
  state.loading.loadedChunks = 0;
  updateLoadingProgress(0, chunks.length, "Fetching initial files…");

  const initialRows = [];
  const initialBatch = chunks.slice(0, Math.min(initialChunkCount, chunks.length));

  for (const entry of initialBatch) {
    const rows = await fetchChunkEntry(entry, loadId);
    if (loadId !== state.currentLoadId) return;
    if (rows.length > 0) {
      initialRows.push(...rows);
    }
    state.loading.loadedChunks += 1;
    updateLoadingProgress(state.loading.loadedChunks, state.loading.totalChunks, "Loading top-ranked files…");
  }

  if (initialRows.length === 0) {
    throw new Error("Chunk manifest contained no readable data.");
  }

  await hydrateRows(initialRows, { append: false });

  if (chunks.length <= initialBatch.length) {
    hideInlineLoader();
    setFiltersEnabled(true, { triggerApply: true });
    return;
  }

  showInlineLoader("Loading remaining files…");
  const remainingChunks = chunks.slice(initialBatch.length);
  backgroundLoadChunks(remainingChunks, loadId).catch((error) => {
    console.warn("Background chunk load failed", error);
    hideInlineLoader();
  });
}

async function backgroundLoadChunks(chunks, loadId, concurrency = 8) {
  if (!Array.isArray(chunks) || chunks.length === 0) {
    setFiltersEnabled(true, { triggerApply: true });
    return;
  }

  let index = 0;
  const worker = async () => {
    while (index < chunks.length) {
      const current = index;
      index += 1;
      const entry = chunks[current];
      if (loadId !== state.currentLoadId) return;
      try {
        const rows = await fetchChunkEntry(entry, loadId);
        if (rows.length > 0 && loadId === state.currentLoadId) {
          await hydrateRows(rows, { append: true, preserveFilters: true });
        }
      } finally {
        state.loading.loadedChunks += 1;
        updateLoadingProgress(
          state.loading.loadedChunks,
          state.loading.totalChunks,
          "Loading remaining files…"
        );
      }
    }
  };

  const workers = Array.from({ length: Math.min(concurrency, chunks.length) }, () => worker());
  await Promise.all(workers);

  if (loadId === state.currentLoadId) {
    hideInlineLoader();
    setFiltersEnabled(true, { triggerApply: true });
  }
}

async function loadSingleFile(loadId = state.currentLoadId) {
  const response = await fetch(`${DATA_URL}?t=${Date.now()}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch data: ${response.status} from ${DATA_URL}`);
  }
  const text = await response.text();
  const parsed = parseJsonl(text);
  if (loadId !== state.currentLoadId) return;
  updateLoadingProgress(1, 1, "Loaded ranked file");
  await hydrateRows(parsed, { append: false });
}

async function fetchChunkEntry(entry, loadId) {
  const jsonPath = resolveChunkPath(entry?.json);
  if (!jsonPath) return [];
  try {
    const response = await fetch(`${jsonPath}?t=${Date.now()}`);
    if (!response.ok) {
      console.warn("Failed to fetch chunk", jsonPath);
      return [];
    }
    const text = await response.text();
    if (loadId !== state.currentLoadId) {
      return [];
    }
    return parseJsonl(text);
  } catch (error) {
    console.warn("Chunk load error", jsonPath, error);
    return [];
  }
}

async function hydrateRows(rows, { append = false, preserveFilters = false } = {}) {
  const normalized = rows.map(normalizeRow);
  state.raw = append ? state.raw.concat(normalized) : normalized;
  state.lastUpdated = new Date();
  populateFilters(state.raw, preserveFilters);
  applyFilters({ force: true });
}

function resolveChunkPath(path) {
  if (!path) return null;
  if (path.startsWith("http://") || path.startsWith("https://")) {
    return path;
  }
  if (path.startsWith("../")) {
    path = path.substring(3);
  }
  if (path.startsWith("./")) {
    path = path.substring(2);
  }
  if (!path.startsWith("/")) {
    return "/" + path;
  }
  return path;
}

function resetFilters() {
  elements.scoreFilter.value = 0;
  elements.scoreValue.textContent = "0";
  state.leadChoices?.removeActiveItems();
  state.powerChoices?.removeActiveItems();
  elements.searchInput.value = "";
  elements.limitInput.value = "";
  applyFilters({ force: true });
}

function runScript(scriptName, options = {}) {
  const btn = options.button || document.querySelector(`.script-btn[data-script="${scriptName}"]`);
  if (!elements.scriptOutputModal || !elements.scriptOutputPre) return;
  const label = btn ? btn.textContent : scriptName;
  if (btn) {
    btn.disabled = true;
    btn.textContent = "Running…";
  }
  elements.scriptOutputModal.classList.remove("hidden");
  if (elements.scriptOutputTitle) elements.scriptOutputTitle.textContent = `Output: ${label}`;
  elements.scriptOutputPre.textContent = "Running…";
  // Enable row Stop button (it's always visible, just disabled)
  if (elements.scriptOutputStop) {
    elements.scriptOutputStop.disabled = false;
  }
  // Show and enable modal Stop button
  if (elements.scriptOutputStopModal) {
    elements.scriptOutputStopModal.classList.remove("hidden");
    elements.scriptOutputStopModal.disabled = false;
  }
  if (elements.deleteDataBtn) elements.deleteDataBtn.disabled = true;
  elements.scriptOutputModal.scrollIntoView({ behavior: "smooth", block: "nearest" });

  const fetchOptions = {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  };
  if (options.body) {
    fetchOptions.body = JSON.stringify(options.body);
  }

  fetch(`/api/run/${scriptName}`, fetchOptions)
    .then(async (res) => {
      const contentType = res.headers.get("content-type");
      if (!contentType || !contentType.includes("application/json")) {
        const text = await res.text();
        throw new Error(`Server returned ${res.status} ${res.statusText}. Response: ${text.substring(0, 200)}`);
      }
      return res.json();
    })
    .then((data) => {
      if (data.ok) {
        let out = data.stdout || "";
        if (data.stderr) out += (out ? "\n" : "") + "[stderr]\n" + data.stderr;
        if (data.returncode !== 0) out += `\n[exit code ${data.returncode}]`;
        if (data.stopped || data.returncode === -9 || data.returncode === 137) {
          out += "\n[Stopped by user]";
        } else if ((scriptName === "gpt_ranker" || scriptName === "converter") && data.returncode === 0) {
          out += "\n\n→ Refresh the page to load new results.";
        }
        elements.scriptOutputPre.textContent = out || "(no output)";
      } else {
        elements.scriptOutputPre.textContent = "Error: " + (data.error || "Unknown error");
      }
    })
    .catch((err) => {
      elements.scriptOutputPre.textContent = "Request failed: " + (err.message || String(err));
    })
    .finally(() => {
      if (btn) {
        btn.textContent = label;
        btn.disabled = false;
      }
      // Disable row Stop button
      if (elements.scriptOutputStop) {
        elements.scriptOutputStop.disabled = true;
      }
      // Hide and disable modal Stop button
      if (elements.scriptOutputStopModal) {
        elements.scriptOutputStopModal.classList.add("hidden");
        elements.scriptOutputStopModal.disabled = true;
      }
      if (elements.deleteDataBtn) elements.deleteDataBtn.disabled = false;
    });
}

function scrapeWebsite() {
  const url = elements.websiteUrl?.value?.trim();
  if (!url) {
    alert("Please enter a website URL");
    return;
  }
  
  // Validate URL
  try {
    new URL(url);
  } catch {
    alert("Please enter a valid URL (e.g., https://example.com)");
    return;
  }
  
  const maxPages = parseInt(elements.maxPages?.value || "10", 10) || 10;
  const urlPattern = elements.urlPattern?.value?.trim() || null;
  const linkSelector = elements.linkSelector?.value?.trim() || null;
  
  if (elements.scrapeWebsiteBtn) {
    elements.scrapeWebsiteBtn.disabled = true;
  }
  
  runScript("website_scraper", {
    button: elements.scrapeWebsiteBtn,
    body: { 
      url, 
      max_pages: maxPages,
      url_pattern: urlPattern,
      link_selector: linkSelector
    }
  });
}

function deleteData() {
  if (!confirm("This will permanently delete all data in the data folder. Continue?")) return;
  if (elements.deleteDataBtn) elements.deleteDataBtn.disabled = true;
  fetch("/api/delete-data", { method: "POST" })
    .then((res) => res.json())
    .then((data) => {
      if (data.ok) {
        state.raw = [];
        state.filtered = [];
        updateSummary();
        updateCharts();
        populateFilters([], false);
        applyFilters({ force: true });
        alert("Data cleared. Run the pipeline to regenerate.");
      } else {
        alert("Error: " + (data.error || "Unknown error"));
      }
    })
    .catch((err) => alert("Request failed: " + (err.message || String(err))))
    .finally(() => {
      if (elements.deleteDataBtn) elements.deleteDataBtn.disabled = false;
    });
}

function stopScript(e) {
  if (e) {
    e.preventDefault();
    e.stopPropagation();
  }
  console.log("stopScript called", { 
    rowStop: elements.scriptOutputStop, 
    modalStop: elements.scriptOutputStopModal 
  });
  for (const el of [elements.scriptOutputStop, elements.scriptOutputStopModal]) {
    if (el) el.disabled = true;
  }
  if (elements.scriptOutputPre) elements.scriptOutputPre.textContent = "Stopping…";
  fetch("/api/stop", { method: "POST" })
    .then((res) => res.json())
    .then((data) => {
      if (data.ok) {
        if (elements.scriptOutputPre) {
          const current = elements.scriptOutputPre.textContent;
          elements.scriptOutputPre.textContent = current + "\n\n[Stop requested - waiting for process to terminate...]";
        }
      } else {
        if (elements.scriptOutputPre) {
          elements.scriptOutputPre.textContent = "Error stopping: " + (data.error || "Unknown error");
        }
      }
    })
    .catch((err) => {
      console.error("Stop request failed:", err);
      if (elements.scriptOutputPre) {
        elements.scriptOutputPre.textContent = "Stop request failed: " + (err.message || String(err));
      }
    })
    .finally(() => {
      // Re-enable after a short delay to allow the process to terminate
      setTimeout(() => {
        for (const el of [elements.scriptOutputStop, elements.scriptOutputStopModal]) {
          if (el) el.disabled = false;
        }
      }, 500);
    });
}

function wireEvents() {
  ["change", "input"].forEach((eventName) => {
    elements.scoreFilter.addEventListener(eventName, applyFilters);
    elements.searchInput.addEventListener(eventName, debounce(applyFilters, 200));
    elements.limitInput.addEventListener(eventName, debounce(applyFilters, 200));
  });
  elements.leadFilter.addEventListener("change", applyFilters);
  elements.powerFilter.addEventListener("change", applyFilters);
  elements.resetFilters.addEventListener("click", resetFilters);
  elements.detailClose.addEventListener("click", () => clearDetail());
  elements.detailTextToggle.addEventListener("click", toggleDetailText);

  if (elements.scriptOutputClose) {
    elements.scriptOutputClose.addEventListener("click", () => {
      elements.scriptOutputModal?.classList.add("hidden");
    });
  }
  if (elements.scriptOutputStop) {
    elements.scriptOutputStop.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      console.log("Row Stop button clicked");
      stopScript(e);
    });
  }
  if (elements.scriptOutputStopModal) {
    elements.scriptOutputStopModal.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      console.log("Modal Stop button clicked");
      stopScript(e);
    });
  }
  if (elements.deleteDataBtn) {
    elements.deleteDataBtn.addEventListener("click", deleteData);
  }
  if (elements.scrapeWebsiteBtn) {
    elements.scrapeWebsiteBtn.addEventListener("click", scrapeWebsite);
  }
  document.querySelectorAll(".script-btn[data-script]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const s = btn.dataset.script;
      if (s) runScript(s);
    });
  });
}

function toggleDetailText() {
  const isExpanded = !elements.detailText.classList.contains("hidden");
  if (isExpanded) {
    elements.detailText.classList.add("hidden");
    elements.detailTextPreview.classList.remove("hidden");
    elements.detailTextToggle.textContent = "Expand";
  } else {
    elements.detailText.classList.remove("hidden");
    elements.detailTextPreview.classList.add("hidden");
    elements.detailTextToggle.textContent = "Collapse";
  }
}

function debounce(fn, delay) {
  let timer;
  return (...args) => {
    window.clearTimeout(timer);
    timer = window.setTimeout(() => fn(...args), delay);
  };
}

document.addEventListener("DOMContentLoaded", () => {
  initGrid();
  initChoices();
  wireEvents();
  loadData();
});

function initChoices() {
  const frequencySorter = (a, b) => {
    const countA = a.customProperties?.count ?? 0;
    const countB = b.customProperties?.count ?? 0;
    if (countA !== countB) return countB - countA;
    const baseA = a.customProperties?.baseCount ?? 0;
    const baseB = b.customProperties?.baseCount ?? 0;
    if (baseA !== baseB) return baseB - baseA;
    const indexA = a.customProperties?.originalIndex ?? 9999;
    const indexB = b.customProperties?.originalIndex ?? 9999;
    return indexA - indexB;
  };

  state.leadChoices = new Choices(elements.leadFilter, {
    removeItemButton: true,
    placeholder: true,
    placeholderValue: "Select fraud types…",
    searchPlaceholderValue: "Search…",
    shouldSort: true,
    sorter: frequencySorter,
    searchResultLimit: 500,
    renderChoiceLimit: 500,
    fuseOptions: {
      keys: ["label", "value", "customProperties.keywords"],
      threshold: 0.3,
      ignoreLocation: true,
      shouldSort: false,
    },
  });
  
  state.powerChoices = new Choices(elements.powerFilter, {
    removeItemButton: true,
    placeholder: true,
    placeholderValue: "Select providers…",
    searchPlaceholderValue: "Search…",
    shouldSort: true,
    sorter: frequencySorter,
    searchResultLimit: 500,
    renderChoiceLimit: 500,
    fuseOptions: {
      keys: ["label", "value", "customProperties.keywords"],
      threshold: 0.3,
      ignoreLocation: true,
      shouldSort: false,
    },
  });
}

function escapeHtml(str) {
  return (str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function getHighlightTerms() {
  const terms = [];
  const searchTerm = elements.searchInput?.value?.trim();
  if (searchTerm) terms.push(searchTerm);
  
  const selectedPowers = getSelectedValues(state.powerChoices);
  selectedPowers.forEach(power => {
    if (power) {
      terms.push(power);
      const words = power.split(/\s+/).filter(w => w.length > 2);
      terms.push(...words);
    }
  });
  
  const selectedLeads = getSelectedValues(state.leadChoices);
  selectedLeads.forEach(lead => {
    if (lead) terms.push(lead);
  });
  
  return terms.filter(Boolean);
}

function highlightText(text, terms) {
  if (!text || !terms || terms.length === 0) {
    return escapeHtml(text);
  }

  let result = escapeHtml(text);
  const sortedTerms = [...new Set(terms)].sort((a, b) => b.length - a.length);
  const matches = [];

  sortedTerms.forEach(term => {
    const lowerText = text.toLowerCase();
    const lowerTerm = term.toLowerCase();
    let searchPos = 0;

    while (true) {
      const match = lowerText.indexOf(lowerTerm, searchPos);
      if (match === -1) break;
      
      matches.push({
        start: match,
        end: match + term.length,
        term: text.substring(match, match + term.length)
      });
      searchPos = match + 1;
    }
  });

  matches.sort((a, b) => a.start - b.start);

  const filteredMatches = [];
  let lastEnd = -1;
  matches.forEach(match => {
    if (match.start >= lastEnd) {
      filteredMatches.push(match);
      lastEnd = match.end;
    }
  });

  if (filteredMatches.length === 0) return result;

  let highlighted = '';
  let lastIndex = 0;

  filteredMatches.forEach(match => {
    highlighted += escapeHtml(text.substring(lastIndex, match.start));
    highlighted += '<mark class="highlight">' + escapeHtml(match.term) + '</mark>';
    lastIndex = match.end;
  });

  highlighted += escapeHtml(text.substring(lastIndex));
  return highlighted;
}

function renderDetail(row, options = {}) {
  if (!row) {
    clearDetail();
    return;
  }
  
  state.activeRowId = row.filename || null;
  elements.detailDrawer.classList.remove("hidden");

  const highlightTerms = getHighlightTerms();

  const headlineText = `${row.headline || row.filename} (${row.filename})`;
  elements.detailTitle.innerHTML = highlightText(headlineText, highlightTerms);
  elements.detailReason.innerHTML = highlightText(row.reason || "—", highlightTerms);
  elements.detailLeadTypes.innerHTML = highlightText(row.fraud_type || "—", highlightTerms);
  elements.detailPower.innerHTML = highlightText(row.implicated_actors.join(", ") || "—", highlightTerms);
  elements.detailAgencies.innerHTML = highlightText(row.federal_programs_involved.join(", ") || "—", highlightTerms);
  elements.detailTags.innerHTML = highlightText(row.statute_violations.join(", ") || "—", highlightTerms);

  const model = row.metadata?.config?.model || "—";
  elements.detailModel.textContent = model;

  const originalText = row.original_text || "No source text captured.";
  const wordCount = originalText.split(/\s+/).filter(Boolean).length;
  const snippet = originalText.split(/\s+/).slice(0, 30).join(" ");

  const highlightedText = highlightText(originalText, highlightTerms);
  const highlightedSnippet = highlightText(snippet, highlightTerms);

  elements.detailText.innerHTML = highlightedText;
  elements.detailTextPreview.innerHTML = `${highlightedSnippet}... (${wordCount.toLocaleString()} words)`;

  elements.detailText.classList.add("hidden");
  elements.detailTextPreview.classList.remove("hidden");
  elements.detailTextToggle.textContent = "Expand";

  elements.detailInsights.innerHTML =
    row.key_facts.length > 0
      ? row.key_facts.map((item) => `<li>${highlightText(item, highlightTerms)}</li>`).join("")
      : "<li>—</li>";

  if (options.scrollToDetail) {
    setTimeout(() => {
      elements.detailDrawer.scrollIntoView({ behavior: "smooth", block: "start" });
    }, 100);
  }
}

function clearDetail() {
  elements.detailDrawer.classList.add("hidden");
  state.activeRowId = null;
  elements.detailTitle.innerHTML = "Select a row to inspect full context";
  elements.detailReason.innerHTML = "—";
  elements.detailLeadTypes.innerHTML = "—";
  elements.detailPower.innerHTML = "—";
  elements.detailAgencies.innerHTML = "—";
  elements.detailTags.innerHTML = "—";
  elements.detailModel.textContent = "—";
  elements.detailText.innerHTML = "—";
  elements.detailTextPreview.innerHTML = "—";
  elements.detailText.classList.add("hidden");
  elements.detailTextPreview.classList.remove("hidden");
  elements.detailTextToggle.textContent = "Expand";
  elements.detailInsights.innerHTML = "";
}