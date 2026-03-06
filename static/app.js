const state = {
  limit: 50,
  offset: 0,
  total: 0,
  detail: null,
  detailEntries: [],
  jsonPathsLoaded: false,
  commPresence: {},
  flashPresence: {},
  phoneByRow: {},
};

const el = {
  healthDot: document.getElementById("health-dot"),
  healthText: document.getElementById("health-text"),
  indexText: document.getElementById("index-text"),
  statTotal: document.getElementById("stat-total"),
  statActive: document.getElementById("stat-active"),
  statInactive: document.getElementById("stat-inactive"),
  statLatest: document.getElementById("stat-latest"),
  rows: document.getElementById("rows"),
  resultMeta: document.getElementById("result-meta"),
  pageIndicator: document.getElementById("page-indicator"),
  prev: document.getElementById("prev"),
  next: document.getElementById("next"),
  apply: document.getElementById("apply"),
  reset: document.getElementById("reset"),
  q: document.getElementById("q"),
  jsonPath: document.getElementById("json_path"),
  jsonValue: document.getElementById("json_value"),
  jsonOp: document.getElementById("json_op"),
  jsonPaths: document.getElementById("json-paths"),
  type: document.getElementById("type"),
  status: document.getElementById("status"),
  country: document.getElementById("country"),
  active: document.getElementById("active"),
  sort: document.getElementById("sort"),
  uniqueIDs: document.getElementById("unique_ids"),
  limit: document.getElementById("limit"),
  advancedJSON: document.getElementById("advanced-json"),
  quickAll: document.getElementById("quick-all"),
  quickActive: document.getElementById("quick-active"),
  quickInactive: document.getElementById("quick-inactive"),
  quickClearJSON: document.getElementById("quick-clear-json"),
  detailDialog: document.getElementById("detail-dialog"),
  detailTitle: document.getElementById("detail-title"),
  detailJSON: document.getElementById("detail-json"),
  tabComm: document.getElementById("tab-comm"),
  tabFlash: document.getElementById("tab-flash"),
  tabJSON: document.getElementById("tab-json"),
  commFilterWrap: document.getElementById("comm-filter-wrap"),
  commFilter: document.getElementById("comm-filter"),
  commTimeline: document.getElementById("comm-timeline"),
  commEmpty: document.getElementById("comm-empty"),
  flashEmpty: document.getElementById("flash-empty"),
  flashMessage: document.getElementById("flash-message"),
  sortableHeaders: Array.from(document.querySelectorAll("th.sortable[data-sort]")),
};

let inputTimer = null;
let recordsAbort = null;

function number(v) {
  return new Intl.NumberFormat().format(v ?? 0);
}

function escapeHTML(text) {
  return String(text ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function hasValue(v) {
  return String(v ?? "").trim() !== "";
}

function parseBool(v) {
  return String(v ?? "").toLowerCase() === "true";
}

function htmlToReadableText(raw) {
  const html = String(raw || "").trim();
  if (!html) return "";
  const normalized = html
    .replace(/<\s*br\s*\/?>/gi, "\n")
    .replace(/<\s*\/p\s*>/gi, "\n\n")
    .replace(/<\s*\/div\s*>/gi, "\n");
  const doc = new DOMParser().parseFromString(normalized, "text/html");
  return (doc.body?.textContent || "").replace(/\n{3,}/g, "\n\n").trim();
}

async function api(path, options = {}) {
  const res = await fetch(path, options);
  if (!res.ok) {
    const payload = await res.json().catch(() => ({}));
    throw new Error(payload.error || `HTTP ${res.status}`);
  }
  return res.json();
}

function queryString() {
  const p = new URLSearchParams();
  const fields = ["q", "type", "status", "country", "active", "sort"];
  for (const field of fields) {
    const value = (el[field]?.value || "").trim();
    if (value) p.set(field, value);
  }

  const jsonPath = el.jsonPath.value.trim();
  const jsonValue = el.jsonValue.value.trim();
  if (jsonPath) p.set("json_path", jsonPath);
  if (jsonValue) p.set("json_value", jsonValue);
  if (jsonPath || jsonValue) p.set("json_op", el.jsonOp.value || "eq");

  if (el.uniqueIDs.checked) p.set("unique_ids", "true");
  p.set("limit", String(state.limit));
  p.set("offset", String(state.offset));
  return p.toString();
}

function updateURLState() {
  const p = new URLSearchParams(queryString());
  history.replaceState(null, "", `${location.pathname}?${p.toString()}`);
}

function setSelectOptions(select, values) {
  const existing = new Set(Array.from(select.options).map((o) => o.value));
  for (const value of values) {
    if (existing.has(value)) continue;
    const opt = document.createElement("option");
    opt.value = value;
    opt.textContent = value;
    select.appendChild(opt);
  }
}

function restoreStateFromURL() {
  const p = new URLSearchParams(location.search);
  const get = (k) => p.get(k) || "";

  el.q.value = get("q");
  el.type.value = get("type");
  el.status.value = get("status");
  el.country.value = get("country");
  el.active.value = get("active");
  el.sort.value = get("sort");
  el.jsonPath.value = get("json_path");
  el.jsonValue.value = get("json_value");
  el.jsonOp.value = get("json_op") || "eq";
  el.uniqueIDs.checked = parseBool(get("unique_ids"));

  const limit = Number(p.get("limit")) || 50;
  state.limit = Math.min(200, Math.max(25, limit));
  const allowed = ["25", "50", "100", "200"];
  el.limit.value = allowed.includes(String(state.limit)) ? String(state.limit) : "50";
  state.limit = Number(el.limit.value);

  const offset = Number(p.get("offset")) || 0;
  state.offset = Math.max(0, offset);

  if (hasValue(el.jsonPath.value) || hasValue(el.jsonValue.value)) {
    el.advancedJSON.open = true;
  }
}

async function loadHealth() {
  try {
    const data = await api("/api/health");
    if (data.ready) {
      el.healthDot.classList.add("ok");
      el.healthText.textContent = `ready - ${number(data.rows)} indexed`;
    } else {
      el.healthDot.classList.remove("ok");
      el.healthText.textContent = `backend indexing - ${number(data.rows)} rows`;
    }
  } catch (err) {
    el.healthDot.classList.remove("ok");
    el.healthText.textContent = `backend error: ${err.message}`;
  }
}

async function loadIndexStatus() {
  try {
    const status = await api("/api/index/status");
    if (status.indexing) {
      const rows = number(status.rows_indexed || 0);
      const errs = number(status.parse_errors || 0);
      const step = status.step || "indexing";
      el.indexText.textContent = `index: ${step} | rows ${rows} | errors ${errs}`;
      return;
    }
    el.indexText.textContent = `index: ${status.step || "ready"}`;
  } catch (err) {
    el.indexText.textContent = `index status error: ${err.message}`;
  }
}

async function loadStats() {
  try {
    const stats = await api("/api/stats");
    el.statTotal.textContent = number(stats.total_rows);
    el.statActive.textContent = number(stats.active_rows);
    el.statInactive.textContent = number(stats.inactive_rows);
    el.statLatest.textContent = stats.last_modified_at || "-";
  } catch (err) {
    el.resultMeta.textContent = `stats failed: ${err.message}`;
  }
}

async function loadFacets() {
  try {
    const facets = await api("/api/facets?limit=160");
    setSelectOptions(el.type, facets.types || []);
    setSelectOptions(el.status, facets.statuses || []);
    setSelectOptions(el.country, facets.countries || []);
  } catch (err) {
    el.resultMeta.textContent = `facets failed: ${err.message}`;
  }
}

async function loadJSONPaths() {
  try {
    const payload = await api("/api/json/paths?limit=300");
    const paths = payload.paths || [];
    el.jsonPaths.innerHTML = "";
    for (const path of paths) {
      const opt = document.createElement("option");
      opt.value = path;
      el.jsonPaths.appendChild(opt);
    }
    state.jsonPathsLoaded = true;
  } catch {
    // optional helper only
  }
}

function updateSortIndicators() {
  const current = el.sort.value;
  for (const th of el.sortableHeaders) {
    th.classList.remove("sorted", "asc", "desc");
    const base = th.dataset.sort;
    if (!base) continue;
    const desc = base.replace("_asc", "_desc");
    if (current === base) {
      th.classList.add("sorted", "asc");
    } else if (current === desc || (base === "modified_asc" && current === "")) {
      th.classList.add("sorted", "desc");
    }
  }
}

function renderRows(rows) {
  if (!rows.length) {
    el.rows.innerHTML = `<tr><td colspan="10">No matches</td></tr>`;
    return;
  }

  el.rows.innerHTML = rows
    .map((r) => {
      const known = Object.prototype.hasOwnProperty.call(state.commPresence, String(r.row_num));
      const hasComm = known ? !!state.commPresence[String(r.row_num)] : !!r.has_sobject_log;
      const flashKnown = Object.prototype.hasOwnProperty.call(state.flashPresence, String(r.row_num));
      const hasFlash = flashKnown ? !!state.flashPresence[String(r.row_num)] : !!r.has_flash_message;
      const phoneKnown = Object.prototype.hasOwnProperty.call(state.phoneByRow, String(r.row_num));
      const phone = phoneKnown ? state.phoneByRow[String(r.row_num)] : (r.phone || "");
      const commBtn = hasComm
        ? `<button class="ghost mini row-action comm-action" data-row="${r.row_num}" data-action="comm">Communicatie</button>`
        : "";
      const flashBtn = hasFlash
        ? `<button class="ghost mini row-action flash-action" data-row="${r.row_num}" data-action="flash">Opmerkingen</button>`
        : "";
      return `
      <tr data-row="${r.row_num}">
        <td>${number(r.row_num)}</td>
        <td>${escapeHTML(r.id)}</td>
        <td>${escapeHTML(r.name)}</td>
        <td class="phone-cell">${escapeHTML(phone)}</td>
        <td>${escapeHTML(r.type)}</td>
        <td>${escapeHTML(r.status)}</td>
        <td>${escapeHTML(r.billing_city)}</td>
        <td>${escapeHTML(r.billing_country)}</td>
        <td>${escapeHTML(r.modified_date)}</td>
        <td>
          <button class="ghost mini row-action" data-row="${r.row_num}" data-action="detail">Details</button>
          ${commBtn}
          ${flashBtn}
        </td>
      </tr>`;
    })
    .join("");
}

async function refreshCommPresence(rows) {
  const rowNums = rows.map((r) => r.row_num).filter((n) => Number.isFinite(n));
  if (!rowNums.length) return;

  try {
    const payload = await api(`/api/records/comm?rows=${rowNums.join(",")}`);
    const map = payload?.rows || {};
    for (const [row, has] of Object.entries(map)) {
      state.commPresence[row] = !!has;
      const tr = el.rows.querySelector(`tr[data-row="${row}"]`);
      if (!tr) continue;
      const actionCell = tr.children[9];
      if (!actionCell) continue;
      const existing = actionCell.querySelector(".comm-action");
      if (has && !existing) {
        actionCell.insertAdjacentHTML(
          "beforeend",
          ` <button class="ghost mini row-action comm-action" data-row="${row}" data-action="comm">Communicatie</button>`
        );
      }
      if (!has && existing) {
        existing.remove();
      }
    }
  } catch {
    // Optional enhancement; details button still works.
  }
}

async function refreshFlashPresence(rows) {
  const rowNums = rows.map((r) => r.row_num).filter((n) => Number.isFinite(n));
  if (!rowNums.length) return;

  try {
    const payload = await api(`/api/records/flash?rows=${rowNums.join(",")}`);
    const map = payload?.rows || {};
    for (const [row, has] of Object.entries(map)) {
      state.flashPresence[row] = !!has;
      const tr = el.rows.querySelector(`tr[data-row="${row}"]`);
      if (!tr) continue;
      const actionCell = tr.children[9];
      if (!actionCell) continue;
      const existing = actionCell.querySelector(".flash-action");
      if (has && !existing) {
        actionCell.insertAdjacentHTML(
          "beforeend",
          ` <button class="ghost mini row-action flash-action" data-row="${row}" data-action="flash">Opmerkingen</button>`
        );
      }
      if (!has && existing) {
        existing.remove();
      }
    }
  } catch {
    // Optional enhancement; details button still works.
  }
}

async function refreshPhones(rows) {
  const rowNums = rows.map((r) => r.row_num).filter((n) => Number.isFinite(n));
  if (!rowNums.length) return;

  try {
    const payload = await api(`/api/records/phones?rows=${rowNums.join(",")}`);
    const map = payload?.rows || {};
    for (const [row, phone] of Object.entries(map)) {
      const value = String(phone || "").trim();
      state.phoneByRow[row] = value;
      const tr = el.rows.querySelector(`tr[data-row="${row}"]`);
      if (!tr) continue;
      const cell = tr.querySelector(".phone-cell");
      if (!cell) continue;
      cell.textContent = value;
    }
  } catch {
    // Optional enhancement; base result still shown.
  }
}

function updatePagination() {
  const page = Math.floor(state.offset / state.limit) + 1;
  const totalPages = Math.max(1, Math.ceil(state.total / state.limit));
  el.pageIndicator.textContent = `Page ${page} / ${totalPages}`;
  el.prev.disabled = state.offset <= 0;
  el.next.disabled = state.offset + state.limit >= state.total;
}

async function loadRecords() {
  if (recordsAbort) {
    recordsAbort.abort();
  }
  recordsAbort = new AbortController();

  el.resultMeta.textContent = "Loading records...";
  updateSortIndicators();
  updateURLState();

  try {
    const result = await api(`/api/records?${queryString()}`, {
      signal: recordsAbort.signal,
    });
    state.total = result.total;
    renderRows(result.records || []);
    refreshPhones(result.records || []);
    refreshCommPresence(result.records || []);
    refreshFlashPresence(result.records || []);
    const start = state.total ? state.offset + 1 : 0;
    const end = Math.min(state.offset + state.limit, state.total);
    el.resultMeta.textContent = `${number(start)}-${number(end)} of ${number(state.total)}`;
    updatePagination();
  } catch (err) {
    if (err.name === "AbortError") {
      return;
    }
    el.resultMeta.textContent = `query failed: ${err.message}`;
    el.rows.innerHTML = `<tr><td colspan="10">Error loading data</td></tr>`;
  } finally {
    recordsAbort = null;
  }
}

function parseSObjectLog(raw) {
  const text = String(raw || "").trim();
  if (!text) return [];

  const chunks = text
    .split(/(?=\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2}\s+-\s+)/g)
    .map((s) => s.trim())
    .filter(Boolean);

  const entries = [];
  for (const line of chunks) {
    const m = line.match(/^(\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2})\s*-\s*([^\-]+?)\s*-\s*([^\-]*?)\s*-\s*(.*)$/);
    const message = (m ? m[4] : line).trim();
    const lower = message.toLowerCase();
    let channel = "other";
    if (lower.includes("email") || lower.includes("e-mail")) channel = "email";
    else if (lower.includes("sms")) channel = "sms";
    else if (lower.includes("call") || lower.includes("gebeld") || lower.includes("telefoon")) channel = "call";

    entries.push({
      at: m ? m[1] : "",
      actor: m ? m[2].trim() : "",
      direction: m ? m[3].trim() : "",
      message,
      channel,
    });
  }
  return entries;
}

function renderTimeline() {
  const filter = el.commFilter.value;
  const filtered = state.detailEntries.filter((e) => filter === "all" || e.channel === filter);

  if (!filtered.length) {
    el.commTimeline.innerHTML = "";
    el.commEmpty.style.display = "block";
    return;
  }

  el.commEmpty.style.display = "none";
  el.commTimeline.innerHTML = filtered
    .map(
      (e) => `
      <article class="timeline-item">
        <div class="timeline-head">
          <span class="timeline-at mono">${escapeHTML(e.at || "unknown")}</span>
          <span class="timeline-channel timeline-${escapeHTML(e.channel)}">${escapeHTML(e.channel.toUpperCase())}</span>
        </div>
        <div class="timeline-meta">${escapeHTML([e.actor, e.direction].filter(Boolean).join(" · "))}</div>
        <div class="timeline-body">${escapeHTML(e.message)}</div>
      </article>`
    )
    .join("");
}

function setDetailTab(tab) {
  const comm = tab === "comm";
  const flash = tab === "flash";
  const json = tab === "json";

  el.tabComm.classList.toggle("active", comm);
  el.tabFlash.classList.toggle("active", flash);
  el.tabJSON.classList.toggle("active", json);

  el.commFilterWrap.style.display = comm ? "inline-flex" : "none";
  el.commTimeline.style.display = comm ? "grid" : "none";
  el.commEmpty.style.display = comm ? el.commEmpty.style.display : "none";

  el.flashMessage.style.display = flash ? "block" : "none";
  el.flashEmpty.style.display = flash ? el.flashEmpty.style.display : "none";

  el.detailJSON.style.display = json ? "block" : "none";
  if (comm) renderTimeline();
}

async function openDetail(rowNum, preferredTab = "json") {
  try {
    const detail = await api(`/api/records/${rowNum}`);
    state.detail = detail;
    state.detailEntries = parseSObjectLog(detail?.data?.SObjectLog__c);
    const flashRaw = String(detail?.data?.Flash_Message__c || "").trim();
    const flashText = htmlToReadableText(flashRaw);

    el.detailTitle.textContent = `Record ${number(detail.row_num)}`;
    el.detailJSON.textContent = JSON.stringify(detail.data, null, 2);
    el.commFilter.value = "all";
    el.flashMessage.textContent = flashText || "";
    el.tabFlash.style.display = flashText ? "inline-block" : "none";
    el.flashEmpty.style.display = flashText ? "none" : "block";

    if (typeof el.detailDialog.showModal === "function") {
      el.detailDialog.showModal();
    }

    if (preferredTab === "comm" && state.detailEntries.length > 0) {
      setDetailTab("comm");
      return;
    }
    if (preferredTab === "flash" && flashText) {
      setDetailTab("flash");
      return;
    }
    setDetailTab("json");
  } catch (err) {
    el.resultMeta.textContent = `detail failed: ${err.message}`;
  }
}

function resetFilters() {
  el.q.value = "";
  el.jsonPath.value = "";
  el.jsonValue.value = "";
  el.jsonOp.value = "eq";
  el.type.value = "";
  el.status.value = "";
  el.country.value = "";
  el.active.value = "";
  el.sort.value = "";
  el.uniqueIDs.checked = false;
}

function runQueryFromStart() {
  state.offset = 0;
  loadRecords();
}

function scheduleRun(delay = 250) {
  clearTimeout(inputTimer);
  inputTimer = setTimeout(() => runQueryFromStart(), delay);
}

function toggleSort(baseSort) {
  const current = el.sort.value;
  const desc = baseSort.replace("_asc", "_desc");

  if (baseSort === "modified_asc") {
    el.sort.value = current === "modified_asc" ? "" : "modified_asc";
    return;
  }
  if (current === baseSort) {
    el.sort.value = desc;
    return;
  }
  if (current === desc) {
    el.sort.value = "";
    return;
  }
  el.sort.value = baseSort;
}

el.apply.addEventListener("click", runQueryFromStart);

el.reset.addEventListener("click", () => {
  resetFilters();
  runQueryFromStart();
});

el.prev.addEventListener("click", () => {
  state.offset = Math.max(0, state.offset - state.limit);
  loadRecords();
});

el.next.addEventListener("click", () => {
  if (state.offset + state.limit < state.total) {
    state.offset += state.limit;
    loadRecords();
  }
});

el.rows.addEventListener("click", (evt) => {
  const actionBtn = evt.target.closest(".row-action[data-row][data-action]");
  if (!actionBtn) return;
  const rowNum = actionBtn.getAttribute("data-row");
  const action = actionBtn.getAttribute("data-action");
  if (!rowNum) return;

  if (action === "comm") openDetail(rowNum, "comm");
  else if (action === "flash") openDetail(rowNum, "flash");
  else openDetail(rowNum, "json");
});

el.tabComm.addEventListener("click", () => setDetailTab("comm"));
el.tabFlash.addEventListener("click", () => setDetailTab("flash"));
el.tabJSON.addEventListener("click", () => setDetailTab("json"));
el.commFilter.addEventListener("change", renderTimeline);

el.sortableHeaders.forEach((th) => {
  th.addEventListener("click", () => {
    const base = th.dataset.sort;
    if (!base) return;
    toggleSort(base);
    runQueryFromStart();
  });
});

for (const input of [el.q, el.jsonPath, el.jsonValue]) {
  input.addEventListener("keydown", (evt) => {
    if (evt.key === "Enter") runQueryFromStart();
  });
}

el.limit.addEventListener("change", () => {
  state.limit = Number(el.limit.value) || 50;
  runQueryFromStart();
});

for (const control of [el.type, el.status, el.country, el.active, el.sort, el.jsonOp, el.uniqueIDs]) {
  control.addEventListener("change", () => scheduleRun(80));
}

el.q.addEventListener("input", () => scheduleRun());
el.jsonPath.addEventListener("input", () => scheduleRun(350));
el.jsonValue.addEventListener("input", () => scheduleRun(350));

el.quickAll.addEventListener("click", () => {
  el.active.value = "";
  runQueryFromStart();
});

el.quickActive.addEventListener("click", () => {
  el.active.value = "true";
  runQueryFromStart();
});

el.quickInactive.addEventListener("click", () => {
  el.active.value = "false";
  runQueryFromStart();
});

el.quickClearJSON.addEventListener("click", () => {
  el.jsonPath.value = "";
  el.jsonValue.value = "";
  el.jsonOp.value = "eq";
  runQueryFromStart();
});

document.addEventListener("keydown", (evt) => {
  const tag = (evt.target?.tagName || "").toLowerCase();
  const typing = tag === "input" || tag === "textarea" || tag === "select";

  if (evt.key === "/" && !typing) {
    evt.preventDefault();
    el.q.focus();
    el.q.select();
    return;
  }

  if (typing) return;

  if (evt.key === "n" && !el.next.disabled) {
    evt.preventDefault();
    el.next.click();
  }
  if (evt.key === "p" && !el.prev.disabled) {
    evt.preventDefault();
    el.prev.click();
  }
});

el.advancedJSON?.addEventListener("toggle", () => {
  if (el.advancedJSON.open && !state.jsonPathsLoaded) {
    loadJSONPaths();
  }
});

async function boot() {
  restoreStateFromURL();
  await loadHealth();
  await loadIndexStatus();
  await loadStats();
  await loadFacets();
  if (el.advancedJSON.open) {
    await loadJSONPaths();
  }
  updateSortIndicators();
  await loadRecords();
  setInterval(loadHealth, 15000);
  setInterval(loadIndexStatus, 5000);
}

boot();
