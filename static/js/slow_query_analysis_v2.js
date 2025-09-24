(async function () {
  const summaryEndpoint = '/v2/slow-query/summary';
  const searchEndpoint = '/v2/slow-query/search';
  const detailEndpoint = (hash) => `/v2/slow-query/details/${hash}`;
  const authEndpoint = '/v2/auth/summary';
  const connectionEndpoint = '/v2/connections/activity';
  const manifestEndpoint = '/v2/manifest/ingests';

  async function fetchJson(url, params = {}) {
    const query = new URLSearchParams(params);
    const resp = await fetch(`${url}?${query.toString()}`, { cache: 'no-store' });
    if (!resp.ok) {
      throw new Error(`Request failed: ${resp.status}`);
    }
    return resp.json();
  }

  function setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
  }

  function renderList(targetId, items, renderItem) {
    const el = document.getElementById(targetId);
    if (!el) return;
    el.innerHTML = '';
    if (!items || items.length === 0) {
      el.innerHTML = '<li class="list-group-item text-muted">No data</li>';
      return;
    }
    items.forEach((item) => {
      const li = document.createElement('li');
      li.className = 'list-group-item d-flex justify-content-between align-items-center';
      li.innerHTML = renderItem(item);
      el.appendChild(li);
    });
  }

  function renderTableBody(tableId, rows, rowRenderer) {
    const table = document.querySelector(`#${tableId} tbody`);
    if (!table) return;
    table.innerHTML = '';
    if (!rows || rows.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 5;
      td.className = 'text-muted text-center';
      td.textContent = 'No data available';
      tr.appendChild(td);
      table.appendChild(tr);
      return;
    }
    rows.forEach((row) => {
      const tr = document.createElement('tr');
      tr.innerHTML = rowRenderer(row);
      table.appendChild(tr);
    });
  }

  async function loadSummary() {
    try {
      const [summary, auth, connections] = await Promise.all([
        fetchJson(summaryEndpoint, { limit: 50 }),
        fetchJson(authEndpoint),
        fetchJson(connectionEndpoint),
      ]);

      const namespaces = summary.namespaces || [];
      setText('stat-namespaces', namespaces.length);
      setText('namespace-count', `${namespaces.length} items`);

      renderTableBody('namespace-table', namespaces, (row) => `
        <td>${row.namespace}</td>
        <td class="text-end">${row.executions.toLocaleString()}</td>
        <td class="text-end">${Math.round(row.avg_duration_ms).toLocaleString()}</td>
        <td class="text-end">${Math.round(row.max_duration_ms).toLocaleString()}</td>
        <td class="text-end">${row.total_docs_examined.toLocaleString()}</td>
      `);

      const topPlan = (summary.plan_mix || [])[0];
      setText('stat-top-plan', topPlan ? `${topPlan.plan_summary} (${topPlan.executions})` : '-');

      renderList('plan-mix', summary.plan_mix || [], (item) => `
        <span>${item.plan_summary}</span>
        <span class="badge bg-primary">${item.executions.toLocaleString()}</span>
      `);

      renderList('auth-summary', auth.authentications || [], (item) => `
        <span>${item.result} / ${item.mechanism}</span>
        <span class="badge bg-success">${item.events.toLocaleString()}</span>
      `);
      setText('stat-auth', (auth.authentications || []).reduce((acc, cur) => acc + cur.events, 0).toLocaleString());

      renderList('connection-activity', connections.connections || [], (item) => `
        <div>
          <div>${item.remote_address}</div>
          <small class="text-muted">accepted: ${item.accepted} • ended: ${item.ended}</small>
        </div>
        <span class="badge bg-secondary">${item.unique_connections}</span>
      `);
      setText('stat-connections', (connections.connections || []).length);

      renderTableBody('search-table', summary.recent || [], (row) => `
        <td>${row.timestamp}</td>
        <td>${row.namespace}</td>
        <td>${row.duration_ms} ms</td>
        <td>${row.plan_summary}</td>
        <td><button class="btn btn-link btn-sm" data-hash="${row.query_hash}" data-action="details">Details</button></td>
      `);
    } catch (error) {
      console.error('Failed to load summary', error);
    }
  }

  async function handleSearch(event) {
    event.preventDefault();
    const form = event.target;
    const data = new FormData(form);
    const searchTerm = data.get('q');
    try {
      const response = await fetchJson(searchEndpoint, { q: searchTerm, limit: 100 });
      renderTableBody('search-table', response.results, (row) => `
        <td>${row.timestamp}</td>
        <td>${row.namespace}</td>
        <td>${row.duration_ms} ms</td>
        <td>${row.plan_summary}</td>
        <td><button class="btn btn-link btn-sm" data-hash="${row.query_hash}" data-action="details">Details</button></td>
      `);
    } catch (error) {
      console.error('Search failed', error);
    }
  }

  async function loadDetails(queryHash) {
    const modalBody = document.getElementById('detailModalBody');
    if (!modalBody) return;
    modalBody.innerHTML = '<div class="alert alert-info">Loading details...</div>';
    try {
      const data = await fetchJson(detailEndpoint(queryHash), { limit: 5 });
      const examples = data.examples || [];
      if (!examples.length) {
        modalBody.innerHTML = '<div class="alert alert-warning">No details available.</div>';
        return;
      }
      modalBody.innerHTML = examples
        .map((item) => `
          <div class="mb-3">
            <div class="d-flex justify-content-between align-items-center">
              <span class="badge bg-secondary">${item.timestamp}</span>
              <span class="text-muted">Line ${item.line_number}</span>
            </div>
            <pre class="bg-light p-3 mt-2"><code>${(item.raw || '').replace(/</g, '&lt;')}</code></pre>
          </div>
        `)
        .join('');
    } catch (error) {
      modalBody.innerHTML = `<div class="alert alert-danger">Failed to load details: ${error.message}</div>`;
    }
  }

  function setupEventListeners() {
    const searchForm = document.getElementById('search-form');
    if (searchForm) {
      searchForm.addEventListener('submit', handleSearch);
    }

    document.addEventListener('click', (event) => {
      const target = event.target;
      if (target && target.matches('[data-action="details"]')) {
        const hash = target.getAttribute('data-hash');
        const modal = new bootstrap.Modal(document.getElementById('detailModal'));
        modal.show();
        loadDetails(hash);
      }
    });
  }

  setupEventListeners();
  await loadSummary();
})();
