(async function () {
  const endpoint = '/v2/dashboard/summary';
  const params = new URLSearchParams({ limit: 10 });

  try {
    const resp = await fetch(`${endpoint}?${params.toString()}`, { cache: 'no-store' });
    if (!resp.ok) throw new Error(`Summary request failed (${resp.status})`);
    const data = await resp.json();

    setText('card-slow-queries', (data.slow_queries_count || 0).toLocaleString());
    setText('card-namespaces', (data.namespaces_count || data.unique_namespaces || 0).toLocaleString());
    setText('card-connections', (data.connections_count || data.total_connections || 0).toLocaleString());
    setText('card-auth', (data.auth_count || data.total_authentications || 0).toLocaleString());

    renderNamespaces(data.top_namespaces || []);
    renderPatterns(data.top_patterns || []);
    renderAuth(data.recent_authentications || []);
  } catch (error) {
    console.error('Failed to load dashboard summary', error);
    setText('card-slow-queries', 'ERR');
  }

  function setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
  }

  function renderNamespaces(rows) {
    const container = document.getElementById('top-namespaces');
    const badge = document.getElementById('top-namespaces-count');
    if (!container) return;
    if (badge) badge.textContent = rows.length.toString();
    if (!rows.length) {
      container.innerHTML = '<p class="text-muted">No data available.</p>';
      return;
    }
    container.innerHTML = rows
      .map((row) => {
        const exec = row.query_count || 0;
        const collscan = row.collscan_count || 0;
        const avg = Math.round(row.avg_duration || 0);
        return `
          <div class="d-flex justify-content-between align-items-center border-bottom py-2">
            <div>
              <div class="fw-bold">${row.namespace || '-'}</div>
              <small class="text-muted">Executions: ${exec.toLocaleString()} • COLLSCAN: ${collscan.toLocaleString()}</small>
            </div>
            <span class="badge bg-primary">${avg.toLocaleString()} ms</span>
          </div>
        `;
      })
      .join('');
  }

  function renderPatterns(rows) {
    const container = document.getElementById('top-patterns');
    const badge = document.getElementById('top-patterns-count');
    if (!container) return;
    if (badge) badge.textContent = rows.length.toString();
    if (!rows.length) {
      container.innerHTML = '<p class="text-muted">No data available.</p>';
      return;
    }
    container.innerHTML = rows
      .map((row) => {
        const avg = Math.round(row.avg_duration_ms || 0);
        const executions = row.executions || 0;
        const label = row.query_hash ? `${row.query_hash.slice(0, 10)}…` : row.namespace || 'pattern';
        return `
          <div class="d-flex justify-content-between align-items-center border-bottom py-2">
            <div>
              <div class="fw-bold">${label}</div>
              <small class="text-muted">Namespace: ${row.namespace || '-'} • Executions: ${executions.toLocaleString()}</small>
            </div>
            <span class="badge bg-primary">${avg.toLocaleString()} ms</span>
          </div>
        `;
      })
      .join('');
  }

  function renderAuth(rows) {
    const container = document.getElementById('recent-auth');
    if (!container) return;
    if (!rows.length) {
      container.innerHTML = '<p class="text-muted">No authentication events.</p>';
      return;
    }
    container.innerHTML = rows
      .map((row) => {
        const result = row.result || 'unknown';
        const badge = result.toLowerCase() === 'success' ? 'bg-success' : 'bg-danger';
        return `
          <div class="border-bottom py-2">
            <div class="d-flex justify-content-between">
              <span class="fw-bold">${row.timestamp || 'unknown'}</span>
              <span class="badge ${badge}">${result}</span>
            </div>
            <small class="text-muted">User: ${row.user || '-'} • Mechanism: ${row.mechanism || '-'} • Remote: ${row.remote_address || '-'}</small>
          </div>
        `;
      })
      .join('');
  }
})();
