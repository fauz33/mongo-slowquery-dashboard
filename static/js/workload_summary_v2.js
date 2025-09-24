(function () {
  const endpoint = '/v2/workload/summary';

  function fmtNumber(value) {
    if (value == null) return '-';
    if (value > 1e12) return `${(value / 1e12).toFixed(2)}T`;
    if (value > 1e9) return `${(value / 1e9).toFixed(2)}G`;
    if (value > 1e6) return `${(value / 1e6).toFixed(2)}M`;
    if (value > 1e3) return `${(value / 1e3).toFixed(2)}K`;
    return value.toLocaleString();
  }

  function setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
  }

  function renderList(targetId, items, metricKey) {
    const el = document.getElementById(targetId);
    if (!el) return;
    el.innerHTML = '';
    if (!items || !items.length) {
      el.innerHTML = '<li class="list-group-item text-muted">No data</li>';
      return;
    }
    items.forEach((item) => {
      const li = document.createElement('li');
      li.className = 'list-group-item d-flex justify-content-between align-items-center';
      const metric = fmtNumber(item[metricKey] || 0);
      li.innerHTML = `
        <div>
          <div class="fw-bold">${item.namespace || 'unknown'}</div>
          <small class="text-muted">Executions: ${(item.executions || 0).toLocaleString()}</small>
        </div>
        <span class="badge bg-primary">${metric}</span>
      `;
      el.appendChild(li);
    });
  }

  async function loadWorkload() {
    try {
      const resp = await fetch(endpoint, { cache: 'no-store' });
      if (!resp.ok) throw new Error(`Request failed: ${resp.status}`);
      const data = await resp.json();
      const stats = data.stats || {};
      setText('stat-executions', (stats.executions || 0).toLocaleString());
      setText('stat-docs-examined', fmtNumber(stats.total_docs_examined || 0));
      setText('stat-docs-returned', fmtNumber(stats.total_docs_returned || 0));
      setText('stat-keys-examined', fmtNumber(stats.total_keys_examined || 0));

      renderList('list-docs-examined', data.top_docs_examined, 'total_docs_examined');
      renderList('list-docs-returned', data.top_docs_returned, 'total_docs_returned');
      renderList('list-duration', data.top_duration, 'total_duration_ms');
      renderList('list-io', data.top_io_time, 'total_keys_examined');
    } catch (error) {
      console.error('Failed to load workload summary', error);
    }
  }

  loadWorkload();
})();
