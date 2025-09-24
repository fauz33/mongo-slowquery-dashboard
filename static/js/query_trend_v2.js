(function () {
  const endpoint = '/v2/query-trend/summary';
  let currentGrouping = 'namespace';

  const groupingButtons = document.querySelectorAll('[data-grouping]');
  groupingButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      currentGrouping = btn.getAttribute('data-grouping');
      groupingButtons.forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');
      loadTrend();
    });
  });

  async function fetchTrend() {
    const params = new URLSearchParams({ grouping: currentGrouping, limit: 50 });
    const resp = await fetch(`${endpoint}?${params.toString()}`, { cache: 'no-store' });
    if (!resp.ok) {
      throw new Error(`Failed to load trend: ${resp.status}`);
    }
    return resp.json();
  }

  function setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
  }

  function renderSummary(summary) {
    const tbody = document.querySelector('#trend-summary-table tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    if (!summary || !summary.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="text-muted text-center">No data available</td></tr>';
      setText('trend-groups', '0');
      setText('trend-top-duration', '-');
      setText('trend-total-exec', '0');
      return;
    }

    let totalExec = 0;
    summary.forEach((item, idx) => {
      totalExec += item.executions;
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${item.pattern_key || item.namespace || item.query_hash || 'n/a'}</td>
        <td class="text-end">${item.executions.toLocaleString()}</td>
        <td class="text-end">${Math.round(item.avg_duration_ms).toLocaleString()}</td>
        <td class="text-end">${Math.round(item.max_duration_ms).toLocaleString()}</td>
        <td class="text-end">${Math.round(item.total_duration_ms).toLocaleString()}</td>
      `;
      tbody.appendChild(tr);
      if (idx === 0) {
        setText('trend-top-duration', `${Math.round(item.total_duration_ms).toLocaleString()} ms`);
      }
    });
    setText('trend-groups', summary.length.toString());
    setText('trend-total-exec', totalExec.toLocaleString());
  }

  function renderSeries(series) {
    const list = document.getElementById('trend-series');
    if (!list) return;
    list.innerHTML = '';
    if (!series || !series.length) {
      list.innerHTML = '<li class="list-group-item text-muted">No samples available</li>';
      return;
    }

    series.forEach((item) => {
      const li = document.createElement('li');
      li.className = 'list-group-item';
      const label = item.pattern_key || item.namespace || item.query_hash || 'n/a';
      li.innerHTML = `
        <div class="d-flex justify-content-between align-items-center">
          <div>
            <div class="fw-bold">${label}</div>
            <small class="text-muted">${item.timestamp}</small>
          </div>
          <span class="badge bg-primary">${item.duration_ms} ms</span>
        </div>
      `;
      list.appendChild(li);
    });
  }

  async function loadTrend() {
    try {
      const data = await fetchTrend();
      renderSummary(data.summary);
      renderSeries(data.series);
    } catch (error) {
      console.error('Trend load failed', error);
    }
  }

  loadTrend();
})();
