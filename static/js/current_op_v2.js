(function () {
  const form = document.getElementById('current-op-form');
  const summaryPanel = document.getElementById('summary-panel');
  const recommendationsPanel = document.getElementById('recommendations-panel');
  const operationsPanel = document.getElementById('operations-panel');

  function renderSummary(data) {
    if (data.error) {
      summaryPanel.innerHTML = `<div class="alert alert-danger">${data.error}</div>`;
      recommendationsPanel.innerHTML = '<p class="text-muted">No recommendations.</p>';
      operationsPanel.innerHTML = '<p class="text-muted">No operations.</p>';
      return;
    }

    const metrics = data.performance_metrics || {};
    summaryPanel.innerHTML = `
      <div class="row g-3">
        <div class="col-md-4">
          <div class="card border-0 bg-light">
            <div class="card-body text-center">
              <h6 class="text-muted">Total Ops</h6>
              <h3>${data.total_operations.toLocaleString()}</h3>
            </div>
          </div>
        </div>
        <div class="col-md-4">
          <div class="card border-0 bg-light">
            <div class="card-body text-center">
              <h6 class="text-muted">Avg Duration (s)</h6>
              <h3>${(metrics.avg_duration || 0).toFixed(2)}</h3>
            </div>
          </div>
        </div>
        <div class="col-md-4">
          <div class="card border-0 bg-light">
            <div class="card-body text-center">
              <h6 class="text-muted">Max Duration (s)</h6>
              <h3>${(metrics.max_duration || 0).toFixed(2)}</h3>
            </div>
          </div>
        </div>
      </div>
    `;

    const recs = data.recommendations || [];
    if (recs.length === 0) {
      recommendationsPanel.innerHTML = '<p class="text-muted">No recommendations.</p>';
    } else {
      recommendationsPanel.innerHTML = recs
        .map(
          (rec) => `
            <div class="alert alert-${rec.type === 'error' ? 'danger' : rec.type === 'warning' ? 'warning' : 'info'}">
              <h6 class="alert-heading">${rec.title}</h6>
              <p class="mb-1">${rec.description}</p>
              <small class="text-muted">Suggested action: ${rec.action}</small>
            </div>
          `
        )
        .join('');
    }

    const ops = data.ops_brief || [];
    if (!ops.length) {
      operationsPanel.innerHTML = '<p class="text-muted">No operations to display.</p>';
    } else {
      operationsPanel.innerHTML = `
        <div class="table-responsive" style="max-height:420px; overflow:auto;">
          <table class="table table-striped table-hover mb-0">
            <thead>
              <tr>
                <th>OpID</th>
                <th>Namespace</th>
                <th>Client</th>
                <th>Plan</th>
                <th class="text-end">CPU (s)</th>
                <th class="text-end">Bytes Read</th>
                <th class="text-end">Bytes Written</th>
              </tr>
            </thead>
            <tbody>
              ${ops
                .map(
                  (item) => `
                    <tr>
                      <td>${item.opid ?? '-'}</td>
                      <td>${item.ns ?? '-'}</td>
                      <td>${item.client ?? '-'}</td>
                      <td>${item.planSummary ?? '-'}</td>
                      <td class="text-end">${item.cpuTime_s != null ? item.cpuTime_s.toFixed(3) : '-'}</td>
                      <td class="text-end">${formatBytes(item.bytesRead)}</td>
                      <td class="text-end">${formatBytes(item.bytesWritten)}</td>
                    </tr>
                  `
                )
                .join('')}
            </tbody>
          </table>
        </div>
      `;
    }
  }

  function formatBytes(value) {
    if (value == null) return '-';
    if (value > 1e9) return `${(value / 1e9).toFixed(2)} GB`;
    if (value > 1e6) return `${(value / 1e6).toFixed(2)} MB`;
    if (value > 1e3) return `${(value / 1e3).toFixed(2)} KB`;
    return value.toLocaleString();
  }

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const data = document.getElementById('current-op-text').value.trim();
    if (!data) {
      summaryPanel.innerHTML = '<div class="alert alert-warning">Please provide currentOp data.</div>';
      return;
    }

    summaryPanel.innerHTML = '<div class="alert alert-info">Analyzing...</div>';
    recommendationsPanel.innerHTML = '';
    operationsPanel.innerHTML = '';

    try {
      const payload = {
        data,
        threshold: parseInt(document.getElementById('threshold').value || '30', 10),
        filters: {
          only_active: document.getElementById('only-active').checked,
          only_waiting: document.getElementById('only-waiting').checked,
          only_collscan: document.getElementById('only-collscan').checked,
        },
      };
      const resp = await fetch('/v2/current-op/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const result = await resp.json();
      if (!resp.ok) {
        summaryPanel.innerHTML = `<div class="alert alert-danger">${result.error || 'Analysis failed'}</div>`;
        return;
      }
      renderSummary(result);
    } catch (error) {
      summaryPanel.innerHTML = `<div class="alert alert-danger">${error.message}</div>`;
    }
  });
})();
