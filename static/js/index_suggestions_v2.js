(function () {
  const form = document.getElementById('index-suggestions-form');
  const resultsContainer = document.getElementById('index-suggestions-results');
  const countBadge = document.getElementById('index-suggestions-count');

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const data = new FormData(form);
    const params = new URLSearchParams();
    for (const [key, value] of data.entries()) {
      if (value) params.append(key, value.toString());
    }

    resultsContainer.innerHTML = '<div class="alert alert-info">Loading suggestions...</div>';

    try {
      const resp = await fetch(`/v2/index-suggestions?${params.toString()}`, { cache: 'no-store' });
      if (!resp.ok) throw new Error(`Request failed (${resp.status})`);
      const payload = await resp.json();
      const rows = payload.suggestions || [];
      countBadge.textContent = rows.length.toString();

      if (!rows.length) {
        resultsContainer.innerHTML = '<p class="text-muted">No suggestions generated (perhaps no collection scans recorded).</p>';
        return;
      }

      resultsContainer.innerHTML = rows
        .map(
          (row) => `
            <div class="card mb-3">
              <div class="card-body">
                <h5 class="card-title">${row.namespace || 'unknown'}</h5>
                <dl class="row mb-0 small text-muted">
                  <dt class="col-sm-3">Avg Duration</dt>
                  <dd class="col-sm-9">${Math.round(row.avg_duration_ms || 0)} ms</dd>
                  <dt class="col-sm-3">Executions</dt>
                  <dd class="col-sm-9">${(row.executions || 0).toLocaleString()}</dd>
                  <dt class="col-sm-3">Docs Examined</dt>
                  <dd class="col-sm-9">${(row.docs_examined || 0).toLocaleString()}</dd>
                  <dt class="col-sm-3">Recommendation</dt>
                  <dd class="col-sm-9">${row.recommendation}</dd>
                </dl>
                <details class="mt-2">
                  <summary class="text-primary">Sample Query</summary>
                  <pre class="bg-light p-2 mt-2"><code>${(row.sample_query || '').replace(/</g, '&lt;')}</code></pre>
                </details>
              </div>
            </div>
          `
        )
        .join('');
    } catch (error) {
      resultsContainer.innerHTML = `<div class="alert alert-danger">${error.message}</div>`;
      countBadge.textContent = '0';
    }
  });
})();
