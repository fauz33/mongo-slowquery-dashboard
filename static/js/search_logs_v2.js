(function () {
  const form = document.getElementById('log-search-form');
  const resultsContainer = document.getElementById('log-results');
  const countBadge = document.getElementById('log-results-count');

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const data = new FormData(form);
    const params = new URLSearchParams();
    for (const [key, value] of data.entries()) {
      if (value) params.append(key, value.toString());
    }
    if (!data.get('case_sensitive')) {
      params.delete('case_sensitive');
    }

    resultsContainer.innerHTML = '<div class="alert alert-info">Searching...</div>';

    try {
      const resp = await fetch(`/v2/logs/search?${params.toString()}`, { cache: 'no-store' });
      if (!resp.ok) throw new Error(`Search failed (${resp.status})`);
      const payload = await resp.json();
      const rows = payload.results || [];
      countBadge.textContent = rows.length.toString();
      if (!rows.length) {
        resultsContainer.innerHTML = '<p class="text-muted">No log entries matched.</p>';
        return;
      }
      resultsContainer.innerHTML = rows
        .map(
          (row) => `
            <div class="mb-3">
              <div class="d-flex justify-content-between align-items-center">
                <span class="fw-bold">${row.timestamp || 'unknown'}</span>
                <span class="text-muted">${row.component || ''} ${row.level || ''}</span>
              </div>
              <pre class="bg-light p-2 mt-2"><code>${(row.raw || '').replace(/</g, '&lt;')}</code></pre>
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
