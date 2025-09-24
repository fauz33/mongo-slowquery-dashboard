(function () {
  const form = document.getElementById('user-access-form');
  const resultsContainer = document.getElementById('user-access-results');
  const countBadge = document.getElementById('user-access-count');

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const data = new FormData(form);
    const params = new URLSearchParams();
    for (const [key, value] of data.entries()) {
      if (value) params.append(key, value.toString());
    }

    resultsContainer.innerHTML = '<div class="alert alert-info">Loading...</div>';

    try {
      const resp = await fetch(`/v2/auth/logs?${params.toString()}`, { cache: 'no-store' });
      if (!resp.ok) throw new Error(`Request failed (${resp.status})`);
      const payload = await resp.json();
      const rows = payload.results || [];
      countBadge.textContent = rows.length.toString();

      if (!rows.length) {
        resultsContainer.innerHTML = '<p class="text-muted">No authentication events found.</p>';
        return;
      }

      resultsContainer.innerHTML = rows
        .map(
          (row) => {
            const resultText = (row.result || 'unknown').toString();
            const resultLower = resultText.toLowerCase();
            const isSuccess = resultLower.includes('success');
            const isFailure = resultLower.includes('fail');
            const badgeClass = isSuccess ? 'bg-success' : isFailure ? 'bg-danger' : 'bg-secondary';
            const iconClass = isSuccess ? 'fas fa-check me-1' : isFailure ? 'fas fa-exclamation-triangle me-1' : '';

            return `
            <div class="mb-3">
              <div class="d-flex justify-content-between align-items-center">
                <span class="fw-bold">${row.timestamp || 'unknown'}</span>
                <span class="badge ${badgeClass}">
                  ${iconClass ? `<i class="${iconClass}"></i>` : ''}${isSuccess ? 'Success' : isFailure ? 'Failed' : resultText}
                </span>
              </div>
              <dl class="row mb-0 small text-muted">
                <dt class="col-sm-3">User</dt>
                <dd class="col-sm-9">${row.user || '-'}</dd>
                <dt class="col-sm-3">Database</dt>
                <dd class="col-sm-9">${row.database || '-'}</dd>
                <dt class="col-sm-3">Mechanism</dt>
                <dd class="col-sm-9">${row.mechanism || '-'}</dd>
                <dt class="col-sm-3">Remote</dt>
                <dd class="col-sm-9">${row.remote_address || '-'}</dd>
                <dt class="col-sm-3">App</dt>
                <dd class="col-sm-9">${row.app_name || '-'}</dd>
                <dt class="col-sm-3">Error</dt>
                <dd class="col-sm-9">${row.error || '-'}</dd>
              </dl>
            </div>
          `;
          }
        )
        .join('');
    } catch (error) {
      resultsContainer.innerHTML = `<div class="alert alert-danger">${error.message}</div>`;
      countBadge.textContent = '0';
    }
  });
})();
