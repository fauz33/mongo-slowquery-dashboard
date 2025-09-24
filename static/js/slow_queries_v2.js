(function () {
  const listEndpoint = '/v2/slow-query/list';
  const detailEndpoint = (hash) => `/v2/slow-query/details/${hash}`;

  function setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
  }

  async function loadSlowQueries() {
    const resp = await fetch(`${listEndpoint}?limit=200`, { cache: 'no-store' });
    if (!resp.ok) {
      throw new Error(`Failed to load slow queries: ${resp.status}`);
    }
    const data = await resp.json();
    const rows = data.results || [];
    setText('slowq-count', `${rows.length} items`);
    const tbody = document.querySelector('#slowq-table tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    if (!rows.length) {
      const tr = document.createElement('tr');
      tr.innerHTML = '<td colspan="6" class="text-muted text-center">No slow queries found</td>';
      tbody.appendChild(tr);
      return;
    }

    rows.forEach((row) => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${row.timestamp}</td>
        <td>${row.namespace}</td>
        <td>${row.duration_ms} ms</td>
        <td>${row.plan_summary}</td>
        <td>${row.operation}</td>
        <td><button class="btn btn-link btn-sm" data-action="details" data-hash="${row.query_hash}">Details</button></td>
      `;
      tbody.appendChild(tr);
    });
  }

  async function loadDetails(queryHash) {
    const modalBody = document.getElementById('detailModalBody');
    if (modalBody) {
      modalBody.innerHTML = '<div class="alert alert-info">Loading details...</div>';
    }
    const resp = await fetch(`${detailEndpoint(queryHash)}?limit=5`, { cache: 'no-store' });
    if (!resp.ok) {
      if (modalBody) {
        modalBody.innerHTML = `<div class="alert alert-danger">Failed to load details (${resp.status})</div>`;
      }
      return;
    }
    const data = await resp.json();
    const examples = data.examples || [];
    if (!modalBody) return;
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
  }

  document.getElementById('refresh-btn').addEventListener('click', () => {
    loadSlowQueries().catch((err) => console.error(err));
  });

  document.addEventListener('click', (event) => {
    const target = event.target;
    if (target && target.matches('[data-action="details"]')) {
      const hash = target.getAttribute('data-hash');
      const modal = new bootstrap.Modal(document.getElementById('detailModal'));
      modal.show();
      loadDetails(hash).catch((err) => console.error(err));
    }
  });

  loadSlowQueries().catch((err) => console.error(err));
})();
