<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { browser } from '$app/environment';
  import { fetchRecentPredictions, type RecentPrediction, API_BASE } from '$lib/api';
  import { RefreshCw, Search, Filter, ArrowUpDown } from 'lucide-svelte';

  let predictions = $state<RecentPrediction[]>([]);
  let loading = $state(true);
  let filterText = $state('');
  let eventSource: EventSource | null = null;

  async function loadRecent() {
    loading = true;
    try {
      const data = await fetchRecentPredictions(50);
      predictions = data.predictions;
    } catch (e) {
      console.error(e);
    } finally {
      loading = false;
    }
  }

  function setupStreaming() {
    if (!browser) return;
    
    if (eventSource) eventSource.close();
    
    const url = `${API_BASE}/api/stream/recent`;
    eventSource = new EventSource(url);
    
    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.predictions) {
          predictions = data.predictions;
        }
      } catch (e) {
        console.error("SSE Error:", e);
      }
    };
    
    eventSource.onerror = (e) => {
      console.error("SSE Connection failed:", e);
      eventSource?.close();
    };
  }

  onMount(() => {
    loadRecent();
    setupStreaming();
  });

  onDestroy(() => {
    eventSource?.close();
  });

  function getBadgeClass(sentiment: string | undefined) {
    const s = sentiment?.toLowerCase();
    if (s === 'positive') return 'badge-success';
    if (s === 'negative') return 'badge-danger';
    if (s === 'neutral') return 'badge-attention';
    return 'badge-neutral';
  }

  function scoreStars(score: string | number | undefined) {
    const s = Number(score) || 0;
    return '★'.repeat(s) + '☆'.repeat(5 - s);
  }

  let filteredPredictions = $derived(
    predictions.filter(p => 
      p.ProductId?.toLowerCase().includes(filterText.toLowerCase()) ||
      p.ProfileName?.toLowerCase().includes(filterText.toLowerCase()) ||
      p.Summary?.toLowerCase().includes(filterText.toLowerCase())
    )
  );
</script>

<svelte:head>
	<title>Recent Activity | Amazon Sentiment</title>
	<meta name="description" content="Live stream of recent sentiment predictions and model outputs." />
</svelte:head>

<div class="recent-page">
  <div class="flex justify-between items-center mb-6" style="margin-bottom: 2rem;">
    <div>
      <h1 style="font-size: 1.5rem; margin-bottom: 0.25rem;">Recent Activity</h1>
      <p style="color: var(--fg-muted); font-size: 0.875rem;">Latest predictions synchronized from MongoDB.</p>
    </div>
    <button class="btn" onclick={loadRecent}>
      <RefreshCw size={14} class={loading ? 'animate-spin' : ''} style="margin-right: 0.5rem;" />
      Refresh
    </button>
  </div>

  <div class="flex gap-4 mb-4" style="margin-bottom: 1.5rem;">
    <div style="position: relative; flex: 1;">
      <Search size={16} style="position: absolute; left: 12px; top: 10px; color: var(--fg-muted);" />
      <input 
        type="text" 
        class="input" 
        placeholder="Filter by product, user, or summary..." 
        style="padding-left: 40px;"
        bind:value={filterText}
      />
    </div>
    <button class="btn">
      <Filter size={16} style="margin-right: 0.5rem;" /> Filter
    </button>
  </div>

  <div class="table-container">
    <table>
      <thead>
        <tr>
          <th>User</th>
          <th>Product</th>
          <th>Score</th>
          <th>Sentiment</th>
          <th>Summary</th>
          <th>Time</th>
        </tr>
      </thead>
      <tbody>
        {#if loading && predictions.length === 0}
          <tr><td colspan="6" style="text-align: center; padding: 3rem;">Loading...</td></tr>
        {:else if filteredPredictions.length === 0}
          <tr><td colspan="6" style="text-align: center; padding: 3rem; color: var(--fg-muted);">No matching predictions found.</td></tr>
        {:else}
          {#each filteredPredictions as p}
            <tr>
              <td>
                <div class="flex items-center gap-2">
                  <div style="width: 24px; height: 24px; border-radius: 50%; background: var(--canvas-inset); display: flex; align-items: center; justify-content: center; font-size: 0.7rem; font-weight: 600;">
                    {p.ProfileName?.charAt(0) || '?'}
                  </div>
                  <span style="font-weight: 500;">{p.ProfileName || 'Anonymous'}</span>
                </div>
              </td>
              <td class="mono"><a href="/dashboard/{p.ProductId}">{p.ProductId}</a></td>
              <td style="color: var(--attention-fg); letter-spacing: 2px;">{scoreStars(p.Score)}</td>
              <td>
                <span class="badge {getBadgeClass(p.PredictedSentiment)}">
                  {p.PredictedSentiment}
                </span>
              </td>
              <td style="max-width: 300px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title={p.Summary}>
                {p.Summary || 'No summary'}
              </td>
              <td class="mono" style="font-size: 0.75rem; color: var(--fg-muted);">
                {p.ProcessingTime ? new Date(p.ProcessingTime).toLocaleString() : '—'}
              </td>
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
  </div>
</div>

<style>
  .animate-spin {
    animation: spin 1s linear infinite;
  }
  @keyframes spin {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
  }
</style>
