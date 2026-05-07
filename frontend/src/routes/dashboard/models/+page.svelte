<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { browser } from '$app/environment';
  import {
    fetchModelInsights,
    fetchPredictionsByDate,
    type ModelInsightsResponse,
    type PredictionsByDateResponse
  } from '$lib/api';
  import { RefreshCw, CheckCircle, AlertTriangle, BarChart3, Cpu, Info } from 'lucide-svelte';

  let insights = $state<ModelInsightsResponse | null>(null);
  let trend = $state<PredictionsByDateResponse | null>(null);
  let loading = $state(true);

  let barCanvas: HTMLCanvasElement | null = null;
  let ChartApi: any = null;
  let barChart: any = null;

  async function loadData() {
    loading = true;
    try {
      const [insightData, trendData] = await Promise.all([
        fetchModelInsights(),
        fetchPredictionsByDate()
      ]);
      insights = insightData;
      trend = trendData;
      if (ChartApi) renderCharts();
    } catch (e) {
      console.error(e);
    } finally {
      loading = false;
    }
  }

  onMount(async () => {
    if (browser) {
      const { Chart, registerables } = await import('chart.js');
      Chart.register(...registerables);
      ChartApi = Chart;
    }
    await loadData();
  });

  onDestroy(() => {
    barChart?.destroy();
  });

  function formatTimestamp(value: string | null | undefined) {
    if (!value) return 'Not available';
    const dt = new Date(value);
    if (Number.isNaN(dt.getTime())) return value;
    return dt.toLocaleString();
  }

  function safeNumber(value: number | null | undefined) {
    return typeof value === 'number' && Number.isFinite(value) ? value : 0;
  }

  function formatPercent(value: number | null | undefined) {
    return (safeNumber(value) * 100).toFixed(1) + '%';
  }

  function renderCharts() {
    if (!ChartApi || !trend || !barCanvas) return;

    const labels = $state.snapshot(trend.labels);
    const positive = $state.snapshot(trend.positive);
    const neutral = $state.snapshot(trend.neutral);
    const negative = $state.snapshot(trend.negative);

    if (barChart) barChart.destroy();
    barChart = new ChartApi(barCanvas, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          { label: 'Positive', data: positive, backgroundColor: '#238636' },
          { label: 'Neutral', data: neutral, backgroundColor: '#d29922' },
          { label: 'Negative', data: negative, backgroundColor: '#f85149' }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { position: 'bottom' } },
        scales: {
          x: { stacked: true, grid: { display: false } },
          y: { stacked: true, grid: { color: 'rgba(128,128,128,0.1)' } }
        }
      }
    });
  }

  let perClass = $derived(() => {
    const data = insights?.metrics?.per_class || null;
    if (!data) return [] as { label: string; precision: number; recall: number; f1: number }[];
    return Object.entries(data).map(([label, row]) => ({
      label,
      precision: safeNumber(row.precision),
      recall: safeNumber(row.recall),
      f1: safeNumber(row.f1)
    }));
  });

  function matrixMax() {
    const matrix = insights?.confusion_matrix?.matrix || [];
    let max = 0;
    for (const row of matrix) {
      for (const value of row) {
        if (value > max) max = value;
      }
    }
    return max || 1;
  }

  function cellStyle(value: number) {
    const max = matrixMax();
    const ratio = max > 0 ? value / max : 0;
    const alpha = 0.15 + ratio * 0.6;
    return `background: rgba(47, 129, 247, ${alpha});`;
  }
</script>

<svelte:head>
	<title>Model Insights | Amazon Sentiment</title>
	<meta
		name="description"
		content="Technical details, evaluation metrics, and confusion matrix for the sentiment analysis model."
	/>
</svelte:head>

<div class="overview-page">
  <div class="flex justify-between items-center mb-6" style="margin-bottom: 2rem;">
    <div>
      <h1 style="font-size: 1.5rem; margin-bottom: 0.25rem;">Model Insights</h1>
      <p style="color: var(--fg-muted); font-size: 0.875rem;">
        Evaluation metrics, confusion matrix, and model metadata.
      </p>
    </div>
    <div class="flex items-center gap-4">
      <span style="font-size: 0.75rem; color: var(--fg-muted)">
        Evaluated at: {formatTimestamp(insights?.evaluated_at)}
      </span>
      <button class="btn" onclick={loadData}>
        <RefreshCw size={14} class={loading ? 'animate-spin' : ''} style="margin-right: 0.5rem;" />
        Refresh
      </button>
    </div>
  </div>

  <div class="card" style="margin-bottom: 1.5rem;">
    <div class="card-title">Model Status</div>
    {#if insights}
      <div class="flex items-center gap-3" style="font-size: 0.875rem;">
        {#if insights.status === 'ok'}
          <CheckCircle size={16} style="color: var(--success-fg);" />
          <span style="color: var(--success-fg); font-weight: 600;">Loaded from saved insights</span>
        {:else}
          <AlertTriangle size={16} style="color: var(--attention-fg);" />
          <span style="color: var(--attention-fg); font-weight: 600;">{insights.status}</span>
        {/if}
        {#if insights.errors && insights.errors.length > 0}
          <span style="color: var(--fg-muted);">Errors: {insights.errors.join(' | ')}</span>
        {/if}
      </div>
    {:else}
      <div style="color: var(--fg-muted);">No model insights available.</div>
    {/if}
  </div>

  <div class="kpi-grid">
    <div class="kpi-card">
      <div class="kpi-label flex items-center gap-2">
        <BarChart3 size={14} /> Accuracy
      </div>
      <div class="kpi-value">{formatPercent(insights?.metrics?.accuracy)}</div>
      <div class="kpi-trend" style="color: var(--fg-muted)">Weighted accuracy</div>
    </div>
    <div class="kpi-card" style="border-left: 4px solid var(--success-fg)">
      <div class="kpi-label flex items-center gap-2">
        <CheckCircle size={14} /> F1 (weighted)
      </div>
      <div class="kpi-value">{formatPercent(insights?.metrics?.f1_weighted)}</div>
      <div class="kpi-trend" style="color: var(--success-fg)">Overall balance</div>
    </div>
    <div class="kpi-card" style="border-left: 4px solid var(--accent-fg)">
      <div class="kpi-label flex items-center gap-2">
        <Cpu size={14} /> Precision (weighted)
      </div>
      <div class="kpi-value">{formatPercent(insights?.metrics?.precision_weighted)}</div>
      <div class="kpi-trend" style="color: var(--fg-muted)">Positive correctness</div>
    </div>
    <div class="kpi-card" style="border-left: 4px solid var(--attention-fg)">
      <div class="kpi-label flex items-center gap-2">
        <Info size={14} /> Recall (weighted)
      </div>
      <div class="kpi-value">{formatPercent(insights?.metrics?.recall_weighted)}</div>
      <div class="kpi-trend" style="color: var(--fg-muted)">Coverage of positives</div>
    </div>
  </div>

  <div style="display: grid; grid-template-columns: 2fr 1fr; gap: 1.5rem; margin-top: 1.5rem;">
    <div class="card">
      <div class="card-title">Prediction Distribution Over Time</div>
      <div style="height: 320px;">
        <canvas bind:this={barCanvas}></canvas>
      </div>
    </div>
    <div class="card">
      <div class="card-title">Model Metadata</div>
      {#if insights?.metadata}
        <div class="table-container">
          <table>
            <tbody>
              <tr><td class="mono">Model</td><td>{insights.metadata.model_name || '—'}</td></tr>
              <tr><td class="mono">Version</td><td>{insights.metadata.model_version || '—'}</td></tr>
              <tr><td class="mono">Trained at</td><td>{formatTimestamp(insights.metadata.trained_at || null)}</td></tr>
              <tr><td class="mono">Dataset</td><td>{insights.metadata.dataset || '—'}</td></tr>
              <tr><td class="mono">Notes</td><td>{insights.metadata.notes || '—'}</td></tr>
            </tbody>
          </table>
        </div>
      {:else}
        <div style="color: var(--fg-muted);">No model metadata found.</div>
      {/if}
    </div>
  </div>

  <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; margin-top: 1.5rem;">
    <div class="card">
      <div class="card-title">Confusion Matrix</div>
      {#if insights?.confusion_matrix}
        <div class="table-container">
          <table>
            <thead>
              <tr>
                <th></th>
                {#each insights.confusion_matrix.labels as label}
                  <th>{label}</th>
                {/each}
              </tr>
            </thead>
            <tbody>
              {#each insights.confusion_matrix.matrix as row, i}
                <tr>
                  <td class="mono">{insights.confusion_matrix.labels[i] || '—'}</td>
                  {#each row as value}
                    <td style={cellStyle(value)}>{value}</td>
                  {/each}
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {:else}
        <div style="color: var(--fg-muted);">No confusion matrix available.</div>
      {/if}
    </div>
    <div class="card">
      <div class="card-title">Per-Class Metrics</div>
      {#if perClass.length > 0}
        <div class="table-container">
          <table>
            <thead>
              <tr>
                <th>Class</th>
                <th>Precision</th>
                <th>Recall</th>
                <th>F1</th>
              </tr>
            </thead>
            <tbody>
              {#each perClass as row}
                <tr>
                  <td class="mono">{row.label}</td>
                  <td>{formatPercent(row.precision)}</td>
                  <td>{formatPercent(row.recall)}</td>
                  <td>{formatPercent(row.f1)}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {:else}
        <div style="color: var(--fg-muted);">No per-class metrics available.</div>
      {/if}
    </div>
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
