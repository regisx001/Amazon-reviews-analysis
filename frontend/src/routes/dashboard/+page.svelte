<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { browser } from '$app/environment';
	import {
		fetchStats,
		fetchPredictionsByDate,
		fetchTopProductsSentiment,
		fetchDriftStatus,
		fetchAggregationStatus,
		type StatsResponse,
		type PredictionsByDateResponse,
		type ProductSentimentListResponse,
		type DriftStatusResponse,
		type AggregationStatusResponse,
		API_BASE
	} from '$lib/api';
	import { RefreshCw, TrendingUp, Users, MessageSquare, AlertCircle } from 'lucide-svelte';

	let stats = $state<StatsResponse | null>(null);
	let predictionsByDate = $state<PredictionsByDateResponse | null>(null);
	let topProductSentiments = $state<ProductSentimentListResponse | null>(null);
	let driftStatus = $state<DriftStatusResponse | null>(null);
	let aggregationStatus = $state<AggregationStatusResponse | null>(null);
	let loading = $state(true);
	let lastRefresh = $state<string>('');

	let barCanvas: HTMLCanvasElement | null = null;
	let pieCanvas: HTMLCanvasElement | null = null;

	let ChartApi: any = null;
	let barChart: any = null;
	let pieChart: any = null;

	let eventSource: EventSource | null = null;

	async function loadData() {
		loading = true;
		try {
			const [s, p, ts, drift, agg] = await Promise.all([
				fetchStats(),
				fetchPredictionsByDate(),
				fetchTopProductsSentiment(10),
				fetchDriftStatus(),
				fetchAggregationStatus()
			]);
			stats = s;
			predictionsByDate = p;
			topProductSentiments = ts;
			driftStatus = drift;
			aggregationStatus = agg;
			lastRefresh = new Date().toLocaleTimeString();

			if (ChartApi) {
				renderCharts();
			}
		} catch (e) {
			console.error(e);
		} finally {
			loading = false;
		}
	}

	function setupStreaming() {
		if (!browser) return;

		// Close existing connection if any
		if (eventSource) eventSource.close();

		const url = `${API_BASE}/api/stream/stats`;
		eventSource = new EventSource(url);

		eventSource.onmessage = (event) => {
			try {
				const data = jsonParse(event.data);
				if (data.stats) stats = data.stats;
				if (data.trend) predictionsByDate = data.trend;
				lastRefresh = new Date().toLocaleTimeString();

				if (ChartApi) renderCharts();
			} catch (e) {
				console.error('SSE Error:', e);
			}
		};

		eventSource.onerror = (e) => {
			console.error('SSE Connection failed:', e);
			eventSource?.close();
		};
	}

	function jsonParse(str: string) {
		try {
			return JSON.parse(str);
		} catch {
			return {};
		}
	}

	function renderCharts() {
		if (!ChartApi || !predictionsByDate || !stats) return;

		// Bar Chart
		if (barCanvas) {
			if (barChart) barChart.destroy();
			barChart = new ChartApi(barCanvas, {
				type: 'bar',
				data: {
					labels: $state.snapshot(predictionsByDate.labels),
					datasets: [
						{
							label: 'Positive',
							data: $state.snapshot(predictionsByDate.positive),
							backgroundColor: '#238636'
						},
						{
							label: 'Neutral',
							data: $state.snapshot(predictionsByDate.neutral),
							backgroundColor: '#d29922'
						},
						{
							label: 'Negative',
							data: $state.snapshot(predictionsByDate.negative),
							backgroundColor: '#f85149'
						}
					]
				},
				options: {
					responsive: true,
					maintainAspectRatio: false,
					plugins: { legend: { display: false } },
					scales: {
						x: { stacked: true, grid: { display: false } },
						y: { stacked: true, grid: { color: 'rgba(128,128,128,0.1)' } }
					}
				}
			});
		}

		// Pie Chart
		if (pieCanvas) {
			if (pieChart) pieChart.destroy();
			pieChart = new ChartApi(pieCanvas, {
				type: 'doughnut',
				data: {
					labels: ['Positive', 'Neutral', 'Negative'],
					datasets: [
						{
							data: $state.snapshot([stats.positive, stats.neutral, stats.negative]),
							backgroundColor: ['#238636', '#d29922', '#f85149'],
							borderWidth: 0
						}
					]
				},
				options: {
					responsive: true,
					maintainAspectRatio: false,
					cutout: '70%',
					plugins: { legend: { position: 'bottom' } }
				}
			});
		}
	}

	onMount(async () => {
		if (browser) {
			const { Chart, registerables } = await import('chart.js');
			Chart.register(...registerables);
			ChartApi = Chart;

			await loadData();
			setupStreaming();
		}
	});

	onDestroy(() => {
		barChart?.destroy();
		pieChart?.destroy();
		eventSource?.close();
	});

	function fmt(n: number | undefined) {
		return n?.toLocaleString() ?? '0';
	}

	function mixWidth(part: number, total: number) {
		return total > 0 ? (part / total) * 100 : 0;
	}

	function formatTimestamp(value: string | null | undefined) {
		if (!value) return 'Not available';
		const dt = new Date(value);
		if (Number.isNaN(dt.getTime())) return value;
		return dt.toLocaleString();
	}

	function ratioPct(value: number | undefined) {
		if (value === undefined || value === null) return '0.0';
		return (value * 100).toFixed(1);
	}

	function driftLabel(status: DriftStatusResponse | null) {
		if (!status) return 'Unknown';
		return status.drift_detected ? 'Drift detected' : 'Healthy';
	}

	function driftTone(status: DriftStatusResponse | null) {
		return status?.drift_detected ? 'var(--danger-fg)' : 'var(--success-fg)';
	}

	function driftBg(status: DriftStatusResponse | null) {
		return status?.drift_detected ? 'var(--danger-subtle)' : 'var(--success-subtle)';
	}

	function aggregationTitle(status: AggregationStatusResponse | null) {
		if (!status) return 'No aggregation info available';
		const monthly = formatTimestamp(status.monthly_last_aggregated_at);
		const product = formatTimestamp(status.product_last_aggregated_at);
		return `Monthly: ${monthly} | Product: ${product}`;
	}
</script>

<div class="overview-page">
	<div class="flex justify-between items-center mb-6" style="margin-bottom: 2rem;">
		<div>
			<h1 style="font-size: 1.5rem; margin-bottom: 0.25rem;">Analytics Overview</h1>
			<p style="color: var(--fg-muted); font-size: 0.875rem;">
				Global sentiment trends for Amazon products.
			</p>
		</div>
		<div class="flex items-center gap-4">
			<span style="font-size: 0.75rem; color: var(--fg-muted)">Last updated: {lastRefresh}</span>
			<span
				style="font-size: 0.75rem; color: var(--fg-muted)"
				title={aggregationTitle(aggregationStatus)}
			>
				Last aggregation: {formatTimestamp(aggregationStatus?.last_aggregated_at)}
			</span>
			<a href="/recommendations" class="btn" style="background: var(--accent-fg); color: white; border: none;">
				<TrendingUp size={14} style="margin-right: 0.5rem;" />
				IA Recommendations
			</a>
			<button class="btn" onclick={loadData}>
				<RefreshCw size={14} class={loading ? 'animate-spin' : ''} style="margin-right: 0.5rem;" />
				Sync
			</button>
		</div>
	</div>

	<div class="kpi-grid">
		<div class="kpi-card">
			<div class="kpi-label flex items-center gap-2">
				<MessageSquare size={14} /> Total Reviews
			</div>
			<div class="kpi-value">{fmt(stats?.total)}</div>
			<div class="kpi-trend" style="color: var(--fg-muted)">Total volume analyzed</div>
		</div>
		<div class="kpi-card" style="border-left: 4px solid var(--success-fg)">
			<div class="kpi-label flex items-center gap-2">
				<TrendingUp size={14} /> Positive
			</div>
			<div class="kpi-value">{fmt(stats?.positive)}</div>
			<div class="kpi-trend" style="color: var(--success-fg)">
				{stats ? ((stats.positive / stats.total) * 100).toFixed(1) : 0}% of total
			</div>
		</div>
		<div class="kpi-card" style="border-left: 4px solid var(--attention-fg)">
			<div class="kpi-label flex items-center gap-2">
				<Users size={14} /> Neutral
			</div>
			<div class="kpi-value">{fmt(stats?.neutral)}</div>
			<div class="kpi-trend" style="color: var(--attention-fg)">
				{stats ? ((stats.neutral / stats.total) * 100).toFixed(1) : 0}% of total
			</div>
		</div>
		<div class="kpi-card" style="border-left: 4px solid var(--danger-fg)">
			<div class="kpi-label flex items-center gap-2">
				<AlertCircle size={14} /> Negative
			</div>
			<div class="kpi-value">{fmt(stats?.negative)}</div>
			<div class="kpi-trend" style="color: var(--danger-fg)">
				{stats ? ((stats.negative / stats.total) * 100).toFixed(1) : 0}% of total
			</div>
		</div>
	</div>

	<div class="card" style="margin-top: 1.5rem;">
		<div class="card-title">Model Drift Status</div>
		{#if driftStatus}
			<div class="flex items-center gap-4" style="flex-wrap: wrap;">
				<div
					style="padding: 0.25rem 0.6rem; border-radius: 999px; background: {driftBg(
						driftStatus
					)}; color: {driftTone(driftStatus)}; font-size: 0.75rem; font-weight: 600;"
				>
					{driftLabel(driftStatus)}
				</div>
				<div style="font-size: 0.875rem; color: var(--fg-muted);">
					Neutral ratio: {ratioPct(driftStatus.neutral_ratio)}% | Negative ratio: {ratioPct(
						driftStatus.negative_ratio
					)}%
				</div>
				<div style="font-size: 0.75rem; color: var(--fg-muted);">
					Evaluated at: {formatTimestamp(driftStatus.evaluated_at)}
				</div>
			</div>
			{#if driftStatus.alerts.length > 0}
				<div style="margin-top: 0.5rem; color: var(--danger-fg); font-size: 0.875rem;">
					{driftStatus.alerts.join(' | ')}
				</div>
			{:else if driftStatus.reason}
				<div style="margin-top: 0.5rem; color: var(--fg-muted); font-size: 0.875rem;">
					Reason: {driftStatus.reason}
				</div>
			{/if}
		{:else}
			<div style="color: var(--fg-muted);">No drift status available.</div>
		{/if}
	</div>

	<div style="display: grid; grid-template-columns: 2fr 1fr; gap: 1.5rem;">
		<div class="card">
			<div class="card-title">Predictions Over Time</div>
			<div style="height: 300px;">
				<canvas bind:this={barCanvas}></canvas>
			</div>
		</div>
		<div class="card">
			<div class="card-title">Sentiment Distribution</div>
			<div style="height: 300px;">
				<canvas bind:this={pieCanvas}></canvas>
			</div>
		</div>
	</div>

	<div class="card">
		<div class="card-title">Top Products - Sentiment Mix (Aggregated)</div>
		<div class="table-container">
			<table>
				<thead>
					<tr>
						<th>Product ID</th>
						<th>Total</th>
						<th>Positive</th>
						<th>Neutral</th>
						<th>Negative</th>
						<th>Mix</th>
					</tr>
				</thead>
				<tbody>
					{#if topProductSentiments && topProductSentiments.products.length > 0}
						{#each topProductSentiments.products as p}
							<tr>
								<td class="mono">
									<a href="/dashboard/{p.product_id}">{p.product_id}</a>
								</td>
								<td>{p.total.toLocaleString()}</td>
								<td style="color: var(--success-fg)">{p.percentages.positive}%</td>
								<td style="color: var(--attention-fg)">{p.percentages.neutral}%</td>
								<td style="color: var(--danger-fg)">{p.percentages.negative}%</td>
								<td>
									<div
										style="display: flex; height: 8px; background: var(--canvas-inset); border-radius: 4px; overflow: hidden; min-width: 120px;"
									>
										<div
											style="width: {mixWidth(
												p.counts.positive,
												p.total
											)}%; background: var(--success-fg);"
										></div>
										<div
											style="width: {mixWidth(
												p.counts.neutral,
												p.total
											)}%; background: var(--attention-fg);"
										></div>
										<div
											style="width: {mixWidth(
												p.counts.negative,
												p.total
											)}%; background: var(--danger-fg);"
										></div>
									</div>
								</td>
							</tr>
						{/each}
					{:else}
						<tr>
							<td colspan="6" style="text-align: center; padding: 2rem; color: var(--fg-muted);">
								No aggregated product sentiment data found.
							</td>
						</tr>
					{/if}
				</tbody>
			</table>
		</div>
	</div>
</div>

<style>
	.animate-spin {
		animation: spin 1s linear infinite;
	}
	@keyframes spin {
		from {
			transform: rotate(0deg);
		}
		to {
			transform: rotate(360deg);
		}
	}
</style>
