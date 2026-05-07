<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { browser } from '$app/environment';
	import {
		fetchPredictionsByDate,
		fetchAggregationStatus,
		type PredictionsByDateResponse,
		type AggregationStatusResponse
	} from '$lib/api';
	import { RefreshCw, TrendingUp, AlertTriangle, BarChart3, CalendarDays } from 'lucide-svelte';

	type Row = {
		label: string;
		positive: number;
		neutral: number;
		negative: number;
		total: number;
	};

	let trend = $state<PredictionsByDateResponse | null>(null);
	let aggregationStatus = $state<AggregationStatusResponse | null>(null);
	let loading = $state(true);

	let barCanvas: HTMLCanvasElement | null = null;
	let lineCanvas: HTMLCanvasElement | null = null;
	let ChartApi: any = null;
	let barChart: any = null;
	let lineChart: any = null;

	async function loadData() {
		loading = true;
		try {
			const [trendData, agg] = await Promise.all([
				fetchPredictionsByDate(),
				fetchAggregationStatus()
			]);
			trend = trendData;
			aggregationStatus = agg;
			if (ChartApi) {
				renderCharts();
			}
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
		lineChart?.destroy();
	});

	let rows = $derived<Row[]>(
		trend
			? trend.labels.map((label, i) => {
					const positive = Number(trend.positive[i] ?? 0) || 0;
					const neutral = Number(trend.neutral[i] ?? 0) || 0;
					const negative = Number(trend.negative[i] ?? 0) || 0;
					const total = positive + neutral + negative;
					return { label, positive, neutral, negative, total };
				})
			: []
	);

	function computeSummary(source: Row[]) {
		const total = source.reduce((acc, row) => acc + Number(row.total || 0), 0);
		const avg = source.length > 0 ? total / source.length : 0;

		let bestPositive: Row | null = source[0] || null;
		let worstNegative: Row | null = source[0] || null;

		for (const row of source) {
			const posRatio = row.total > 0 ? row.positive / row.total : 0;
			const negRatio = row.total > 0 ? row.negative / row.total : 0;
			const bestRatio =
				bestPositive && bestPositive.total > 0 ? bestPositive.positive / bestPositive.total : -1;
			const worstRatio =
				worstNegative && worstNegative.total > 0
					? worstNegative.negative / worstNegative.total
					: -1;

			if (!bestPositive || posRatio > bestRatio) bestPositive = row;
			if (!worstNegative || negRatio > worstRatio) worstNegative = row;
		}

		return {
			total,
			avg,
			bestPositive,
			worstNegative
		};
	}

	let summary = $derived(computeSummary(rows));

	function formatTimestamp(value: string | null | undefined) {
		if (!value) return 'Not available';
		const dt = new Date(value);
		if (Number.isNaN(dt.getTime())) return value;
		return dt.toLocaleString();
	}

	function mixWidth(part: number, total: number) {
		return total > 0 ? (part / total) * 100 : 0;
	}

	function pct(part: number, total: number) {
		return total > 0 ? ((part / total) * 100).toFixed(1) : '0.0';
	}

	function safeNumber(value: number | string | undefined | null) {
		if (typeof value === 'number' && Number.isFinite(value)) return value;
		if (typeof value === 'string') {
			const parsed = Number(value);
			return Number.isFinite(parsed) ? parsed : 0;
		}
		return 0;
	}

	function renderCharts() {
		if (!ChartApi || !trend) return;

		const labels = $state.snapshot(trend.labels);
		const positive = $state.snapshot(trend.positive);
		const neutral = $state.snapshot(trend.neutral);
		const negative = $state.snapshot(trend.negative);
		const totals = labels.map(
			(_, i) => (positive[i] || 0) + (neutral[i] || 0) + (negative[i] || 0)
		);

		if (barCanvas) {
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

		if (lineCanvas) {
			if (lineChart) lineChart.destroy();
			lineChart = new ChartApi(lineCanvas, {
				type: 'line',
				data: {
					labels,
					datasets: [
						{
							label: 'Total reviews',
							data: totals,
							borderColor: '#2f81f7',
							backgroundColor: 'rgba(47, 129, 247, 0.2)',
							pointRadius: 3,
							tension: 0.25,
							fill: true
						}
					]
				},
				options: {
					responsive: true,
					maintainAspectRatio: false,
					plugins: { legend: { display: false } },
					scales: {
						x: { grid: { display: false } },
						y: { grid: { color: 'rgba(128,128,128,0.1)' } }
					}
				}
			});
		}
	}
</script>

<svelte:head>
	<title>Monthly Trends | Amazon Sentiment</title>
	<meta name="description" content="Long-term sentiment analysis and monthly performance trends." />
</svelte:head>

<div class="overview-page">
	<div class="flex justify-between items-center mb-6" style="margin-bottom: 2rem;">
		<div>
			<h1 style="font-size: 1.5rem; margin-bottom: 0.25rem;">Monthly Aggregation</h1>
			<p style="color: var(--fg-muted); font-size: 0.875rem;">
				Monthly sentiment totals built from the aggregation pipeline.
			</p>
		</div>
		<div class="flex items-center gap-4">
			<span style="font-size: 0.75rem; color: var(--fg-muted)">
				Last aggregation: {formatTimestamp(aggregationStatus?.monthly_last_aggregated_at)}
			</span>
			<button class="btn" onclick={loadData}>
				<RefreshCw size={14} class={loading ? 'animate-spin' : ''} style="margin-right: 0.5rem;" />
				Refresh
			</button>
		</div>
	</div>

	<div class="kpi-grid">
		<div class="kpi-card">
			<div class="kpi-label flex items-center gap-2">
				<BarChart3 size={14} /> Total Aggregated Reviews
			</div>
			<div class="kpi-value">{safeNumber(summary?.total).toLocaleString()}</div>
			<div class="kpi-trend" style="color: var(--fg-muted)">
				From {rows.length} monthly buckets
			</div>
		</div>
		<div class="kpi-card" style="border-left: 4px solid var(--accent-fg)">
			<div class="kpi-label flex items-center gap-2">
				<CalendarDays size={14} /> Average per Month
			</div>
			<div class="kpi-value">{safeNumber(summary?.avg).toFixed(0)}</div>
			<div class="kpi-trend" style="color: var(--fg-muted)">Mean monthly volume</div>
		</div>
		<div class="kpi-card" style="border-left: 4px solid var(--success-fg)">
			<div class="kpi-label flex items-center gap-2">
				<TrendingUp size={14} /> Best Positive Month
			</div>
			<div class="kpi-value">
				{summary?.bestPositive ? summary.bestPositive.label : '—'}
			</div>
			<div class="kpi-trend" style="color: var(--success-fg)">
				{summary?.bestPositive
					? pct(summary.bestPositive.positive, summary.bestPositive.total)
					: '0.0'}% positive
			</div>
		</div>
		<div class="kpi-card" style="border-left: 4px solid var(--danger-fg)">
			<div class="kpi-label flex items-center gap-2">
				<AlertTriangle size={14} /> Highest Negative Month
			</div>
			<div class="kpi-value">
				{summary?.worstNegative ? summary.worstNegative.label : '—'}
			</div>
			<div class="kpi-trend" style="color: var(--danger-fg)">
				{summary?.worstNegative
					? pct(summary.worstNegative.negative, summary.worstNegative.total)
					: '0.0'}% negative
			</div>
		</div>
	</div>

	<div style="display: grid; grid-template-columns: 2fr 1fr; gap: 1.5rem; margin-top: 1.5rem;">
		<div class="card">
			<div class="card-title">Sentiment Mix by Month</div>
			<div style="height: 320px;">
				<canvas bind:this={barCanvas}></canvas>
			</div>
		</div>
		<div class="card">
			<div class="card-title">Monthly Review Volume</div>
			<div style="height: 320px;">
				<canvas bind:this={lineCanvas}></canvas>
			</div>
		</div>
	</div>

	<div class="card">
		<div class="card-title">Monthly Sentiment Breakdown</div>
		<div class="table-container">
			<table>
				<thead>
					<tr>
						<th>Month</th>
						<th>Total</th>
						<th>Positive</th>
						<th>Neutral</th>
						<th>Negative</th>
						<th>Mix</th>
					</tr>
				</thead>
				<tbody>
					{#if rows.length === 0}
						<tr>
							<td colspan="6" style="text-align: center; padding: 2.5rem; color: var(--fg-muted);">
								{loading ? 'Loading monthly aggregation...' : 'No monthly aggregation data found.'}
							</td>
						</tr>
					{:else}
						{#each rows as row}
							<tr>
								<td class="mono">{row.label}</td>
								<td>{row.total.toLocaleString()}</td>
								<td style="color: var(--success-fg)">
									{row.positive.toLocaleString()} ({pct(row.positive, row.total)}%)
								</td>
								<td style="color: var(--attention-fg)">
									{row.neutral.toLocaleString()} ({pct(row.neutral, row.total)}%)
								</td>
								<td style="color: var(--danger-fg)">
									{row.negative.toLocaleString()} ({pct(row.negative, row.total)}%)
								</td>
								<td>
									<div
										style="display: flex; height: 8px; background: var(--canvas-inset); border-radius: 4px; overflow: hidden; min-width: 120px;"
									>
										<div
											style="width: {mixWidth(
												row.positive,
												row.total
											)}%; background: var(--success-fg);"
										></div>
										<div
											style="width: {mixWidth(
												row.neutral,
												row.total
											)}%; background: var(--attention-fg);"
										></div>
										<div
											style="width: {mixWidth(
												row.negative,
												row.total
											)}%; background: var(--danger-fg);"
										></div>
									</div>
								</td>
							</tr>
						{/each}
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
