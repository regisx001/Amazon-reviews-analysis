<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { browser } from '$app/environment';
	import { page } from '$app/state';
	import { fetchProductScoring, type ProductScoringResponse } from '$lib/api';
	import { Package, Star, TrendingUp, AlertCircle, MessageSquare, ArrowLeft } from 'lucide-svelte';

	let productId = $derived(page.params.productId);
	let scoring = $state<ProductScoringResponse | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);

	let pieCanvas = $state<HTMLCanvasElement | null>(null);
	let ChartApi = $state<any>(null);
	let pieChart: any = null;

	$effect(() => {
		if (browser && productId && ChartApi) {
			loadScoring();
		}
	});

	async function loadScoring() {
		loading = true;
		error = null;
		try {
			// @ts-ignore
			scoring = await fetchProductScoring(productId);
		} catch (e) {
			error = 'Could not find data for this product.';
			console.error('Scoring load error:', e);
		} finally {
			loading = false;
		}
	}

	$effect(() => {
		if (scoring && pieCanvas && ChartApi) {
			renderChart();
		}
	});

	function renderChart() {
		if (!ChartApi || !scoring || !pieCanvas) return;
		if (pieChart) pieChart.destroy();

		pieChart = new ChartApi(pieCanvas, {
			type: 'pie',
			data: {
				labels: ['Positive', 'Neutral', 'Negative'],
				datasets: [
					{
						data: $state.snapshot([
							scoring.counts.positive,
							scoring.counts.neutral,
							scoring.counts.negative
						]),
						backgroundColor: ['#238636', '#d29922', '#f85149'],
						borderWidth: 2,
						borderColor:
							document.documentElement.getAttribute('data-theme') === 'dark' ? '#0d1117' : '#ffffff'
					}
				]
			},
			options: {
				responsive: true,
				maintainAspectRatio: false,
				plugins: {
					legend: { position: 'bottom' }
				}
			}
		});
	}

	onMount(async () => {
		if (browser) {
			const { Chart, registerables } = await import('chart.js');
			Chart.register(...registerables);
			ChartApi = Chart;
		}
	});

	onDestroy(() => {
		pieChart?.destroy();
	});
</script>

<svelte:head>
	<title>{productId} - Product Sentiment | Amazon Sentiment</title>
	<meta
		name="description"
		content="Detailed sentiment breakdown and review history for product {productId}."
	/>
</svelte:head>

<div class="product-page">
	<div class="mb-6" style="margin-bottom: 2rem;">
		<a
			href="/dashboard"
			class="flex items-center gap-2"
			style="font-size: 0.875rem; color: var(--fg-muted); margin-bottom: 1rem;"
		>
			<ArrowLeft size={14} /> Back to Overview
		</a>
		<div class="flex items-center gap-4">
			<div
				style="width: 48px; height: 48px; background: var(--canvas-subtle); border: 1px solid var(--border-default); border-radius: var(--radius-md); display: flex; align-items: center; justify-content: center; color: var(--accent-fg);"
			>
				<Package size={24} />
			</div>
			<div>
				<h1 style="font-size: 1.5rem; margin-bottom: 0.25rem;">Product Analysis</h1>
				<p class="mono" style="color: var(--fg-muted); font-size: 0.875rem;">{productId}</p>
			</div>
		</div>
	</div>

	{#if loading}
		<div class="flex items-center justify-center" style="padding: 5rem;">
			<div
				class="animate-spin"
				style="width: 32px; height: 32px; border: 4px solid var(--canvas-subtle); border-top-color: var(--accent-emphasis); border-radius: 50%;"
			></div>
		</div>
	{:else if error}
		<div
			class="card"
			style="border-color: var(--danger-fg); background: var(--danger-subtle); color: var(--danger-fg);"
		>
			<div class="flex items-center gap-2">
				<AlertCircle size={18} />
				{error}
			</div>
		</div>
	{:else if scoring}
		<div class="kpi-grid">
			<div class="kpi-card">
				<div class="kpi-label">Review Sample Size</div>
				<div class="kpi-value">{scoring.total}</div>
				<div class="kpi-trend" style="color: var(--fg-muted)">Total reviews for this product</div>
			</div>
			<div class="kpi-card">
				<div class="kpi-label">Sentiment Score</div>
				<div class="kpi-value">
					{((scoring.counts.positive / scoring.total) * 100).toFixed(0)}%
				</div>
				<div class="kpi-trend" style="color: var(--success-fg)">Positive rating</div>
			</div>
		</div>

		<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem;">
			<div class="card">
				<div class="card-title">Sentiment Breakdown</div>
				<div style="height: 350px;">
					<canvas bind:this={pieCanvas}></canvas>
				</div>
			</div>
			<div
				class="flex flex-direction-column gap-4"
				style="display: flex; flex-direction: column; gap: 1rem;"
			>
				<div
					class="card"
					style="margin-bottom: 0; flex: 1; display: flex; flex-direction: column; justify-content: center;"
				>
					<div class="kpi-label flex items-center gap-2">
						<TrendingUp size={16} color="var(--success-fg)" /> Positive Feedback
					</div>
					<div class="kpi-value" style="color: var(--success-fg)">
						{scoring.percentages.positive}%
					</div>
					<div class="kpi-trend">{scoring.counts.positive} total positive reviews</div>
				</div>
				<div
					class="card"
					style="margin-bottom: 0; flex: 1; display: flex; flex-direction: column; justify-content: center;"
				>
					<div class="kpi-label flex items-center gap-2">
						<MessageSquare size={16} color="var(--attention-fg)" /> Neutral Feedback
					</div>
					<div class="kpi-value" style="color: var(--attention-fg)">
						{scoring.percentages.neutral}%
					</div>
					<div class="kpi-trend">{scoring.counts.neutral} total neutral reviews</div>
				</div>
				<div
					class="card"
					style="margin-bottom: 0; flex: 1; display: flex; flex-direction: column; justify-content: center;"
				>
					<div class="kpi-label flex items-center gap-2">
						<AlertCircle size={16} color="var(--danger-fg)" /> Negative Feedback
					</div>
					<div class="kpi-value" style="color: var(--danger-fg)">
						{scoring.percentages.negative}%
					</div>
					<div class="kpi-trend">{scoring.counts.negative} total negative reviews</div>
				</div>
			</div>
		</div>
	{/if}
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
