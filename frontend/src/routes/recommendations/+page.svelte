<script lang="ts">
	import { onMount } from 'svelte';
	import { fetchRecommendations, type RecommendationsResponse } from '$lib/api';
	import { ThumbsUp, ThumbsDown, Info, ShoppingCart, AlertTriangle, RefreshCw, LayoutDashboard } from 'lucide-svelte';

	let data = $state<RecommendationsResponse | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);

	async function loadRankings() {
		loading = true;
		error = null;
		try {
			data = await fetchRecommendations();
		} catch (e) {
			console.error(e);
			error = 'Failed to load recommendations. Make sure the API is running and the DAG has been executed.';
		} finally {
			loading = false;
		}
	}

	onMount(() => {
		loadRankings();
	});

	function formatPct(val: number) {
		return val.toFixed(1);
	}
</script>

<div class="recommendations-page">
	<div class="flex justify-between items-center mb-6" style="margin-bottom: 2rem;">
		<div>
			<h1 style="font-size: 1.5rem; margin-bottom: 0.25rem;">IA Product Recommendations</h1>
			<p style="color: var(--fg-muted); font-size: 0.875rem;">
				Based on AI text analysis, not just star ratings.
			</p>
		</div>
		<div class="flex items-center gap-4">
			<a href="/dashboard" class="btn" style="text-decoration: none;">
				<LayoutDashboard size={14} style="margin-right: 0.5rem;" />
				Back to Dashboard
			</a>
			<button class="btn" onclick={loadRankings} disabled={loading}>
				<RefreshCw size={14} class={loading ? 'animate-spin' : ''} style="margin-right: 0.5rem;" />
				Refresh
			</button>
		</div>
	</div>

	{#if error}
		<div class="card" style="border-left: 4px solid var(--danger-fg); color: var(--danger-fg);">
			<div class="flex items-center gap-2">
				<AlertTriangle size={18} />
				<span>{error}</span>
			</div>
		</div>
	{:else if loading}
		<div class="flex justify-center items-center" style="height: 300px; color: var(--fg-muted);">
			<RefreshCw class="animate-spin" style="margin-right: 1rem;" />
			Analyzing sentiments and calculating rankings...
		</div>
	{:else if data}
		{#if data.status === 'no_data'}
			<div class="card" style="text-align: center; padding: 3rem;">
				<Info size={48} style="margin: 0 auto 1.5rem; color: var(--accent-fg);" />
				<h2 style="margin-bottom: 0.5rem;">No recommendations yet</h2>
				<p style="color: var(--fg-muted); margin-bottom: 1.5rem;">
					The weekly ranking DAG hasn't run yet.
				</p>
				<code style="display: block; padding: 1rem; background: var(--canvas-inset); border-radius: 6px;">
					airflow dags trigger weekly_product_ranking
				</code>
			</div>
		{:else}
			<div class="grid" style="display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem;">
				<!-- COLUMN 1: TOP RECOMMENDED (PEPITESS) -->
				<div class="card" style="border-top: 4px solid var(--success-fg);">
					<div class="card-title flex items-center gap-2" style="color: var(--success-fg); margin-bottom: 1.5rem;">
						<ThumbsUp size={20} />
						<span>🏆 Les Pépites IA (Top 10)</span>
					</div>
					<p style="font-size: 0.875rem; color: var(--fg-muted); margin-bottom: 1.5rem;">
						Products with the highest positive sentiment ratios (minimum {data.metadata.confidence_threshold} reviews).
					</p>

					<div class="table-container">
						<table>
							<thead>
								<tr>
									<th>Product ID</th>
									<th>Satisfaction</th>
									<th>Reviews</th>
								</tr>
							</thead>
							<tbody>
								{#each data.top_recommended as p}
									<tr>
										<td class="mono">
											<div class="flex items-center gap-2">
												<ShoppingCart size={14} style="color: var(--fg-muted);" />
												<a href="/dashboard/{p.product_id}">{p.product_id}</a>
											</div>
										</td>
										<td>
											<span style="font-weight: 600; color: var(--success-fg);">{formatPct(p.satisfaction_rate)}%</span>
										</td>
										<td>{p.total_reviews}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>

				<!-- COLUMN 2: QUALITY ALERTS (FLOPS) -->
				<div class="card" style="border-top: 4px solid var(--danger-fg);">
					<div class="card-title flex items-center gap-2" style="color: var(--danger-fg); margin-bottom: 1.5rem;">
						<ThumbsDown size={20} />
						<span>⚠️ Alertes Qualité (Flops 10)</span>
					</div>
					<p style="font-size: 0.875rem; color: var(--fg-muted); margin-bottom: 1.5rem;">
						Products with the highest dissatisfaction rates detected by AI analysis.
					</p>

					<div class="table-container">
						<table>
							<thead>
								<tr>
									<th>Product ID</th>
									<th>Déception</th>
									<th>Reviews</th>
								</tr>
							</thead>
							<tbody>
								{#each data.quality_alerts as p}
									<tr>
										<td class="mono">
											<div class="flex items-center gap-2">
												<AlertTriangle size={14} style="color: var(--fg-muted);" />
												<a href="/dashboard/{p.product_id}">{p.product_id}</a>
											</div>
										</td>
										<td>
											<span style="font-weight: 600; color: var(--danger-fg);">{formatPct(p.dissatisfaction_rate)}%</span>
										</td>
										<td>{p.total_reviews}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			</div>

			<div style="margin-top: 1.5rem; text-align: center; color: var(--fg-muted); font-size: 0.75rem;">
				Last calculated on {new Date(data.updated_at).toLocaleString()} | 
				Week: {data.week_label} | 
				Total eligible products scanned: {data.metadata.total_eligible}
			</div>
		{/if}
	{/if}
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
