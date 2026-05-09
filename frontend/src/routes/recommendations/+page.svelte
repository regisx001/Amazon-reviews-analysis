<script lang="ts">
	import { onMount } from 'svelte';
	import { fetchRecommendations, type RecommendationsResponse } from '$lib/api';
	import { 
		ThumbsUp, 
		ThumbsDown, 
		RefreshCw, 
		Package, 
		TrendingUp, 
		AlertOctagon,
		ArrowRight
	} from 'lucide-svelte';

	let data = $state<any>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let refreshInterval: any;

	async function loadRankings() {
		try {
			const res = await fetchRecommendations();
			data = res;
		} catch (e) {
			console.error(e);
			error = 'API Error: Connection failed.';
		} finally {
			loading = false;
		}
	}

	onMount(() => {
		loadRankings();
		// Auto refresh every 5 seconds for real-time feeling
		refreshInterval = setInterval(loadRankings, 5000);
	});

	import { onDestroy } from 'svelte';
	onDestroy(() => {
		if (refreshInterval) clearInterval(refreshInterval);
	});
</script>

<div class="reco-container">
	<!-- Page Header -->
	<div class="flex justify-between items-end mb-8" style="margin-bottom: 2.5rem;">
		<div>
			<div class="badge badge-neutral" style="margin-bottom: 0.5rem; text-transform: uppercase; letter-spacing: 1px; font-size: 0.65rem;">
				BUSINESS LOGIC : SMART INSIGHTS
			</div>
			<h1 style="font-size: 1.875rem; margin: 0; font-weight: 700;">
				Système de <span style="color: var(--accent-fg)">Recommandation par Sentiment</span>
			</h1>
			<p style="color: var(--fg-muted); margin-top: 0.5rem;">
				Analyse en temps réel de la performance produits basée sur l'IA textuelle.
			</p>
		</div>
		<button class="btn" onclick={loadRankings} disabled={loading} style="border-radius: 99px; padding: 0.6rem 1.25rem;">
			<RefreshCw size={16} class={loading ? 'animate-spin' : ''} style="margin-right: 0.5rem;" />
			Actualiser les Smart Insights
		</button>
	</div>

	{#if error}
		<div class="card" style="border: 1px solid var(--danger-fg); background: var(--danger-subtle);">
			<p style="color: var(--danger-fg); margin: 0;">{error}</p>
		</div>
	{:else if loading && !data}
		<div class="flex flex-col items-center justify-center" style="height: 400px; color: var(--fg-muted);">
			<RefreshCw size={32} class="animate-spin" style="margin-bottom: 1rem;" />
			<p>Calcul des indices de confiance en cours...</p>
		</div>
	{:else if data}
		<div class="reco-grid">
			<!-- TOP 10 RECOMMENDED -->
			<div class="reco-column">
				<div class="reco-column-header" style="color: var(--success-fg)">
					<ThumbsUp size={20} fill="currentColor" />
					<span>🏆 TOP 10 RECOMMANDÉS (PÉPITES)</span>
				</div>
				<p class="reco-subtitle">Produits avec le plus haut taux de satisfaction IA (min. 3 avis)</p>

				<div class="reco-list">
					{#each data.top_recommended as p, i}
						<div class="reco-item">
							<div class="reco-rank">{i + 1}</div>
							<div class="reco-info">
								<div class="reco-id mono">{p.product_id}</div>
								<div class="reco-summary">{p.summary || 'Aucune description'}</div>
								<div style="font-size: 0.65rem; color: var(--fg-subtle); margin-top: 0.25rem;">
									⭐ Moyenne Étoiles : {p.avg_human_score}/5 | 📊 Confiance : {p.total_reviews} avis
								</div>
							</div>
							<div class="reco-score success">
								{p.satisfaction_pct}% 🤩
							</div>
						</div>
					{:else}
						<div class="empty-state">Pas encore de pépites détectées.</div>
					{/each}
				</div>
			</div>

			<!-- QUALITY ALERTS -->
			<div class="reco-column">
				<div class="reco-column-header" style="color: var(--danger-fg)">
					<AlertOctagon size={20} fill="currentColor" />
					<span>⚠️ ALERTES QUALITÉ (FLOPS)</span>
				</div>
				<p class="reco-subtitle">Déception textuelle élevée : agir avant que la note ne chute</p>

				<div class="reco-list">
					{#each data.quality_alerts as p, i}
						<div class="reco-item">
							<div class="reco-rank">{i + 1}</div>
							<div class="reco-info">
								<div class="reco-id mono">{p.product_id}</div>
								<div class="reco-summary">{p.summary || 'Aucune description'}</div>
								<div style="font-size: 0.65rem; color: var(--fg-subtle); margin-top: 0.25rem;">
									⭐ Moyenne Étoiles : {p.avg_human_score}/5 | 📊 Confiance : {p.total_reviews} avis
								</div>
							</div>
							<div class="reco-score danger">
								{p.disappointment_pct}% 😡
							</div>
						</div>
					{:else}
						<div class="empty-state">Aucun flop critique détecté.</div>
					{/each}
				</div>
			</div>
		</div>

		<div class="footer-note">
			<div class="flex items-center gap-2">
				<TrendingUp size={14} />
				Calculé en temps réel à {new Date(data.generated_at).toLocaleTimeString()}
			</div>
			<div>Seuls les produits avec un indice de confiance ≥ 3 sont éligibles.</div>
		</div>
	{/if}
</div>

<style>
	.reco-container {
		max-width: 1200px;
		margin: 0 auto;
	}

	.reco-grid {
		display: grid;
		grid-template-columns: 1fr 1fr;
		gap: 2rem;
	}

	@media (max-width: 1024px) {
		.reco-grid {
			grid-template-columns: 1fr;
		}
	}

	.reco-column {
		background: var(--canvas-subtle);
		border: 1px solid var(--border-default);
		border-radius: var(--radius-lg);
		padding: 1.5rem;
		box-shadow: var(--shadow-md);
	}

	.reco-column-header {
		display: flex;
		align-items: center;
		gap: 0.75rem;
		font-weight: 800;
		font-size: 0.9rem;
		letter-spacing: 0.05em;
		margin-bottom: 0.5rem;
	}

	.reco-subtitle {
		font-size: 0.75rem;
		color: var(--fg-muted);
		margin-bottom: 1.5rem;
	}

	.reco-list {
		display: flex;
		flex-direction: column;
		gap: 0.75rem;
	}

	.reco-item {
		display: flex;
		align-items: center;
		gap: 1rem;
		padding: 0.75rem 1rem;
		background: var(--canvas-default);
		border: 1px solid var(--border-muted);
		border-radius: var(--radius-md);
		transition: transform 0.2s, border-color 0.2s;
	}

	.reco-item:hover {
		transform: translateX(4px);
		border-color: var(--accent-fg);
	}

	.reco-rank {
		font-size: 0.75rem;
		font-weight: 700;
		color: var(--fg-subtle);
		width: 1.5rem;
	}

	.reco-info {
		flex: 1;
		min-width: 0;
	}

	.reco-id {
		font-size: 0.85rem;
		font-weight: 600;
		color: var(--fg-default);
	}

	.reco-summary {
		font-size: 0.7rem;
		color: var(--fg-muted);
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.reco-score {
		font-weight: 700;
		font-size: 0.9rem;
		white-space: nowrap;
	}

	.reco-score.success { color: var(--success-fg); }
	.reco-score.danger { color: var(--danger-fg); }

	.footer-note {
		margin-top: 2rem;
		padding-top: 1rem;
		border-top: 1px solid var(--border-muted);
		display: flex;
		justify-content: space-between;
		font-size: 0.75rem;
		color: var(--fg-muted);
	}

	.empty-state {
		text-align: center;
		padding: 2rem;
		color: var(--fg-muted);
		font-size: 0.875rem;
	}

	.animate-spin {
		animation: spin 1s linear infinite;
	}

	@keyframes spin {
		from { transform: rotate(0deg); }
		to { transform: rotate(360deg); }
	}
</style>
