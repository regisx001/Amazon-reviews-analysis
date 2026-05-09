<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/stores';
	import favicon from '$lib/assets/favicon.svg';
	import '../app.css';
	import { 
		LayoutDashboard, 
		TrendingUp, 
		Zap, 
		Settings as SettingsIcon,
		PackageSearch,
		MonitorPlay,
		Moon,
		Sun
	} from 'lucide-svelte';

	let { children } = $props();
	let currentPath = $derived($page.url.pathname);
	let isDark = $state(true);

	function toggleTheme() {
		isDark = !isDark;
		document.documentElement.setAttribute('data-theme', isDark ? 'dark' : 'light');
	}

	onMount(() => {
		// Default to dark theme for premium look
		document.documentElement.setAttribute('data-theme', 'dark');
	});
</script>

<svelte:head>
	<title>Amazon Sentiment Tracker</title>
	<meta name="description" content="Real-time sentiment analysis and analytics for Amazon product reviews." />
	<link rel="icon" href={favicon} />
</svelte:head>

<div class="dashboard-layout">
	<!-- Sidebar -->
	<aside class="sidebar">
		<div class="sidebar-brand">
			<Zap size={24} color="var(--accent-fg)" fill="var(--accent-fg)" />
			<span>Amazon <span style="color: var(--accent-fg)">Sentiment</span></span>
		</div>

		<nav class="nav-group">
			<div class="nav-label">Main Pipeline</div>
			<a href="/dashboard" class="nav-link" class:active={currentPath === '/dashboard'}>
				<MonitorPlay size={18} />
				<span>Live Monitor</span>
			</a>
			<a href="/recommendations" class="nav-link" class:active={currentPath === '/recommendations'}>
				<PackageSearch size={18} />
				<span>IA Recommendations</span>
			</a>
		</nav>

		<nav class="nav-group">
			<div class="nav-label">Insights</div>
			<a href="/dashboard/stats" class="nav-link" class:active={currentPath.includes('/stats')}>
				<TrendingUp size={18} />
				<span>Historical Analytics</span>
			</a>
		</nav>

		<div style="margin-top: auto; padding-top: 1rem; border-top: 1px solid var(--border-muted);">
			<button class="nav-link" style="width: 100%; border: none; background: transparent; cursor: pointer;" onclick={toggleTheme}>
				{#if isDark}
					<Sun size={18} />
					<span>Light Mode</span>
				{:else}
					<Moon size={18} />
					<span>Dark Mode</span>
				{/if}
			</button>
			<a href="/settings" class="nav-link" class:active={currentPath === '/settings'}>
				<SettingsIcon size={18} />
				<span>System Settings</span>
			</a>
		</div>
	</aside>

	<!-- Main Content Area -->
	<main class="main-container">
		<header class="header">
			<div style="font-weight: 500; font-size: 0.875rem;">
				{currentPath === '/dashboard' ? 'Real-time Monitoring' : 'Product Insights'}
			</div>
			<div class="flex items-center gap-4">
				<div class="badge badge-success" style="padding: 0.2rem 0.5rem; font-size: 0.65rem;">
					<div style="width: 6px; height: 6px; border-radius: 50%; background: currentColor; margin-right: 0.4rem; animation: pulse 2s infinite;"></div>
					STREAMING LIVE
				</div>
			</div>
		</header>

		<div class="content-wrapper">
			{@render children()}
		</div>
	</main>
</div>

<style>
	@keyframes pulse {
		0% { opacity: 1; }
		50% { opacity: 0.4; }
		100% { opacity: 1; }
	}
</style>
