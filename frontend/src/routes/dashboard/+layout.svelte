<script lang="ts">
	import { browser } from '$app/environment';
	import { page } from '$app/state';
	import {
		LayoutDashboard,
		List,
		Package,
		Calendar,
		Activity,
		Sun,
		Moon,
		Search,
		ChevronRight,
		Github
	} from 'lucide-svelte';
	import { onMount } from 'svelte';

	let { children } = $props();

	let theme = $state<'light' | 'dark'>('dark');

	function initTheme() {
		if (!browser) return;
		const root = document.documentElement;
		const stored = root.getAttribute('data-theme');
		const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
		theme = (stored || (prefersDark ? 'dark' : 'light')) as 'light' | 'dark';
		root.setAttribute('data-theme', theme);
	}

	function toggleTheme() {
		theme = theme === 'dark' ? 'light' : 'dark';
		document.documentElement.setAttribute('data-theme', theme);
	}

	onMount(() => {
		initTheme();
	});

	const navItems = [
		{ label: 'Overview', href: '/dashboard', icon: LayoutDashboard },
		{ label: 'Monthly Aggregation', href: '/dashboard/monthly', icon: Calendar },
		{ label: 'Model Insights', href: '/dashboard/models', icon: Activity },
		{ label: 'Recent Predictions', href: '/dashboard/recent', icon: List }
	];

	let productIdSearch = $state('');

	function handleSearch(e: SubmitEvent) {
		e.preventDefault();
		if (productIdSearch.trim()) {
			window.location.href = `/dashboard/${productIdSearch.trim()}`;
		}
	}
</script>

<div class="dashboard-layout">
	<aside class="sidebar">
		<div class="sidebar-brand">
			<Github size={24} />
			<span>SentimentView</span>
		</div>

		<nav class="nav-group">
			<div class="nav-label">Analytics</div>
			{#each navItems as item}
				<a href={item.href} class="nav-link" class:active={page.url.pathname === item.href}>
					<item.icon size={18} />
					{item.label}
				</a>
			{/each}
		</nav>

		<div class="nav-group">
			<div class="nav-label">Search Product</div>
			<form onsubmit={handleSearch} class="flex gap-2" style="padding: 0 0.5rem">
				<input type="text" class="input" placeholder="Product ID..." bind:value={productIdSearch} />
			</form>
		</div>

		<div
			style="margin-top: auto; padding: 1rem 0.5rem; font-size: 0.75rem; color: var(--fg-muted);"
		>
			<div class="flex items-center gap-2">
				<div
					style="width: 8px; height: 8px; border-radius: 50%; background-color: var(--success-fg);"
				></div>
				MongoDB Connected
			</div>
		</div>
	</aside>

	<main class="main-container">
		<header class="header">
			<div class="flex items-center gap-2" style="font-size: 0.875rem; color: var(--fg-muted)">
				<span>SentimentView</span>
				<ChevronRight size={14} />
				<span style="color: var(--fg-default); font-weight: 500;">
					{page.url.pathname.split('/').filter(Boolean).pop()?.replace('[productId]', 'Product') ||
						'Dashboard'}
				</span>
			</div>

			<div class="flex items-center gap-4">
				<button class="theme-toggle" onclick={toggleTheme}>
					{#if theme === 'dark'}
						<Sun size={18} />
					{:else}
						<Moon size={18} />
					{/if}
				</button>
			</div>
		</header>

		<div class="content-wrapper">
			{@render children()}
		</div>
	</main>
</div>
