<script lang="ts">
	import favicon from '$lib/assets/favicon.svg';
	import '../app.css';

	let { children } = $props();
</script>

<svelte:head>
	<title>Amazon Sentiment Analysis</title>
	<meta name="description" content="Real-time sentiment analysis and analytics for Amazon product reviews." />
	<link rel="icon" href={favicon} />
</svelte:head>

{@render children()}
