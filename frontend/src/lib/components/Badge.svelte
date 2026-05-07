<script lang="ts">
  let { sentiment, children } = $props();

  function getBadgeClass(s: string | undefined) {
    const val = s?.toLowerCase();
    if (val === 'positive') return 'badge-success';
    if (val === 'negative') return 'badge-danger';
    if (val === 'neutral') return 'badge-attention';
    return 'badge-neutral';
  }
</script>

<span class="badge {getBadgeClass(sentiment)}">
  {#if children}
    {@render children()}
  {:else}
    {sentiment}
  {/if}
</span>
