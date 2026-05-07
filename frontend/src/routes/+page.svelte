<script lang="ts">
  import { onMount } from 'svelte';
  import { goto } from '$app/navigation';

  onMount(() => {
    goto('/dashboard');
  });
</script>

<div class="flex items-center justify-center" style="height: 100vh;">
  <p style="color: var(--fg-muted);">Redirecting to dashboard...</p>
</div>
