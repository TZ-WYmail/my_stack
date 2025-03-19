<template>
  <div class="container-main">
    <div class="name-label">
      <slot name="heading"></slot>
    </div>
    <div class="container">
      <slot name="introduction" class="introduction"></slot>
      <slot name="parameters_input" class="parameters-input"></slot>
      <slot name="chart"></slot>
    </div>
  </div>
</template>

<style scoped>
  .container-main {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    flex-basis: max-content;
    -webkit-filter: drop-shadow(10px 10px 10px rgba(0,0,0,.2));
    caret-color: transparent;

  }

  .introduction {
    color: var(--color-text);

  }

  .parameters-input {
    display: flex;

  }

  .name-label {
    background-color: var(--color-background-soft-hover);
    caret-color: transparent;
    flex-basis: 40px;
    width: max-content;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 0.1em 1em 0.1em 1em;
    border-top: 3px solid var(--color-border);
    border-left: 3px solid var(--color-border);
    border-right: 3px solid var(--color-border);
    font-size: 1.4rem;
    font-weight: 500;
    color: var(--color-heading);
  }

  .container {
    border: 3px solid var(--color-border);
    display: flex;
    justify-content: flex-start;
    flex-direction: column;
    gap: 25px;
    flex-basis: max-content;
    width: 650px;
    padding: 1.3em 2em 1.3em 2em;
    background-color: var(--color-background-soft-hover);
  }
</style>