import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  sourcemap: true,
  clean: true,
  target: 'node18',
  // ws is a real runtime dependency; keep it external so the consumer dedupes it.
  external: ['ws', '@camunda8/orchestration-cluster-api'],
});
