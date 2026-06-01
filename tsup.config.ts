import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts', 'src/consumer.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  sourcemap: true,
  clean: true,
  splitting: false,
  treeshake: true,
  target: 'es2020',
  outDir: 'dist',
});
