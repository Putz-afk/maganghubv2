import { defineConfig } from 'astro/config';

export default defineConfig({
  site: 'https://maganghubv2.vercel.app',
  output: 'static',
  build: {
    format: 'directory'
  }
});