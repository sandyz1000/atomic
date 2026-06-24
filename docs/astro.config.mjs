import { defineConfig } from 'astro/config'
import starlight from '@astrojs/starlight'
import starlightTypeDoc from 'starlight-typedoc'

export default defineConfig({
  integrations: [
    starlight({
      title: 'Atomic',
      description:
        'Atomic — a distributed compute engine for Rust, Python, and TypeScript.',
      logo: {
        alt: 'Atomic',
        light: './src/assets/logo-light.svg',
        dark: './src/assets/logo-dark.svg',
      },
      social: {
        github: 'https://github.com/sandyz1000/atomic',
      },
      editLink: {
        baseUrl: 'https://github.com/sandyz1000/atomic/edit/main/docs/src/content/docs/',
      },
      plugins: [
        starlightTypeDoc({
          entryPoints: ['../crates/atomic-js/index.d.ts'],
          tsconfig: '../crates/atomic-js/tsconfig.json',
          // Output lands in src/content/docs/reference/js/
          output: 'reference/js',
          typeDoc: {
            entryPointStrategy: 'resolve',
            name: 'atomic-js',
            includeVersion: false,
            excludePrivate: true,
            excludeInternal: true,
            readme: 'none',
          },
          sidebar: {
            label: 'JS API Reference',
            collapsed: true,
          },
        }),
      ],
      sidebar: [
        { label: 'Introduction', link: '/' },
        { label: 'About', link: '/about/' },
        {
          label: 'Guides',
          items: [
            { label: 'Getting Started', link: '/guides/getting-started/' },
            { label: 'SQL and DataFrames', link: '/guides/sql/' },
            { label: 'Streaming', link: '/guides/streaming/' },
            { label: 'Graph Processing', link: '/guides/graph/' },
            { label: 'Natural Language Queries', link: '/guides/nlq/' },
            { label: 'Distributed Subagents', link: '/guides/agent-step/' },
            { label: 'Configuration', link: '/guides/configuration/' },
            { label: 'Deployment', link: '/guides/deployment/' },
          ],
        },
        {
          label: 'Concepts',
          items: [{ label: 'The RDD API', link: '/concepts/rdd-api/' }],
        },
        {
          label: 'Architecture',
          items: [
            { label: 'Overview', link: '/architecture/overview/' },
            { label: 'Execution Model', link: '/architecture/execution-model/' },
            { label: 'Shuffle', link: '/architecture/shuffle/' },
          ],
        },
        {
          label: 'API Reference',
          items: [
            {
              label: 'Rust (docs.rs)',
              link: 'https://docs.rs/atomic-compute',
              attrs: { target: '_blank' },
            },
            {
              label: 'JS API Reference',
              autogenerate: { directory: 'reference/js' },
            },
          ],
        },
        { label: 'Roadmap', link: '/roadmap/' },
        { label: 'Contributing', link: '/contributing/' },
      ],
    }),
  ],
})
