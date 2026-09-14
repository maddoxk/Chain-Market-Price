import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Chain-Market-Price',
  tagline: 'Institutional Ultra-Low Latency CeFi & DeFi Market Data Platform',
  favicon: 'img/favicon.ico',

  // Production URL and GitHub Pages deployment configuration
  url: 'https://maddoxk.github.io',
  baseUrl: '/Chain-Market-Price/',
  organizationName: 'maddoxk',
  projectName: 'Chain-Market-Price',
  deploymentBranch: 'gh-pages',
  trailingSlash: false,

  onBrokenLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/maddoxk/Chain-Market-Price/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/docusaurus-social-card.jpg',
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Chain-Market-Price',
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          to: '/docs/architecture/mechanical-sympathy',
          label: 'Architecture',
          position: 'left',
        },
        {
          to: '/docs/distribution/shared-memory',
          label: 'Shared Memory IPC',
          position: 'left',
        },
        {
          to: '/docs/clients/cpp-sdk',
          label: 'Client SDKs',
          position: 'left',
        },
        {
          to: '/docs/ops/bare-metal-hardware',
          label: 'Production Ops',
          position: 'left',
        },
        {
          href: 'https://github.com/maddoxk/Chain-Market-Price',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Core Engine',
          items: [
            {
              label: 'Architecture Overview',
              to: '/docs/intro',
            },
            {
              label: 'Mechanical Sympathy',
              to: '/docs/architecture/mechanical-sympathy',
            },
            {
              label: 'Nanosecond Telemetry',
              to: '/docs/architecture/telemetry',
            },
          ],
        },
        {
          title: 'Integration & SDKs',
          items: [
            {
              label: 'POSIX Shared Memory (SHM)',
              to: '/docs/distribution/shared-memory',
            },
            {
              label: 'Modern C++23 Client',
              to: '/docs/clients/cpp-sdk',
            },
            {
              label: 'Python 3.11+ MMap Feed',
              to: '/docs/clients/python-sdk',
            },
            {
              label: 'SBE Binary Framing',
              to: '/docs/distribution/websocket-sbe',
            },
          ],
        },
        {
          title: 'Production Operations',
          items: [
            {
              label: 'Bare-Metal Hardware BOM',
              to: '/docs/ops/bare-metal-hardware',
            },
            {
              label: 'Linux Kernel & Sysctl Tuning',
              to: '/docs/ops/kernel-tuning',
            },
            {
              label: 'Systemd Real-Time Service',
              to: '/docs/ops/systemd-service',
            },
            {
              label: 'Pre-Flight Validator',
              to: '/docs/ops/preflight-validation',
            },
          ],
        },
        {
          title: 'Community & Source',
          items: [
            {
              label: 'GitHub Repository',
              href: 'https://github.com/maddoxk/Chain-Market-Price',
            },
            {
              label: 'Latency Benchmarks',
              to: '/docs/benchmarks/tick-to-egress',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Chain-Market-Price Platform. Released under Apache-2.0 / MIT. Institutional-Grade HFT Engineering.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['rust', 'cpp', 'python', 'bash', 'json', 'toml', 'ini'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
