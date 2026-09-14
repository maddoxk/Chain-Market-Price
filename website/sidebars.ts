import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    {
      type: 'doc',
      id: 'intro',
      label: 'Overview & Introduction',
    },
    {
      type: 'category',
      label: 'Core Architecture',
      collapsed: false,
      items: [
        'architecture/mechanical-sympathy',
        'architecture/telemetry',
      ],
    },
    {
      type: 'category',
      label: 'Ingestion Pipelines',
      collapsed: false,
      items: [
        'ingestion/cefi-pipelines',
        'ingestion/onchain-mempool',
        'ingestion/kernel-bypass',
      ],
    },
    {
      type: 'category',
      label: 'AMM & DeFi Virtualization',
      collapsed: false,
      items: [
        'amm/constant-product',
        'amm/concentrated-liquidity',
        'amm/curve-stableswap',
        'amm/v4-hooks',
      ],
    },
    {
      type: 'category',
      label: 'Distribution & Wire Transports',
      collapsed: false,
      items: [
        'distribution/shared-memory',
        'distribution/websocket-sbe',
        'distribution/topic-routing',
      ],
    },
    {
      type: 'category',
      label: 'Client SDKs & Integration',
      collapsed: false,
      items: [
        'clients/cpp-sdk',
        'clients/python-sdk',
        'clients/rust-integration',
      ],
    },
    {
      type: 'category',
      label: 'Production Ops & Bare-Metal',
      collapsed: false,
      items: [
        'ops/bare-metal-hardware',
        'ops/kernel-tuning',
        'ops/systemd-service',
        'ops/preflight-validation',
      ],
    },
    {
      type: 'category',
      label: 'Performance & Benchmarks',
      collapsed: false,
      items: [
        'benchmarks/tick-to-egress',
      ],
    },
    {
      type: 'category',
      label: 'API & Protocol Reference',
      collapsed: true,
      items: [
        'api/sbe-schema',
        'api/shm-layout',
      ],
    },
  ],
};

export default sidebars;
