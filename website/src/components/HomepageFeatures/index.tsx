import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  badge: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Sub-75ns Shared Memory IPC',
    badge: '< 75 ns Read Latency',
    description: (
      <>
        Zero-copy POSIX shared memory ring buffer at <code>/dev/shm</code> for colocated HFT bots.
        Features modern C++23 (<code>_mm_pause</code>) and Python 3.11 memory-mapped struct unpackers.
      </>
    ),
  },
  {
    title: 'Unified CeFi & DeFi Engine',
    badge: 'Tick Normalization',
    description: (
      <>
        Simultaneously normalizes centralized exchange book tickers (Binance, OKX, Bybit) and
        virtualizes AMM curves (Uniswap v2, v3, v4, and Curve Stableswap with 256-bit math) into a single L2 ladder.
      </>
    ),
  },
  {
    title: 'Mechanical Sympathy & Zero Alloc',
    badge: '0 Bytes Heap Hot Path',
    description: (
      <>
        Strict 64-byte cache alignment (<code>alignas(64)</code>), lock-free LMAX Disruptor SPSC queues,
        and branchless bitmap topic routing ensure deterministic ~2.16µs wire egress latency.
      </>
    ),
  },
];

function Feature({title, badge, description}: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md" style={{paddingTop: '2rem', paddingBottom: '2rem'}}>
        <div style={{marginBottom: '1rem'}}>
          <span className="badge badge--success" style={{fontSize: '0.9rem', padding: '0.4rem 0.8rem'}}>
            {badge}
          </span>
        </div>
        <Heading as="h3">{title}</Heading>
        <p style={{lineHeight: '1.6'}}>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features} style={{padding: '3rem 0'}}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
