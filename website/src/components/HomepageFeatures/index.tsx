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
    title: 'Live Coinbase Feed',
    badge: 'Real, public, no key',
    description: (
      <>
        Opt-in crate that connects to the Coinbase Exchange WebSocket, with heartbeat watchdog,
        sequence and trade-id gap detection, resubscribe and reconnect with backoff. Tested offline on a recorded capture.
      </>
    ),
  },
  {
    title: 'Measured In-Process Pipeline',
    badge: '~1.8 µs per message (CI VM)',
    description: (
      <>
        Parse, SPSC hand-offs, price ladder and fan-out to 50 clients on one thread, measured on a GitHub Actions runner.
        Not a wire-to-wire latency; raw output is committed under <code>results/</code>.
      </>
    ),
  },
  {
    title: 'Honest Status',
    badge: 'Real vs simulated',
    description: (
      <>
        Shared memory, kernel bypass and on-chain ingestion are simulated or design-only today.
        The README status table says which is which.
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
