import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--dark', styles.heroBanner)} style={{padding: '5rem 0', background: 'linear-gradient(180deg, #0b0f19 0%, #111827 100%)'}}>
      <div className="container text--center">
        <div style={{marginBottom: '1rem'}}>
          <span className="badge badge--primary" style={{fontSize: '0.85rem', letterSpacing: '0.05em', textTransform: 'uppercase', padding: '0.35rem 0.75rem'}}>
            Production-Grade HFT Infrastructure
          </span>
        </div>
        <Heading as="h1" className="hero__title" style={{fontSize: '3.2rem', fontWeight: 800, color: '#f9fafb'}}>
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle" style={{maxWidth: '850px', margin: '1rem auto 2.5rem', color: '#9ca3af', fontSize: '1.25rem', lineHeight: '1.6'}}>
          Institutional ultra-low latency cryptocurrency market data feed platform in pure Rust.
          Sub-microsecond tick normalization, virtual AMM order book reconstruction, and sub-75ns shared memory IPC fan-out.
        </p>
        <div style={{display: 'flex', justifyContent: 'center', gap: '1rem', flexWrap: 'wrap'}}>
          <Link
            className="button button--primary button--lg"
            style={{fontWeight: 700, padding: '0.8rem 1.8rem'}}
            to="/docs/intro">
            Architecture Blueprint & Docs
          </Link>
          <Link
            className="button button--secondary button--lg"
            style={{fontWeight: 700, padding: '0.8rem 1.8rem'}}
            to="/docs/distribution/shared-memory">
            Shared Memory IPC (&lt; 75ns)
          </Link>
          <Link
            className="button button--outline button--secondary button--lg"
            style={{fontWeight: 700, padding: '0.8rem 1.8rem'}}
            to="/docs/clients/cpp-sdk">
            C++23 & Python SDKs
          </Link>
        </div>
      </div>
    </header>
  );
}

function LatencyMatrixSection() {
  return (
    <section style={{padding: '3rem 0', backgroundColor: '#0f172a', borderTop: '1px solid #1e293b', borderBottom: '1px solid #1e293b'}}>
      <div className="container">
        <div className="text--center" style={{marginBottom: '2rem'}}>
          <Heading as="h2" style={{color: '#f8fafc', fontSize: '2rem'}}>
            Measured Hardware Latency Matrix
          </Heading>
          <p style={{color: '#94a3b8'}}>
            100,000-sample live pipeline benchmark under hardware IEEE 1588 PTP timestamps
          </p>
        </div>
        <div className="table-responsive" style={{maxWidth: '900px', margin: '0 auto'}}>
          <table className="table" style={{width: '100%', color: '#f1f5f9', borderCollapse: 'collapse'}}>
            <thead>
              <tr style={{borderBottom: '2px solid #334155', textAlign: 'left'}}>
                <th style={{padding: '0.75rem 1rem'}}>Transport Protocol</th>
                <th style={{padding: '0.75rem 1rem'}}>Wire Format</th>
                <th style={{padding: '0.75rem 1rem'}}>Parsing Overhead</th>
                <th style={{padding: '0.75rem 1rem'}}>Median E2E Latency</th>
                <th style={{padding: '0.75rem 1rem'}}>99th Percentile</th>
              </tr>
            </thead>
            <tbody>
              <tr style={{borderBottom: '1px solid #1e293b'}}>
                <td style={{padding: '0.75rem 1rem', fontWeight: 600, color: '#38bdf8'}}>POSIX SHM Ring</td>
                <td style={{padding: '0.75rem 1rem'}}>64B Struct</td>
                <td style={{padding: '0.75rem 1rem'}}>0 ns (Direct Cache)</td>
                <td style={{padding: '0.75rem 1rem', fontWeight: 700, color: '#4ade80'}}>&lt; 75 ns</td>
                <td style={{padding: '0.75rem 1rem', color: '#4ade80'}}>&lt; 120 ns</td>
              </tr>
              <tr style={{borderBottom: '1px solid #1e293b'}}>
                <td style={{padding: '0.75rem 1rem', fontWeight: 600, color: '#818cf8'}}>WebSocket SBE</td>
                <td style={{padding: '0.75rem 1rem'}}>Binary Little-Endian</td>
                <td style={{padding: '0.75rem 1rem'}}>~15 ns</td>
                <td style={{padding: '0.75rem 1rem', fontWeight: 700, color: '#4ade80'}}>~2.16 µs</td>
                <td style={{padding: '0.75rem 1rem'}}>~4.80 µs</td>
              </tr>
              <tr>
                <td style={{padding: '0.75rem 1rem', fontWeight: 600, color: '#94a3b8'}}>WebSocket SIMD-JSON</td>
                <td style={{padding: '0.75rem 1rem'}}>Text JSON</td>
                <td style={{padding: '0.75rem 1rem'}}>~180 ns</td>
                <td style={{padding: '0.75rem 1rem'}}>~4.50 µs</td>
                <td style={{padding: '0.75rem 1rem'}}>~12.2 µs</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title} | Institutional HFT Market Data`}
      description="Chain-Market-Price: Ultra-low latency institutional cryptocurrency market data feed platform in pure Rust. Sub-microsecond tick normalization, virtual AMM order book reconstruction, and sub-75ns shared memory IPC.">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <LatencyMatrixSection />
      </main>
    </Layout>
  );
}
