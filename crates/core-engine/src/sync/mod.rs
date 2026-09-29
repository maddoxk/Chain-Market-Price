//! # Synchronization & Wait Strategies Module

pub mod wait_strategy;

pub use wait_strategy::{
    AdaptiveBurstWaitStrategy, BusySpinStrategy, DynamicWaitStrategy, HybridAdaptiveStrategy,
    PowerEfficientStrategy, WaitStrategy, WaitStrategyKind,
};
