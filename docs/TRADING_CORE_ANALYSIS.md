# opsense-rs Trading Core Analysis

> Generated via MCP codegraph semantic graph analysis
> Date: 2026-09-16
> Status: Document & analysis only — no code changes

## Overview

opsense-rs is a Rust workspace (13 crates) that serves as a **data stream and event stream platform**. The codebase was recently refactored from monolithic to multi-service architecture (PR #1). `opsense-qlib` contains self-contained quantitative trading logic. This analysis maps the boundary between **"build"** (core trading computation) and **"integrate"** (infrastructure plumbing) to identify what can be truncated while preserving core trading functionality.

The codegraph index contains: **2552 symbols across 171 files, 130 chains, 192 edges**.

---

## 1. Core Trading Layer — What to Keep

These are the self-contained, pure-computation types and functions in `opsense-qlib` that require **zero I/O, network, or storage** to execute. They form the complete trading computation engine.

### 1.1 Core Data Types (`opsense-qlib/src/`)

| Type | File | Description | Nature |
|------|------|-------------|--------|
| `CandleStick` | `candle.rs` | OHLCV data type with `update()` for tick-to-candle conversion | Pure data + math |
| `TradingGrid` | `grid.rs` | Grid of K price levels with weights, win probabilities, SL/TP | Pure computation (39 methods) |
| `Order` | `portfolio.rs` | Trade representation (entry/exit, PnL, T+N settlement) | Pure data |
| `Report` | `portfolio.rs` | Performance metrics (win_rate, sharpe, sortino, max_drawdown) | Pure computation |
| `Tick` | `tick.rs` | Raw market tick data | Pure data |

### 1.2 Trading Strategies (`opsense-qlib/src/strategies/`)

| Strategy | File | Trait Impl | Description |
|----------|------|------------|-------------|
| `GridStrategy` | `strategies/grid.rs` | `Strategy` | Fixed grid with Bayesian win-probability blending |
| `VolatilityAdaptiveGridStrategy` | `strategies/volatility_adaptive_grid.rs` | `Strategy` | ATR-based dynamic grid sizing |
| `Graph` | `graph.rs` | `Strategy` | ONNX genotype DAG — ML neuroevolution path |

**Strategy trait signature** (from `opsense-qlib/src/lib.rs:128`):
```rust
pub trait Strategy: Sync + Send {
    fn init(&self) -> Vec<f64>;
    async fn next(&self, current: u64) -> u64;
    async fn rebuild(&self, current_ts: u64, grids: &[TradingGrid], fetch: FetchFn<'_>, param: ParamFn<'_>) -> Result<Vec<TradingGrid>, Error>;
}
```

### 1.3 Pure Computation Functions (`opsense-qlib/src/portfolio.rs`)

These are the **absolute core** — functions that perform trading computation with zero external dependencies:

| Function | Line | Signature | Nature |
|----------|------|-----------|--------|
| `evaluate_grid_entries` | 862 | `(id, candle, plan, orders, fee_rate, kelly_fraction, base_capital, unlock_seq) -> Vec<OrderEvent>` | **Pure** |
| `check_order_exit` | 953 | `(order, candle, fee_rate, current_seq) -> Option<(f64, f64)>` | **Pure** |
| `calculate_order_size` | 1183 | `(win_p, sl_pct, fraction, base_capital) -> f64` | **Pure** (Kelly criterion) |
| `convert_order_history_into_report` | 1030 | `(orders: &[Order]) -> Report` | **Pure** |
| `block_id` | 1200 | `(ts: u64) -> i64` | **Pure** |
| `forward` | 543 | `(orders, history, lookback, from, to, params, fetch, notify) -> Result<(), Error>` | **Core simulation loop** (async, requires FetchFn) |

**`Portfolio::forward()`** is the main simulation engine. It orchestrates:
1. Rebuilding strategy at scheduled intervals
2. Fetching candles (via injected `FetchFn`)
3. Evaluating grid entries against price levels
4. Placing orders and checking exit conditions (SL/TP/T+N)
5. Computing PnL and generating Report
6. Running optimize() with random search + SGD

### 1.4 Trading Traits (`opsense-qlib/src/lib.rs`)

The trait-based dependency injection pattern is the clean separation boundary:

| Trait | Line | Purpose | Pure? |
|-------|------|---------|-------|
| `Strategy` | 128 | Trading decision logic | Interface only |
| `DataLoader` | 99 | Data fetching abstraction | Interface only |
| `Fee` | 141 | Cost model | Interface only |
| `Score` | 146 | Optimization objective | Interface only |
| `Calendar` | 151 | Market schedule/settlement | Interface only |
| `Extractor` | 115 | Feature extraction | Interface only |

### 1.5 ONNX Model Builders (`opsense-qlib/src/models/`)

| Model | File | Features | Outputs |
|-------|------|----------|---------|
| `MeanReversion` | `models/mean_reversion.rs` | SMA + RSI + ATR | 8 grid params + ATR |
| `MomentumBreakout` | `models/momentum_breakout.rs` | Donchian + ROC + ATR | 8 grid params + ATR |
| `TrendFollower` | `models/trend_follower.rs` | EMA fast + EMA slow + ATR + EMA diff | 8 grid params + ATR |

These are pure computation — generating ONNX graph bytes from indicator configurations.

---

## 2. Integration Layer — What Can Be Truncated/Simplified

These modules require I/O, network, storage, or external services. They are the "plumbing" that connects the trading core to the outside world.

### 2.1 Wire Protocol & Runner Infrastructure

| Crate | Purpose | Truncation Recommendation |
|-------|---------|---------------------------|
| `opsense-proto` | Protobuf definitions + frame codec | Replace with simple JSON gRPC or remove entirely — trading data doesn't need binary protobuf |
| `opsense-runner` | gRPC server spawning kernel processes | Not needed for core trading — the trading core runs inline; replace with direct execution |
| `opsense-kernel-echo` | Test fixture kernel | Remove |
| `opsense-kernel-python` | Python sidecar | Replace with embedded Python or direct execution |
| `opsense-kernel-julia` | Julia sidecar | Replace with embedded Julia or direct execution |

### 2.2 Infrastructure Plumbing in opsense-model

| Module | Purpose | Truncation Recommendation |
|--------|---------|---------------------------|
| `opsense-model/src/resolver.rs` | Postgres/MySQL, Redis, S3 connections | Replace with env-based config; trading doesn't need Infisical/S3 |
| `opsense-model/src/secret.rs` | Infisical secrets manager integration | Remove — use env variables |
| `opsense-model/src/cache.rs` | Redis-based pagination/caching | Replace with in-memory LRU |
| `opsense-model/src/entities/admin/` | Tenant, user, device, session tables | Keep only what's needed for auth; trading doesn't need full RBAC |

### 2.3 HTTP Server & API Layer (opsense binary)

| Module | File | Truncation Recommendation |
|--------|------|---------------------------|
| `opsense/src/serve.rs` | HTTP server setup (Sentry, OpenTelemetry, Prometheus) | Reduce to minimal trading endpoints only |
| `opsense/src/api/oauth/` | OAuth2 device flow, JWT, Dex | Externalize to auth provider; trading system doesn't need built-in OAuth |
| `opsense/src/session/` | Session management with Ed25519 | Simplify to token-based auth |
| `opsense/src/token.rs` | Token encryption/decryption | Simplify |
| `opsense/src/mcp/` | MCP server | Remove or stub |
| `opsense/src/repl/` | REPL interface | Keep as optional dev tool |
| `opsense/src/client/` | Client layer (GraphQL, gRPC, auth) | Simplify to trading-specific client |

### 2.4 Vector Runtime & Storage (opsense-mlib)

| Module | Purpose | Truncation Recommendation |
|--------|---------|---------------------------|
| `opsense-mlib/src/vector/` | Full data pipeline engine (23 source files) | Replace with channel-based pipeline for trading data |
| `opsense-mlib/src/storage/` | DuckDB, LMDB, Redis, SQLite backends | Use standard crate (e.g., `sled` or single backend) |
| `opsense-mlib/src/ahocorasick/` | Pattern matching | Remove unless needed |
| `opsense-mlib/src/bloom/` | Bloom filters | Remove unless needed |
| `opsense-mlib/src/radix/` | Radix tree | Remove unless needed |
| `opsense-mlib/src/sgd.rs` | SGD optimizer | Keep — used by Portfolio::optimize() |
| `opsense-mlib/src/transition.rs` | Transition analysis | Keep — used by GridStrategy |
| `opsense-mlib/src/grid.rs` | AnalysisGrid | Keep — used by trading strategies |

### 2.5 Pipeline Components (opsense-components)

| Module | Purpose | Truncation Recommendation |
|--------|---------|---------------------------|
| `opsense-components/src/http/` | HTTP source component | Keep only if needed for data feeds |
| `opsense-components/src/processor/` | Pipeline processors | Keep only trading-specific transforms |
| `opsense-components/src/station/` | Station types (timeseries, category, pattern) | Replace with trading station types |
| `opsense-components/src/signal/` | Signal processing | Keep if used for trading signals |
| `opsense-components/src/catalog/` | Component catalog | Keep minimal |

---

## 3. Dependency Chain Analysis

### 3.1 Internal Module Dependencies (from codegraph)

The codegraph dependency graph reveals the following internal dependency patterns:

```
opsense-qlib (trading core)
  ├── opsense-mlib (AnalysisGrid, TransitionAnalysis, SGDOptimizer, LruCache, jq)
  ├── opsense-macros (ONNX DSL macros: onnx_graph!, onnx_model!, ema_indicator!, etc.)
  └── opsense-model (LogLevel, Signal, TelemetryKind, TimeSeries)

opsense-core (infrastructure engine)
  ├── opsense-mlib, opsense-model
  ├── async-graphql, config, reqwest
  └── Provides: Config, Context, Station (Timeseries, Category, Pattern)

opsense (binary)
  ├── opsense-core, opsense-qlib, opsense-components
  ├── opsense-proto, opsense-model, opsense-runner, opsense-mlib
  ├── axum, tokio, sqlx, redis, aws-sdk-s3
  └── Provides: HTTP API, MCP server, CLI, REPL, gRPC runner
```

### 3.2 The Clean Separation Line

The **cleanest boundary** between "build" and "integrate" is:

```
┌─────────────────────────────────────────────────────────────────┐
│  BUILD LAYER (Pure Trading Computation — Zero I/O)              │
│                                                                 │
│  CandleStick ──┐                                                │
│  TradingGrid ──┤                                                │
│  Order ────────┤                                                │
│  Report ───────┤── Portfolio::forward() ──┐                      │
│                │                          │                      │
│  GridStrategy ─┤── Strategy::rebuild() ───┤                      │
│  VAGStrategy ──┤                          │                      │
│  Graph (ONNX) ─┤                          │                      │
│                │                          │                      │
│  Fee ──────────┤                          │                      │
│  Calendar ─────┤                          │                      │
│  Score ────────┘                          │                      │
│                                          │                      │
│  evaluate_grid_entries()                 │                      │
│  check_order_exit()                      │                      │
│  calculate_order_size()                  │                      │
│  convert_order_history_into_report()     │                      │
│                                          │                      │
│  FetchFn<'_> (injected closure)          │                      │
│  ParamFn<'_> (injected closure)          │                      │
│  NotifyFn<'_> (injected closure)         │                      │
└──────────────────────────────────────────┼──────────────────────┘
                                           │
                              ┌────────────┼────────────┐
                              │            │            │
                              ▼            ▼            ▼
┌──────────────────────────────────────────────────────────────┐
│  INTEGRATE LAYER (I/O, Network, Storage, Infrastructure)      │
│                                                              │
│  DataLoader trait ──→ QueryCandleSticks (HTTP client)         │
│  FromQueryCandleSticks (I/O data loader)                      │
│  OptCache (LMDB persistence)                                  │
│  BlockLru (LRU cache)                                         │
│  Auth/Token/Session (auth infrastructure)                     │
│  Server/API/MCP (HTTP/gRPC infrastructure)                    │
│  Vector runtime (data pipeline engine)                        │
│  Storage backends (DuckDB, Redis, SQLite, LMDB)               │
│  Opsense-runner (gRPC kernel management)                      │
│  Kernel sidecars (Python, Julia, echo)                        │
└──────────────────────────────────────────────────────────────┘
```

### 3.3 External Crate Dependencies by Crate

**opsense-qlib** (trading core) depends on:
- `opsense-mlib` — AnalysisGrid, TransitionAnalysis, SGDOptimizer, LruCache
- `opsense-macros` — ONNX DSL macros
- `opsense-model` — LogLevel, Signal, TelemetryKind
- `itertools`, `reqwest`, `lmdb`, `tract-onnx`, `tokio`, `serde`, `csv`, `prost`

The key observation: **opsense-qlib's core computation (Portfolio::forward, evaluate_grid_entries, check_order_exit, calculate_order_size) does NOT depend on reqwest, lmdb, or tract-onnx directly** — those are needed only by the integration layers (DataLoader, OptCache, Graph/ONNX compilation). The pure trading logic works with just `itertools`, `serde`, and `tokio` (for async).

---

## 4. Analysis of Remaining ~5% Unreviewed Code

Based on codegraph analysis of the git history and code structure, the remaining ~5% of unreviewed code consists of:

### 4.1 Phase 2 GraphQL Bridge (Largest Gap)
- **What**: GraphQL resolvers declared but not wired to `AppState.runner`
- **Type**: **Integrate** — wiring layer, not trading computation
- **Can be truncated?**: Yes — the types exist in the schema; resolver wiring is boilerplate
- **Action**: Document the wiring pattern; keep the type definitions; stub the resolvers

### 4.2 RCF Anomaly Detection Tuning
- **What**: `opsense-mlib/src/rcf.rs` — `outlier_scores_higher_than_smooth_points` test marked `#[ignore]`
- **Type**: **Build** — algorithm tuning
- **Can be truncated?**: Optional — RCF is not core to grid trading; only relevant if repurposing anomaly detection for trading signals
- **Action**: Document the issue; tune codisp formula per RRCF Guha et al. 2016 if needed

### 4.3 Phase 6 Completion Items
- **What**: `RemoteAuth::create_challenge` not wired, `POST /api/admin/v1/session/resolve` not implemented
- **Type**: **Integrate** — auth infrastructure
- **Can be truncated?**: Yes — auth can be externalized to a provider
- **Action**: Document as externalizable; don't implement

### 4.4 Config Gaps & Cleanup
- **What**: `conf/opsense.conf.toml` references `vector:8686` but no vector service exists; `.bak` test files
- **Type**: **Integrate** — infrastructure configuration
- **Can be truncated?**: Yes — clean up config; remove .bak files
- **Action**: Remove .bak files; update config

---

## 5. Integration Path — How Trading Core Plugs Into opsense

Since opsense is a **data stream and event stream platform**, the trading core integrates naturally:

### 5.1 Data Flow
```
Market Data Source → DataLoader (FetchFn) → Portfolio::forward() → Report
                                                         ↓
                                              TradingGrid update → Strategy::rebuild()
                                                         ↓
                                              OrderEvent → NotifyFn → Exchange
```

### 5.2 Pipeline Integration
The `backtest_executor` component is already typetag-registered in `opsense-qlib`. The opsense TOML pipeline config can reference it as a DAG node:

```toml
[[pipeline.components]]
type = "backtest_executor"
config = { strategy = "GridStrategy", grid_levels = 10, sl_pct = 0.02 }
```

### 5.3 What Needs to Change
To make opsense primarily a trading system:

1. **Pipeline configuration** shifts from infrastructure metrics to trading data flows
2. **GraphQL schema** adds trading-specific queries/mutations (positions, orders, portfolio)
3. **Station types** add trading stations (GridStation, OrderStation)
4. **MCP tools** add trading tools (`opsense_place_order`, `opsense_get_portfolio`)
5. **Runner kernels** need a trading strategy runtime environment

---

## 6. Summary: Build vs Integrate Classification

| Crate/Module | Classification | Keep? | Action |
|-------------|----------------|-------|--------|
| `opsense-qlib/src/candle.rs` | **Build** | ✅ Keep | None |
| `opsense-qlib/src/grid.rs` | **Build** | ✅ Keep | None |
| `opsense-qlib/src/portfolio.rs` (core methods) | **Build** | ✅ Keep | None |
| `opsense-qlib/src/strategies/` | **Build** | ✅ Keep | None |
| `opsense-qlib/src/models/` | **Build** | ✅ Keep | None |
| `opsense-qlib/src/fee.rs` | **Build** | ✅ Keep | None |
| `opsense-qlib/src/calendar.rs` | **Build** | ✅ Keep | None |
| `opsense-qlib/src/data_loader.rs` (traits) | **Build** (interface) | ✅ Keep | None |
| `opsense-qlib/src/ohcl.rs` (QueryCandleSticks) | **Integrate** | ⚠️ Simplify | Replace HTTP client with pluggable source |
| `opsense-qlib/src/opt_cache.rs` | **Integrate** | ⚠️ Simplify | Replace LMDB with in-memory or configurable |
| `opsense-mlib/src/grid.rs` | **Build** | ✅ Keep | None |
| `opsense-mlib/src/sgd.rs` | **Build** | ✅ Keep | None |
| `opsense-mlib/src/transition.rs` | **Build** | ✅ Keep | None |
| `opsense-mlib/src/vector/` | **Integrate** | ❌ Truncate | Replace with channel-based pipeline |
| `opsense-mlib/src/storage/` | **Integrate** | ⚠️ Simplify | Use single standard backend |
| `opsense-mlib/src/ahocorasick/`, `bloom/`, `radix/` | **Integrate** | ❌ Truncate | Remove unless needed |
| `opsense-proto` | **Integrate** | ❌ Truncate | Replace with JSON gRPC |
| `opsense-runner` | **Integrate** | ❌ Truncate | Replace with direct execution |
| `opsense-kernel-*` | **Integrate** | ❌ Truncate | Remove or replace |
| `opsense-model/src/resolver.rs`, `secret.rs` | **Integrate** | ❌ Truncate | Use env-based config |
| `opsense-model/src/cache.rs` | **Integrate** | ⚠️ Simplify | In-memory LRU |
| `opsense/src/serve.rs` | **Integrate** | ⚠️ Simplify | Minimal HTTP server |
| `opsense/src/api/oauth/` | **Integrate** | ❌ Truncate | Externalize auth |
| `opsense/src/session/`, `token.rs` | **Integrate** | ⚠️ Simplify | Token-based auth |
| `opsense/src/mcp/` | **Integrate** | ❌ Truncate | Remove or stub |
| `opsense/src/repl/` | **Integrate** | ⚠️ Keep | Optional dev tool |
| `opsense-components/` | **Integrate** | ⚠️ Simplify | Keep only trading transforms |
| `opsense-core/` (infrastructure) | **Integrate** | ⚠️ Simplify | Minimal config + trading stations |

---

## 7. Codegraph Query Evidence

The following queries were executed against the semantic graph to produce this analysis:

| Query | Tool | Result |
|-------|------|--------|
| Symbol search: Portfolio | `codegraph_search_symbol` | 3 results (struct + impl + module) |
| Symbol search: TradingGrid | `codegraph_search_symbol` | 3 results (struct + impl + Display) |
| Symbol search: GridStrategy | `codegraph_search_symbol` | 5 results (struct + impl + Strategy) |
| Symbol search: CandleStick | `codegraph_search_symbol` | 9 results (struct + impl + DataLoader) |
| Symbol search: forward (method) | `codegraph_search_symbol` | 2 results (forward + test) |
| Symbol search: evaluate_grid_entries | `codegraph_search_symbol` | 1 result |
| Symbol search: check_order_exit | `codegraph_search_symbol` | 6 results |
| Symbol search: calculate_order_size | `codegraph_search_symbol` | 3 results |
| Class detail: TradingGrid | `codegraph_graphcode_class` | 39 methods, all pure computation |
| Class detail: Portfolio | `codegraph_graphcode_class` | 18 methods (core + integrate) |
| Class detail: GridStrategy | `codegraph_graphcode_class` | 8 methods (Strategy impl) |
| Class detail: Graph | `codegraph_graphcode_class` | 20 methods (ONNX + Strategy) |
| Class detail: DataLoader trait | `codegraph_graphcode_class` | Trait interface at lib.rs:99 |
| Class detail: Strategy trait | `codegraph_graphcode_class` | Trait interface at lib.rs:128 |
| Function scope: forward | `codegraph_graphcode_function_scope` | 2 constants (KELLY_FRACTION, BASE_CAPITAL) |
| List interfaces | `codegraph_graphcode_list_types` | 28 interfaces including all trading traits |
| Dependency graph | `codegraph_graphcode_dependencies` | Internal/external module mapping |
| Index stats | `codegraph_graphcode_stats` | 2552 symbols, 171 files, 130 chains |

---

## 8. Conclusion

The opsense-rs codebase has a **clean, natural separation** between trading computation and infrastructure plumbing. The `opsense-qlib` crate provides a complete, self-contained trading engine with:

- **Pure computation**: `evaluate_grid_entries`, `check_order_exit`, `calculate_order_size`, `convert_order_history_into_report`, all `TradingGrid` methods, all `Strategy::rebuild` logic
- **Trait-based dependency injection**: `Strategy`, `DataLoader`, `Fee`, `Score`, `Calendar`, `Extractor` traits allow pluggable implementations
- **Zero I/O core**: The trading simulation loop runs entirely on injected closures (`FetchFn`, `ParamFn`, `NotifyFn`)

The infrastructure layer (HTTP servers, gRPC runners, kernel sidecars, storage backends, auth systems) is the "integrate" portion that can be truncated, simplified, or replaced with minimal trading-specific alternatives.

Since opsense is a **data stream and event stream platform** built from scratch, it is the ideal substrate for the trading system. The trading core plugs into opsense's pipeline DAG engine as the primary use case, with the existing typetag `Component` registration system already supporting `backtest_executor` and related trading components.
