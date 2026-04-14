# CxCluster — GitHub Copilot Workspace Instructions

## Project overview

CxCluster is the distributed cluster orchestration layer for CxLLM Studio.
Namespace: `CxAI\CxPHP\Cluster\` → `src/Cluster/`

## Repository structure

```
src/Cluster/
  ClusterOrchestrator.php    MongoDB-backed node management, routing, failover
  HealthWatchdog.php         Continuous health monitoring
  EventDispatcher.php        Inter-component event bus
  AgenticFlowEngine.php      Multi-step agentic workflow orchestration
  AutonomousScheduler.php    Job scheduling (priority queues, cron, retry)
  AutonomousOrchestrator.php High-level autonomous task orchestration
  NvidiaContainer.php        NVIDIA GPU runtime abstraction
  ChatAutomationBridge.php   Automation trigger binding
```

## Code conventions

- PHP 8.0+ minimum; PHP 8.3 in production
- PSR-4 autoloading: `CxAI\CxPHP\Cluster\` → `src/Cluster/`
- `declare(strict_types=1)` in all files
- Readonly constructor properties for value objects
- MongoDB via `CxLLM` singleton (no direct MongoClient usage)
- No direct `$_ENV`/`$_SERVER` — use `env()` helper or constructor injection
- Factory pattern via `::fromEnv()` static constructors

## Key patterns

- **Singleton**: `ClusterOrchestrator::fromEnv()`, `CxLLM::getInstance()`
- **Observer**: `EventDispatcher` decouples components via named events
- **Strategy**: Health check strategies (latency, availability, GPU utilization)
- **Bridge**: `ChatAutomationBridge` connects chat events → automation pipeline

## Dependencies

- `CxAI\CxPHP\Database\CxLLM` — MongoDB connector (from CxDB)
- `CxAI\CxPHP\Acp\CxModelBridge` — Model routing (from CxPHP)
- No external CPAN/Composer dependencies beyond the CxPHP monorepo
