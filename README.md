# CxCluster

Distributed cluster orchestration, autonomous scheduling, health monitoring, and NVIDIA container management for [CxLLM Studio](https://github.com/CxAI-LLM/Studio).

## Overview

CxCluster is the PHP orchestration layer that manages distributed AI inference nodes, autonomous task scheduling, event-driven workflows, and NVIDIA GPU container orchestration. It provides the backbone for multi-node CxLLM Studio deployments.

## Components

| File | Purpose |
|------|---------|
| `ClusterOrchestrator.php` | MongoDB-backed node management, health checks, intelligent routing with automated failover |
| `HealthWatchdog.php` | Continuous health monitoring with configurable thresholds and alerting |
| `EventDispatcher.php` | Inter-component event bus for decoupled communication |
| `AgenticFlowEngine.php` | Orchestrate multi-step agentic workflows across nodes |
| `AutonomousScheduler.php` | Job scheduling with priority queues, retry logic, and cron expressions |
| `AutonomousOrchestrator.php` | High-level autonomous task orchestration and resource allocation |
| `NvidiaContainer.php` | NVIDIA GPU runtime abstraction (container lifecycle, GPU assignment) |
| `ChatAutomationBridge.php` | Automation trigger binding for chat-driven workflows |

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  ClusterOrchestrator                 │
│  ┌──────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │  Health   │  │   Event      │  │   Agentic     │  │
│  │ Watchdog  │  │  Dispatcher  │  │ Flow Engine   │  │
│  └────┬─────┘  └──────┬───────┘  └──────┬────────┘  │
│       │               │                 │            │
│  ┌────▼─────┐  ┌──────▼───────┐  ┌──────▼────────┐  │
│  │ Autonomous│  │    Chat      │  │   NVIDIA      │  │
│  │ Scheduler │  │  Automation  │  │  Container    │  │
│  │ + Orch.   │  │   Bridge     │  │  Manager      │  │
│  └───────────┘  └──────────────┘  └───────────────┘  │
└─────────────────────────┬───────────────────────────┘
                          │
                    ┌─────▼─────┐
                    │  MongoDB  │
                    │  (CxLLM)  │
                    └───────────┘
```

## Requirements

- PHP 8.0+ (8.3 recommended)
- MongoDB (via [CxDB](https://github.com/CxAI-LLM/CxDB) connector)
- NVIDIA GPU runtime (optional, for `NvidiaContainer`)

## Namespace

```
CxAI\CxPHP\Cluster\
```

PSR-4 autoloading: `CxAI\CxPHP\Cluster\` → `src/Cluster/`

## Installation

This package is part of the [CxPHP](https://github.com/CxAI-LLM/CxPHP) monorepo. To use standalone:

```php
use CxAI\CxPHP\Cluster\ClusterOrchestrator;

$orchestrator = ClusterOrchestrator::fromEnv();

// Register a node
$orchestrator->registerNode('node-1', [
    'host' => '10.0.0.1',
    'port' => 8080,
    'models' => ['cx-model-1.0', 'cx-model-2.0'],
    'gpu' => 'A100',
]);

// Route a request to the healthiest node
$node = $orchestrator->route('cx-model-2.0');
```

## Related Repositories

| Repository | Description |
|------------|-------------|
| [CxPHP](https://github.com/CxAI-LLM/CxPHP) | PHP 8.3 ACP Gateway + Perl MCP Engine (monorepo) |
| [CxDB](https://github.com/CxAI-LLM/CxDB) | MongoDB Atlas + MySQL + Supabase data layer |
| [CxAgentic](https://github.com/CxAI-LLM/CxAgentic) | Python AI agent with tool-use and NVIDIA NIM inference |
| [CxNode](https://github.com/CxAI-LLM/CxNode) | Node.js 22 SDK (Gateway + MCP dual-mode server) |
| [CxDock](https://github.com/CxAI-LLM/CxDock) | Docker infrastructure (10 services, multi-stage builds) |
| [CxDevOps](https://github.com/CxAI-LLM/CxDevOps) | CI/CD and deployment automation |

## License

[Apache-2.0](LICENSE)
