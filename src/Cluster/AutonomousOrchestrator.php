<?php

declare(strict_types=1);

namespace CxAI\CxPHP\Cluster;

use CxAI\CxPHP\Acp\CxModelBridge;
use CxAI\CxPHP\Acp\ModelRouter;
use CxAI\CxPHP\Acp\RateLimiter;
use CxAI\CxPHP\Database\CxLLM;
use CxAI\CxPHP\Session\SessionManager;

/**
 * Autonomous Orchestrator — the unified self-managing controller for CxPHP.
 *
 * Ties together all autonomous subsystems into a cohesive, zero-human-intervention
 * operational loop:
 *
 *   - Health watchdog: continuous model/cluster health monitoring
 *   - Self-healing: automatic failover, circuit breaking, recovery
 *   - Autonomous scheduling: cron-free task execution
 *   - Flow orchestration: multi-step LLM pipelines
 *   - Model auto-routing: capability-based selection with cost optimization
 *   - Event-driven automation: reactive task triggering
 *
 * The orchestrator can run as:
 *   1. Inline (per-request lifecycle) — attach to Gateway
 *   2. Daemon (long-running process) — standalone tick loop
 *   3. Hybrid — inline + periodic background ticks
 *
 * No external cron or human intervention required.
 */
class AutonomousOrchestrator
{
    /** Orchestrator state collection. */
    private const COLL_STATE = 'orchestrator_state';

    /** Minimum tick interval (seconds) to prevent CPU spin. */
    private const MIN_TICK_INTERVAL = 5;

    /** Default tick interval (seconds). */
    private const DEFAULT_TICK_INTERVAL = 30;

    /** Maximum consecutive health check failures before escalation. */
    private const ESCALATION_THRESHOLD = 3;

    /** Circuit breaker states. */
    private const CB_CLOSED    = 'closed';
    private const CB_OPEN      = 'open';
    private const CB_HALF_OPEN = 'half_open';

    /** @var array<string, array{state: string, failures: int, last_failure: ?string, opened_at: ?string, half_open_successes: int}> */
    private array $circuitBreakers = [];

    /** @var array<string, array{healthy: bool, last_check: float, latency_ms: int, consecutive_failures: int}> */
    private array $modelHealth = [];

    /** @var float Last tick timestamp. */
    private float $lastTick = 0;

    /** @var int Tick interval in seconds. */
    private int $tickInterval;

    /** @var bool Whether the orchestrator is running in daemon mode. */
    private bool $isDaemon = false;

    /** @var bool Stop signal for daemon mode. */
    private bool $shouldStop = false;

    /** @var list<array{timestamp: string, event: string, detail: string}> */
    private array $auditLog = [];

    /** @var int Max audit log entries in memory. */
    private int $maxAuditEntries = 200;

    public function __construct(
        private readonly CxLLM $db,
        private readonly ClusterOrchestrator $cluster,
        private readonly AgenticFlowEngine $flowEngine,
        private readonly AutonomousScheduler $scheduler,
        private readonly SessionManager $sessions,
        private readonly ModelRouter $router,
        private readonly ?EventDispatcher $eventDispatcher = null,
        private readonly ?ChatAutomationBridge $chatAutomation = null,
    ) {
        $this->tickInterval = max(
            self::MIN_TICK_INTERVAL,
            (int) (getenv('ORCHESTRATOR_TICK_INTERVAL') ?: self::DEFAULT_TICK_INTERVAL),
        );
        $this->initializeCircuitBreakers();
    }

    /**
     * Build from environment — wires all subsystems automatically.
     */
    public static function fromEnv(SessionManager $sessions): self
    {
        $db      = CxLLM::getInstance();
        $cluster = new ClusterOrchestrator($db);
        $engine  = new AgenticFlowEngine($db, $cluster, $sessions);
        $sched   = new AutonomousScheduler($db, $cluster, $engine, $sessions);
        $router  = new ModelRouter();
        $disp    = new EventDispatcher();
        $disp->setScheduler($sched);
        $chat    = new ChatAutomationBridge($sched, $disp, $engine, $sessions);

        return new self($db, $cluster, $engine, $sched, $sessions, $router, $disp, $chat);
    }

    // ─── Initialization ─────────────────────────────────────────────────────

    /**
     * Bootstrap all autonomous subsystems for zero-intervention operation.
     *
     * Call once at startup to:
     *   1. Auto-register cluster nodes from config
     *   2. Register built-in agentic flows
     *   3. Schedule default autonomous tasks
     *   4. Register default chat automation triggers
     *   5. Run initial health check
     *
     * @return array{nodes: int, flows: int, tasks: int, triggers: int, health: array}
     */
    public function bootstrap(): array
    {
        $result = [
            'nodes'    => 0,
            'flows'    => 0,
            'tasks'    => 0,
            'triggers' => 0,
            'health'   => [],
        ];

        // 1. Auto-register all models as cluster nodes
        if ($this->cluster->isAvailable()) {
            $result['nodes'] = $this->cluster->autoRegisterFromConfig();
        }

        // 2. Register built-in agentic flows
        if ($this->db->isConfigured()) {
            $result['flows'] = $this->flowEngine->registerBuiltInFlows();
        }

        // 3. Schedule default autonomous tasks
        $result['tasks'] = $this->scheduleDefaultTasks();

        // 4. Register default chat triggers
        if ($this->chatAutomation !== null) {
            $result['triggers'] = $this->registerDefaultTriggers();
        }

        // 5. Initial health check
        $result['health'] = $this->runHealthCheck();

        $this->audit('bootstrap', json_encode($result) ?: '{}');

        return $result;
    }

    /**
     * Schedule the default autonomous tasks (health, cleanup, optimize).
     */
    private function scheduleDefaultTasks(): int
    {
        $count = 0;

        // Health check every 60 seconds
        if ($this->scheduler->scheduleTask(
            'auto-health-check',
            'Autonomous Health Check',
            AutonomousScheduler::TYPE_HEALTH,
            [],
            ['interval_seconds' => 60],
        )) {
            $count++;
        }

        // Cleanup old data every 6 hours
        if ($this->scheduler->scheduleTask(
            'auto-cleanup',
            'Autonomous Cleanup',
            AutonomousScheduler::TYPE_CLEANUP,
            ['retention_days' => 30],
            ['interval_seconds' => 21600],
        )) {
            $count++;
        }

        // Auto-optimize every 5 minutes
        if ($this->scheduler->scheduleTask(
            'auto-optimize',
            'Autonomous Optimization',
            AutonomousScheduler::TYPE_OPTIMIZE,
            [],
            ['interval_seconds' => 300],
        )) {
            $count++;
        }

        return $count;
    }

    /**
     * Register default chat automation triggers.
     */
    private function registerDefaultTriggers(): int
    {
        if ($this->chatAutomation === null) {
            return 0;
        }

        $count = 0;

        // High-latency trigger: auto-switch to faster model
        if ($this->chatAutomation->registerChatTrigger([
            'name'       => 'auto-latency-watchdog',
            'conditions' => ['latency_ms' => ['op' => 'gt', 'value' => 10000]],
            'action'     => ['type' => 'event', 'event' => 'model.latency_exceeded'],
            'priority'   => 100,
        ])) {
            $count++;
        }

        // High-token trigger: extract intelligence for long conversations
        if ($this->chatAutomation->registerChatTrigger([
            'name'       => 'auto-intelligence-extract',
            'conditions' => ['total_tokens' => ['op' => 'gt', 'value' => 5000]],
            'action'     => ['type' => 'event', 'event' => 'memory.auto_extract'],
            'priority'   => 50,
        ])) {
            $count++;
        }

        // Error trigger: auto-failover on provider errors
        if ($this->chatAutomation->registerChatTrigger([
            'name'       => 'auto-error-failover',
            'conditions' => ['error' => ['op' => 'exists', 'value' => true]],
            'action'     => ['type' => 'event', 'event' => 'model.auto_failover'],
            'priority'   => 200,
        ])) {
            $count++;
        }

        return $count;
    }

    // ─── Circuit Breaker ────────────────────────────────────────────────────

    /**
     * Initialize circuit breakers for all known models.
     */
    private function initializeCircuitBreakers(): void
    {
        $configPath = dirname(__DIR__, 2) . '/config/cx_models.php';
        if (!is_file($configPath)) {
            return;
        }

        try {
            $config = (array) (require $configPath);
        } catch (\Throwable) {
            return;
        }

        foreach (array_keys($config['models'] ?? []) as $modelId) {
            $this->circuitBreakers[(string) $modelId] = [
                'state'               => self::CB_CLOSED,
                'failures'            => 0,
                'last_failure'        => null,
                'opened_at'           => null,
                'half_open_successes' => 0,
            ];
            $this->modelHealth[(string) $modelId] = [
                'healthy'              => true,
                'last_check'           => 0.0,
                'latency_ms'           => 0,
                'consecutive_failures' => 0,
            ];
        }
    }

    /**
     * Check if a model's circuit breaker allows requests.
     */
    public function isModelAvailable(string $modelId): bool
    {
        $cb = $this->circuitBreakers[$modelId] ?? null;
        if ($cb === null) {
            return true; // No breaker = always available
        }

        if ($cb['state'] === self::CB_CLOSED) {
            return true;
        }

        if ($cb['state'] === self::CB_OPEN) {
            // Check recovery timeout (30 seconds)
            $openedAt = strtotime($cb['opened_at'] ?? 'now');
            if (time() - $openedAt > 30) {
                $this->circuitBreakers[$modelId]['state'] = self::CB_HALF_OPEN;
                $this->circuitBreakers[$modelId]['half_open_successes'] = 0;
                return true; // Allow a probe request
            }
            return false;
        }

        // Half-open: allow requests
        return true;
    }

    /**
     * Record a model success — closes circuit breaker if half-open.
     */
    public function recordModelSuccess(string $modelId, int $latencyMs = 0): void
    {
        $this->router->reportSuccess($modelId);

        if (!isset($this->circuitBreakers[$modelId])) {
            return;
        }

        $this->modelHealth[$modelId]['healthy'] = true;
        $this->modelHealth[$modelId]['latency_ms'] = $latencyMs;
        $this->modelHealth[$modelId]['last_check'] = microtime(true);
        $this->modelHealth[$modelId]['consecutive_failures'] = 0;

        if ($this->circuitBreakers[$modelId]['state'] === self::CB_HALF_OPEN) {
            $this->circuitBreakers[$modelId]['half_open_successes']++;
            if ($this->circuitBreakers[$modelId]['half_open_successes'] >= 3) {
                $this->circuitBreakers[$modelId]['state'] = self::CB_CLOSED;
                $this->circuitBreakers[$modelId]['failures'] = 0;
                $this->audit('circuit_breaker', "Model {$modelId}: closed (recovered)");
            }
        } elseif ($this->circuitBreakers[$modelId]['state'] === self::CB_OPEN) {
            // Should not happen, but handle gracefully
            $this->circuitBreakers[$modelId]['state'] = self::CB_HALF_OPEN;
        }
    }

    /**
     * Record a model failure — opens circuit breaker if threshold exceeded.
     */
    public function recordModelFailure(string $modelId, string $error = ''): void
    {
        $this->router->reportFailure($modelId, $error);

        if (!isset($this->circuitBreakers[$modelId])) {
            return;
        }

        $this->modelHealth[$modelId]['healthy'] = false;
        $this->modelHealth[$modelId]['last_check'] = microtime(true);
        $this->modelHealth[$modelId]['consecutive_failures']++;
        $this->circuitBreakers[$modelId]['failures']++;
        $this->circuitBreakers[$modelId]['last_failure'] = gmdate('c');

        $threshold = 5;
        if (
            $this->circuitBreakers[$modelId]['failures'] >= $threshold
            && $this->circuitBreakers[$modelId]['state'] === self::CB_CLOSED
        ) {
            $this->circuitBreakers[$modelId]['state'] = self::CB_OPEN;
            $this->circuitBreakers[$modelId]['opened_at'] = gmdate('c');
            $this->audit('circuit_breaker', "Model {$modelId}: OPEN (threshold={$threshold}, error={$error})");

            // Dispatch event for automation
            $this->eventDispatcher?->dispatch(EventDispatcher::EVENT_AGENT_INVOKED, [
                'event'    => 'model.circuit_open',
                'model_id' => $modelId,
                'error'    => $error,
                'failures' => $this->circuitBreakers[$modelId]['failures'],
            ]);
        }
    }

    /**
     * Get all circuit breaker states.
     *
     * @return array<string, array{state: string, failures: int, last_failure: ?string}>
     */
    public function getCircuitBreakerStates(): array
    {
        return $this->circuitBreakers;
    }

    // ─── AutoPilot Model Selection ──────────────────────────────────────────

    /**
     * Auto-select the best available model for a given task.
     *
     * The AutoPilot algorithm:
     *   1. Filter out models with open circuit breakers
     *   2. Apply capability requirements
     *   3. Route using configured strategy (failover, capability-match, etc.)
     *   4. Fall back through the fallback chain if primary is unavailable
     *
     * @param array $requirements Capability requirements for the task.
     * @return string The selected model ID.
     */
    public function autoSelectModel(array $requirements = []): string
    {
        // Add exclusion list for circuit-broken models
        $exclude = $requirements['exclude'] ?? [];
        foreach ($this->circuitBreakers as $modelId => $cb) {
            if ($cb['state'] === self::CB_OPEN) {
                $exclude[] = $modelId;
            }
        }
        $requirements['exclude'] = array_unique($exclude);

        return $this->router->route($requirements);
    }

    /**
     * Get an auto-piloted CxModelBridge with failover and circuit breaking.
     *
     * @param array $requirements Task requirements for model selection.
     * @return CxModelBridge The wired bridge.
     */
    public function getAutoPilotBridge(array $requirements = []): CxModelBridge
    {
        $modelId = $this->autoSelectModel($requirements);
        return CxModelBridge::forModel($modelId);
    }

    /**
     * Execute a prompt with full autonomous failover.
     *
     * Tries the best model, then walks the fallback chain on failure.
     * Records health data for circuit breaker updates.
     *
     * @param string $prompt     The prompt to send.
     * @param array  $history    Conversation history.
     * @param array  $options    Provider options.
     * @param array  $requirements Model selection requirements.
     *
     * @return array{model_id: string, response: string, usage: array, latency_ms: int, attempts: int, fallbacks_used: list<string>}
     */
    public function completeWithFailover(
        string $prompt,
        array $history = [],
        array $options = [],
        array $requirements = [],
    ): array {
        $modelId   = $this->autoSelectModel($requirements);
        $attempted = [];
        $maxAttempts = 6; // At most 6 models in the stack

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            if (in_array($modelId, $attempted, true)) {
                break; // Already tried this model
            }

            if (!$this->isModelAvailable($modelId)) {
                $attempted[] = $modelId;
                $modelId = $this->getNextFallback($modelId, $attempted);
                if ($modelId === '') {
                    break;
                }
                continue;
            }

            $attempted[] = $modelId;
            $startMs = (int) (microtime(true) * 1000);

            try {
                $bridge   = CxModelBridge::forModel($modelId);
                $session  = $this->sessions->create('autopilot');
                $response = $bridge->chat($session, $prompt, $options);
                $latencyMs = (int) (microtime(true) * 1000) - $startMs;

                if ($response->isSuccess()) {
                    $this->recordModelSuccess($modelId, $latencyMs);

                    return [
                        'model_id'       => $modelId,
                        'response'       => $response->getContent(),
                        'usage'          => $response->getUsage(),
                        'latency_ms'     => $latencyMs,
                        'attempts'       => $attempt,
                        'fallbacks_used' => array_slice($attempted, 1),
                    ];
                }

                // Provider returned an error response
                $this->recordModelFailure($modelId, $response->getErrorMessage() ?: 'Provider error');
            } catch (\Throwable $e) {
                $latencyMs = (int) (microtime(true) * 1000) - $startMs;
                $this->recordModelFailure($modelId, $e->getMessage());
            }

            // Try next fallback
            $modelId = $this->getNextFallback($modelId, $attempted);
            if ($modelId === '') {
                break;
            }
        }

        // All models exhausted
        return [
            'model_id'       => '',
            'response'       => '',
            'usage'          => [],
            'latency_ms'     => 0,
            'attempts'       => count($attempted),
            'fallbacks_used' => $attempted,
            'error'          => 'All models exhausted after ' . count($attempted) . ' attempts',
        ];
    }

    /**
     * Get the next fallback model, excluding already-attempted ones.
     */
    private function getNextFallback(string $currentModel, array $exclude): string
    {
        // Read fallback chain from config
        $configPath = dirname(__DIR__, 2) . '/config/cx_models.php';
        if (!is_file($configPath)) {
            return '';
        }

        try {
            $config = (array) (require $configPath);
        } catch (\Throwable) {
            return '';
        }

        $chain = $config['models'][$currentModel]['fallback_chain'] ?? [];
        foreach ($chain as $fallback) {
            if (!in_array($fallback, $exclude, true) && $this->isModelAvailable($fallback)) {
                return $fallback;
            }
        }

        // Try any available model
        foreach (array_keys($config['models'] ?? []) as $modelId) {
            if (!in_array($modelId, $exclude, true) && $this->isModelAvailable($modelId)) {
                return (string) $modelId;
            }
        }

        return '';
    }

    // ─── Health Watchdog ────────────────────────────────────────────────────

    /**
     * Run a full health check cycle across all models.
     *
     * @return array<string, array{healthy: bool, latency_ms: int, error?: string}>
     */
    public function runHealthCheck(): array
    {
        $results = [];
        $configPath = dirname(__DIR__, 2) . '/config/cx_models.php';
        if (!is_file($configPath)) {
            return $results;
        }

        try {
            $config = (array) (require $configPath);
        } catch (\Throwable) {
            return $results;
        }

        foreach (array_keys($config['models'] ?? []) as $modelId) {
            $modelId = (string) $modelId;

            try {
                $bridge = CxModelBridge::forModel($modelId);
                $provider = $this->extractProvider($bridge);

                if ($provider !== null) {
                    $health = $provider->healthCheck();
                    $results[$modelId] = $health;

                    if ($health['healthy']) {
                        $this->recordModelSuccess($modelId, $health['latency_ms'] ?? 0);
                    } else {
                        $this->recordModelFailure($modelId, $health['error'] ?? 'Health check failed');
                    }
                }
            } catch (\Throwable $e) {
                $results[$modelId] = [
                    'healthy'    => false,
                    'latency_ms' => 0,
                    'error'      => $e->getMessage(),
                ];
                $this->recordModelFailure($modelId, $e->getMessage());
            }
        }

        // Persist health check to MongoDB
        if ($this->db->isConfigured()) {
            try {
                $this->db->insertOne('health_checks', [
                    'type'       => 'autonomous',
                    'results'    => $results,
                    'checked_at' => gmdate('c'),
                ]);
            } catch (\Throwable) {
                // Non-critical
            }
        }

        return $results;
    }

    /**
     * Extract the ProviderInterface instance from a CxModelBridge.
     */
    private function extractProvider(CxModelBridge $bridge): ?\CxAI\CxPHP\Acp\Provider\ProviderInterface
    {
        // Use reflection to access the private provider property
        try {
            $ref = new \ReflectionClass($bridge);
            $prop = $ref->getProperty('provider');
            return $prop->getValue($bridge);
        } catch (\Throwable) {
            return null;
        }
    }

    // ─── Autonomous Tick Loop ───────────────────────────────────────────────

    /**
     * Perform one autonomous tick — called per-request or from daemon loop.
     *
     * The tick checks if enough time has elapsed since the last tick and, if so:
     *   1. Runs due scheduled tasks
     *   2. Checks model health for degraded models
     *   3. Processes half-open circuit breakers
     *
     * Safe to call frequently; rate-limits itself via tickInterval.
     *
     * @return array{ticked: bool, tasks_executed: int, health_checks: int, breakers_evaluated: int}
     */
    public function tick(): array
    {
        $now = microtime(true);

        if (($now - $this->lastTick) < $this->tickInterval) {
            return ['ticked' => false, 'tasks_executed' => 0, 'health_checks' => 0, 'breakers_evaluated' => 0];
        }

        $this->lastTick = $now;

        // 1. Run due scheduled tasks
        $tasksExecuted = 0;
        if ($this->db->isConfigured()) {
            try {
                $tasksExecuted = $this->scheduler->runDueTasks();
            } catch (\Throwable) {
                // Non-fatal
            }
        }

        // 2. Check health for models with open or half-open circuit breakers
        $healthChecks = 0;
        foreach ($this->circuitBreakers as $modelId => $cb) {
            if ($cb['state'] !== self::CB_CLOSED) {
                $healthChecks++;
                try {
                    $bridge   = CxModelBridge::forModel($modelId);
                    $provider = $this->extractProvider($bridge);
                    if ($provider !== null) {
                        $health = $provider->healthCheck();
                        if ($health['healthy']) {
                            $this->recordModelSuccess($modelId, $health['latency_ms'] ?? 0);
                        }
                    }
                } catch (\Throwable) {
                    // Model still unhealthy
                }
            }
        }

        // 3. Count breakers evaluated
        $breakersEvaluated = 0;
        foreach ($this->circuitBreakers as $cb) {
            if ($cb['state'] !== self::CB_CLOSED) {
                $breakersEvaluated++;
            }
        }

        return [
            'ticked'              => true,
            'tasks_executed'      => $tasksExecuted,
            'health_checks'       => $healthChecks,
            'breakers_evaluated'  => $breakersEvaluated,
        ];
    }

    /**
     * Run the orchestrator as a daemon (blocking loop).
     *
     * For use in a dedicated worker process (e.g., bin/orchestrator.php).
     * Runs until stop() is called or a signal is received.
     */
    public function runDaemon(): void
    {
        $this->isDaemon = true;
        $this->shouldStop = false;
        $this->audit('daemon', 'Orchestrator daemon started');

        $this->bootstrap();

        while (!$this->shouldStop) {
            $this->tick();
            usleep($this->tickInterval * 1_000_000);
        }

        $this->audit('daemon', 'Orchestrator daemon stopped');
    }

    /**
     * Signal the daemon to stop.
     */
    public function stop(): void
    {
        $this->shouldStop = true;
    }

    // ─── Status & Introspection ─────────────────────────────────────────────

    /**
     * Get comprehensive orchestrator status.
     *
     * @return array<string, mixed>
     */
    public function getStatus(): array
    {
        $availableModels = [];
        $unavailableModels = [];

        foreach ($this->circuitBreakers as $modelId => $cb) {
            if ($cb['state'] === self::CB_CLOSED || $cb['state'] === self::CB_HALF_OPEN) {
                $availableModels[] = $modelId;
            } else {
                $unavailableModels[] = $modelId;
            }
        }

        return [
            'orchestrator'       => 'autonomous',
            'is_daemon'          => $this->isDaemon,
            'tick_interval'      => $this->tickInterval,
            'last_tick'          => $this->lastTick > 0 ? date('c', (int) $this->lastTick) : null,
            'models_available'   => $availableModels,
            'models_unavailable' => $unavailableModels,
            'circuit_breakers'   => $this->circuitBreakers,
            'model_health'       => $this->modelHealth,
            'routing_strategy'   => $this->router->getStrategy(),
            'default_model'      => $this->router->getDefaultModel(),
            'fallback_model'     => $this->router->getFallbackModel(),
            'scheduler_tasks'    => count($this->scheduler->listTasks()),
            'audit_log_size'     => count($this->auditLog),
        ];
    }

    /**
     * Get the audit log.
     *
     * @return list<array{timestamp: string, event: string, detail: string}>
     */
    public function getAuditLog(): array
    {
        return $this->auditLog;
    }

    /**
     * Get model health map.
     *
     * @return array<string, array{healthy: bool, last_check: float, latency_ms: int, consecutive_failures: int}>
     */
    public function getModelHealth(): array
    {
        return $this->modelHealth;
    }

    // ─── Autonomous Execution ───────────────────────────────────────────────

    /**
     * Execute an autonomous flow with full orchestration.
     *
     * Combines:
     *   - Model auto-selection per step
     *   - Circuit breaker protection
     *   - Automatic failover
     *   - Event dispatching
     *
     * @param string $flowId  Flow to execute.
     * @param string $input   Initial input/prompt.
     * @return array  Execution result from AgenticFlowEngine.
     */
    public function executeFlow(string $flowId, string $input): array
    {
        $this->audit('flow_start', "Flow {$flowId} started");

        $this->eventDispatcher?->dispatch(EventDispatcher::EVENT_FLOW_STARTED, [
            'flow_id' => $flowId,
            'input'   => mb_substr($input, 0, 200),
        ]);

        $result = $this->flowEngine->execute($flowId, $input);

        if ($result['status'] === 'completed') {
            $this->eventDispatcher?->dispatch(EventDispatcher::EVENT_FLOW_COMPLETED, [
                'flow_id'      => $flowId,
                'execution_id' => $result['execution_id'] ?? '',
                'total_tokens' => $result['total_tokens'] ?? 0,
            ]);
        } else {
            $this->eventDispatcher?->dispatch(EventDispatcher::EVENT_FLOW_FAILED, [
                'flow_id'      => $flowId,
                'execution_id' => $result['execution_id'] ?? '',
                'error'        => $result['error'] ?? 'unknown',
            ]);
        }

        $this->audit('flow_end', "Flow {$flowId}: {$result['status']}");
        return $result;
    }

    /**
     * Execute a prompt autonomously — auto-select model, failover, track health.
     *
     * This is the highest-level autonomous completion method.
     *
     * @param string $prompt       The prompt to process.
     * @param array  $requirements Optional capability requirements.
     * @param array  $options      Provider options (temperature, max_tokens, etc.).
     *
     * @return array{model_id: string, response: string, usage: array, latency_ms: int, attempts: int, fallbacks_used: list<string>}
     */
    public function complete(string $prompt, array $requirements = [], array $options = []): array
    {
        $result = $this->completeWithFailover($prompt, [], $options, $requirements);

        // Dispatch event
        $this->eventDispatcher?->dispatch(
            !empty($result['error']) ? EventDispatcher::EVENT_CHAT_FAILED : EventDispatcher::EVENT_CHAT_COMPLETED,
            [
                'model_id'   => $result['model_id'],
                'attempts'   => $result['attempts'],
                'latency_ms' => $result['latency_ms'],
            ],
        );

        // Tick after completion (inline mode)
        if (!$this->isDaemon) {
            $this->tick();
        }

        return $result;
    }

    // ─── Internal ───────────────────────────────────────────────────────────

    private function audit(string $event, string $detail): void
    {
        $this->auditLog[] = [
            'timestamp' => gmdate('c'),
            'event'     => $event,
            'detail'    => $detail,
        ];

        if (count($this->auditLog) > $this->maxAuditEntries) {
            $this->auditLog = array_slice($this->auditLog, -$this->maxAuditEntries);
        }
    }
}
