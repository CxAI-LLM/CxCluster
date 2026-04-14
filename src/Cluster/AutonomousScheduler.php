<?php

declare(strict_types=1);

namespace CxAI\CxPHP\Cluster;

use CxAI\CxPHP\Database\CxLLM;
use CxAI\CxPHP\Session\SessionManager;

/**
 * Autonomous Scheduler — enables non-human intervention automated task execution.
 *
 * Provides:
 *   - Scheduled task execution (cron-like, interval-based)
 *   - Event-driven triggers (webhooks, health events, flow completions)
 *   - Self-healing automation (auto-recovery, retry with backoff)
 *   - Autonomous optimization (auto-tune parameters based on metrics)
 *   - Conditional workflow branching
 *
 * Tasks are stored in MongoDB `scheduled_tasks` collection and executed
 * by the scheduler daemon or via explicit runDueTasks() calls.
 */
class AutonomousScheduler
{
    /** MongoDB collection for scheduled tasks. */
    private const COLL_TASKS = 'scheduled_tasks';

    /** MongoDB collection for task execution history. */
    private const COLL_HISTORY = 'task_history';

    /** MongoDB collection for autonomous tuning parameters. */
    private const COLL_TUNING = 'autonomous_tuning';

    /** Maximum concurrent task executions. */
    private const MAX_CONCURRENT = 10;

    /** Default task timeout in seconds. */
    private const DEFAULT_TIMEOUT = 300;

    /** Exponential backoff base delay (ms). */
    private const BACKOFF_BASE_MS = 1000;

    /** Maximum backoff delay (ms). */
    private const BACKOFF_MAX_MS = 60000;

    /** Task types. */
    public const TYPE_FLOW       = 'flow';       // Execute an agentic flow
    public const TYPE_HEALTH     = 'health';     // Run health check cycle
    public const TYPE_AUTOMATION = 'automation'; // Run full automation cycle
    public const TYPE_CLEANUP    = 'cleanup';    // Cleanup old data
    public const TYPE_OPTIMIZE   = 'optimize';   // Auto-optimization task
    public const TYPE_CUSTOM     = 'custom';     // Custom callback

    /** Trigger types. */
    public const TRIGGER_SCHEDULE = 'schedule';  // Cron/interval
    public const TRIGGER_EVENT    = 'event';     // Event-driven
    public const TRIGGER_WEBHOOK  = 'webhook';   // External webhook
    public const TRIGGER_MANUAL   = 'manual';    // Manual invocation

    public function __construct(
        private readonly CxLLM $db,
        private readonly ClusterOrchestrator $cluster,
        private readonly AgenticFlowEngine $flowEngine,
        private readonly SessionManager $sessions,
    ) {}

    /**
     * Create from environment.
     */
    public static function fromEnv(SessionManager $sessions): self
    {
        $db      = CxLLM::getInstance();
        $cluster = new ClusterOrchestrator($db);
        $engine  = new AgenticFlowEngine($db, $cluster, $sessions);
        return new self($db, $cluster, $engine, $sessions);
    }

    // ─── Task scheduling ────────────────────────────────────────────────────

    /**
     * Schedule a new autonomous task.
     *
     * @param string $taskId      Unique task identifier.
     * @param string $name        Human-readable name.
     * @param string $type        One of TYPE_* constants.
     * @param array  $config      Task-specific configuration.
     * @param array  $schedule    Schedule definition (see below).
     * @param array  $triggers    Optional event triggers.
     *
     * Schedule format:
     *   - interval_seconds: Run every N seconds
     *   - cron: Cron expression (minute hour day month weekday)
     *   - run_at: ISO timestamp for one-time execution
     *   - timezone: IANA timezone (default UTC)
     *
     * @return bool  Success/failure.
     */
    public function scheduleTask(
        string $taskId,
        string $name,
        string $type,
        array $config = [],
        array $schedule = [],
        array $triggers = [],
    ): bool {
        $validTypes = [
            self::TYPE_FLOW,
            self::TYPE_HEALTH,
            self::TYPE_AUTOMATION,
            self::TYPE_CLEANUP,
            self::TYPE_OPTIMIZE,
            self::TYPE_CUSTOM,
        ];

        if (!in_array($type, $validTypes, true)) {
            return false;
        }

        try {
            $this->db->updateOne(self::COLL_TASKS, ['task_id' => $taskId], [
                '$set' => [
                    'task_id'           => $taskId,
                    'name'              => $name,
                    'type'              => $type,
                    'config'            => $config,
                    'schedule'          => $schedule,
                    'triggers'          => $triggers,
                    'is_enabled'        => true,
                    'is_running'        => false,
                    'last_run'          => null,
                    'next_run'          => $this->calculateNextRun($schedule),
                    'consecutive_fails' => 0,
                    'retry_delay_ms'    => 0,
                ],
                '$inc' => ['version' => 1],
            ]);
            $this->cluster->logEvent('task_scheduled', "Scheduled task: {$taskId}", ['type' => $type]);
            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * Get a scheduled task.
     *
     * @return array<string,mixed>|null
     */
    public function getTask(string $taskId): ?array
    {
        try {
            return $this->db->findOne(self::COLL_TASKS, ['task_id' => $taskId]);
        } catch (\Throwable) {
            return null;
        }
    }

    /**
     * List all scheduled tasks.
     *
     * @return list<array<string,mixed>>
     */
    public function listTasks(bool $enabledOnly = true): array
    {
        $filter = $enabledOnly ? ['is_enabled' => true] : [];
        try {
            return $this->db->find(self::COLL_TASKS, $filter, ['sort' => ['name' => 1]]);
        } catch (\Throwable) {
            return [];
        }
    }

    /**
     * Enable or disable a task.
     */
    public function setTaskEnabled(string $taskId, bool $enabled): bool
    {
        try {
            $this->db->updateOne(
                self::COLL_TASKS,
                ['task_id' => $taskId],
                ['$set' => ['is_enabled' => $enabled]],
                false,
            );
            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * Delete a scheduled task.
     */
    public function deleteTask(string $taskId): bool
    {
        try {
            $this->db->deleteOne(self::COLL_TASKS, ['task_id' => $taskId]);
            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    // ─── Task execution ─────────────────────────────────────────────────────

    /**
     * Run all due tasks. Returns the number of tasks executed.
     *
     * This is the main entry point for autonomous execution.
     * Call this from a cron job or daemon process.
     */
    public function runDueTasks(): int
    {
        $now = gmdate('c');

        try {
            $dueTasks = $this->db->find(self::COLL_TASKS, [
                'is_enabled'  => true,
                'is_running'  => false,
                'next_run'    => ['$lte' => $now],
            ], [
                'sort'  => ['next_run' => 1],
                'limit' => self::MAX_CONCURRENT,
            ]);
        } catch (\Throwable) {
            return 0;
        }

        $executed = 0;
        foreach ($dueTasks as $task) {
            $result = $this->executeTask((string) ($task['task_id'] ?? ''), self::TRIGGER_SCHEDULE);
            if ($result['status'] !== 'skipped') {
                $executed++;
            }
        }

        return $executed;
    }

    /**
     * Execute a specific task immediately (manual trigger).
     *
     * @return array{task_id: string, status: string, output: mixed, duration_ms: int, error: string|null}
     */
    public function executeTask(string $taskId, string $trigger = self::TRIGGER_MANUAL): array
    {
        $task = $this->getTask($taskId);
        if ($task === null || !($task['is_enabled'] ?? false)) {
            return $this->taskResult($taskId, 'skipped', null, 0, 'Task not found or disabled');
        }

        // Prevent concurrent execution of the same task
        if ($task['is_running'] ?? false) {
            return $this->taskResult($taskId, 'skipped', null, 0, 'Task already running');
        }

        // Mark as running
        try {
            $this->db->updateOne(
                self::COLL_TASKS,
                ['task_id' => $taskId],
                ['$set' => ['is_running' => true, 'started_at' => gmdate('c')]],
                false,
            );
        } catch (\Throwable) {
            return $this->taskResult($taskId, 'error', null, 0, 'Failed to acquire task lock');
        }

        $startMs = (int) (microtime(true) * 1000);
        $output  = null;
        $error   = null;
        $status  = 'completed';

        try {
            $output = $this->runTaskByType(
                (string) ($task['type'] ?? ''),
                (array) ($task['config'] ?? []),
            );
        } catch (\Throwable $e) {
            $error  = $e->getMessage();
            $status = 'failed';
        }

        $durationMs = (int) (microtime(true) * 1000) - $startMs;

        // Update task state
        $updateFields = [
            'is_running' => false,
            'last_run'   => gmdate('c'),
            'next_run'   => $this->calculateNextRun((array) ($task['schedule'] ?? [])),
        ];

        if ($status === 'completed') {
            $updateFields['consecutive_fails'] = 0;
            $updateFields['retry_delay_ms']    = 0;
        } else {
            // Apply exponential backoff for retries
            $fails     = (int) ($task['consecutive_fails'] ?? 0) + 1;
            $backoffMs = min(self::BACKOFF_MAX_MS, self::BACKOFF_BASE_MS * (2 ** ($fails - 1)));

            $updateFields['consecutive_fails'] = $fails;
            $updateFields['retry_delay_ms']    = $backoffMs;

            // Self-healing: auto-disable after too many failures
            if ($fails >= 5) {
                $updateFields['is_enabled'] = false;
                $this->cluster->logEvent(
                    'task_auto_disabled',
                    "Task {$taskId} auto-disabled after {$fails} consecutive failures",
                    ['error' => $error],
                );
            }
        }

        try {
            $this->db->updateOne(self::COLL_TASKS, ['task_id' => $taskId], ['$set' => $updateFields], false);
        } catch (\Throwable) {
            // Non-critical
        }

        // Record execution history
        $this->recordHistory($taskId, $trigger, $status, $output, $durationMs, $error);

        $this->cluster->logEvent('task_executed', json_encode([
            'task_id'     => $taskId,
            'status'      => $status,
            'duration_ms' => $durationMs,
            'trigger'     => $trigger,
        ]));

        return $this->taskResult($taskId, $status, $output, $durationMs, $error);
    }

    /**
     * Execute task based on its type.
     *
     * @return mixed  Task output.
     */
    private function runTaskByType(string $type, array $config): mixed
    {
        return match ($type) {
            self::TYPE_FLOW       => $this->runFlowTask($config),
            self::TYPE_HEALTH     => $this->runHealthTask($config),
            self::TYPE_AUTOMATION => $this->runAutomationTask($config),
            self::TYPE_CLEANUP    => $this->runCleanupTask($config),
            self::TYPE_OPTIMIZE   => $this->runOptimizeTask($config),
            self::TYPE_CUSTOM     => $this->runCustomTask($config),
            default               => throw new \RuntimeException("Unknown task type: {$type}"),
        };
    }

    private function runFlowTask(array $config): array
    {
        $flowId = (string) ($config['flow_id'] ?? '');
        $input  = (string) ($config['input'] ?? '');

        if ($flowId === '') {
            throw new \RuntimeException('flow_id is required');
        }

        return $this->flowEngine->execute($flowId, $input);
    }

    private function runHealthTask(array $config): array
    {
        return $this->cluster->checkAllNodes();
    }

    private function runAutomationTask(array $config): array
    {
        return $this->cluster->runAutomationCycle();
    }

    private function runCleanupTask(array $config): array
    {
        $retentionDays = (int) ($config['retention_days'] ?? 30);
        $cutoff        = gmdate('c', time() - ($retentionDays * 86400));

        $deleted = [
            'health'     => 0,
            'events'     => 0,
            'executions' => 0,
            'history'    => 0,
        ];

        try {
            $deleted['health'] = $this->db->deleteMany(CxLLM::COLL_HEALTH, [
                'checked_at' => ['$lt' => $cutoff],
            ]);
        } catch (\Throwable) {
            // Continue
        }

        try {
            $deleted['events'] = $this->db->deleteMany(CxLLM::COLL_EVENTS, [
                'timestamp' => ['$lt' => $cutoff],
            ]);
        } catch (\Throwable) {
            // Continue
        }

        try {
            $deleted['executions'] = $this->db->deleteMany(CxLLM::COLL_EXECUTIONS, [
                'completed_at' => ['$lt' => $cutoff],
            ]);
        } catch (\Throwable) {
            // Continue
        }

        try {
            $deleted['history'] = $this->db->deleteMany(self::COLL_HISTORY, [
                'executed_at' => ['$lt' => $cutoff],
            ]);
        } catch (\Throwable) {
            // Continue
        }

        $this->cluster->logEvent('cleanup_completed', json_encode($deleted));
        return $deleted;
    }

    private function runOptimizeTask(array $config): array
    {
        return $this->runAutonomousOptimization($config);
    }

    private function runCustomTask(array $config): mixed
    {
        $callback = $config['callback'] ?? null;
        if (!is_callable($callback)) {
            throw new \RuntimeException('Custom task requires a callable callback');
        }
        return $callback($this, $config);
    }

    // ─── Autonomous optimization ────────────────────────────────────────────

    /**
     * Run autonomous optimization based on historical metrics.
     *
     * Analyzes:
     *   - Node health patterns → adjust health check intervals
     *   - Flow execution times → optimize step timeouts
     *   - Error rates → adjust retry parameters
     *   - Load patterns → suggest routing rule changes
     */
    public function runAutonomousOptimization(array $config = []): array
    {
        $recommendations = [];
        $applied         = [];

        // 1. Analyze health check patterns
        try {
            $healthStats = $this->analyzeHealthPatterns();
            if (!empty($healthStats['recommendations'])) {
                $recommendations = array_merge($recommendations, $healthStats['recommendations']);
            }
        } catch (\Throwable) {
            // Continue
        }

        // 2. Analyze flow execution patterns
        try {
            $flowStats = $this->analyzeFlowPatterns();
            if (!empty($flowStats['recommendations'])) {
                $recommendations = array_merge($recommendations, $flowStats['recommendations']);
            }
        } catch (\Throwable) {
            // Continue
        }

        // 3. Analyze error rates
        try {
            $errorStats = $this->analyzeErrorPatterns();
            if (!empty($errorStats['recommendations'])) {
                $recommendations = array_merge($recommendations, $errorStats['recommendations']);
            }
        } catch (\Throwable) {
            // Continue
        }

        // 4. Auto-apply safe optimizations if enabled
        if ($config['auto_apply'] ?? false) {
            foreach ($recommendations as $rec) {
                if (($rec['safe_to_apply'] ?? false) && ($rec['apply'] ?? null) !== null) {
                    try {
                        ($rec['apply'])();
                        $applied[] = $rec['id'];
                    } catch (\Throwable) {
                        // Continue
                    }
                }
            }
        }

        // Store tuning report
        try {
            $this->db->insertOne(self::COLL_TUNING, [
                'timestamp'       => gmdate('c'),
                'recommendations' => $recommendations,
                'applied'         => $applied,
                'config'          => $config,
            ]);
        } catch (\Throwable) {
            // Non-critical
        }

        return [
            'recommendations' => $recommendations,
            'applied'         => $applied,
            'timestamp'       => gmdate('c'),
        ];
    }

    private function analyzeHealthPatterns(): array
    {
        $recommendations = [];

        // Get health check frequency per node
        $healthStats = $this->db->aggregate(CxLLM::COLL_HEALTH, [
            ['$match' => ['checked_at' => ['$gte' => gmdate('c', time() - 86400)]]],
            ['$group' => [
                '_id'         => '$node_id',
                'total'       => ['$sum' => 1],
                'healthy'     => ['$sum' => ['$cond' => [['$eq' => ['$status', 'healthy']], 1, 0]]],
                'avg_latency' => ['$avg' => '$latency_ms'],
            ]],
        ]);

        foreach ($healthStats as $stat) {
            $nodeId      = (string) ($stat['_id'] ?? '');
            $total       = (int) ($stat['total'] ?? 0);
            $healthy     = (int) ($stat['healthy'] ?? 0);
            $avgLatency  = (float) ($stat['avg_latency'] ?? 0);
            $healthRate  = $total > 0 ? ($healthy / $total) : 0;

            // Recommend reducing check frequency for stable nodes
            if ($total > 100 && $healthRate > 0.99 && $avgLatency < 500) {
                $recommendations[] = [
                    'id'            => "reduce_check_freq_{$nodeId}",
                    'type'          => 'health_check',
                    'node_id'       => $nodeId,
                    'message'       => "Node {$nodeId} is highly stable (99%+ healthy). Consider reducing health check frequency.",
                    'safe_to_apply' => true,
                    'metrics'       => ['health_rate' => $healthRate, 'avg_latency' => $avgLatency],
                ];
            }

            // Recommend increasing priority for consistently fast nodes
            if ($total > 50 && $healthRate > 0.95 && $avgLatency < 200) {
                $recommendations[] = [
                    'id'            => "increase_priority_{$nodeId}",
                    'type'          => 'routing',
                    'node_id'       => $nodeId,
                    'message'       => "Node {$nodeId} has excellent latency (<200ms). Consider increasing routing priority.",
                    'safe_to_apply' => false,
                    'metrics'       => ['health_rate' => $healthRate, 'avg_latency' => $avgLatency],
                ];
            }
        }

        return ['recommendations' => $recommendations];
    }

    private function analyzeFlowPatterns(): array
    {
        $recommendations = [];

        // Get flow execution patterns
        $flowStats = $this->db->aggregate(CxLLM::COLL_EXECUTIONS, [
            ['$match' => ['started_at' => ['$gte' => gmdate('c', time() - 604800)]]],  // Last 7 days
            ['$group' => [
                '_id'               => '$flow_id',
                'total'             => ['$sum' => 1],
                'completed'         => ['$sum' => ['$cond' => [['$eq' => ['$status', 'completed']], 1, 0]]],
                'avg_latency_ms'    => ['$avg' => '$total_latency_ms'],
                'avg_tokens'        => ['$avg' => '$total_tokens'],
            ]],
        ]);

        foreach ($flowStats as $stat) {
            $flowId      = (string) ($stat['_id'] ?? '');
            $total       = (int) ($stat['total'] ?? 0);
            $completed   = (int) ($stat['completed'] ?? 0);
            $avgLatency  = (float) ($stat['avg_latency_ms'] ?? 0);
            $successRate = $total > 0 ? ($completed / $total) : 0;

            // Recommend increasing timeout for slow-but-successful flows
            if ($total > 20 && $successRate > 0.8 && $avgLatency > 60000) {
                $recommendations[] = [
                    'id'            => "increase_timeout_{$flowId}",
                    'type'          => 'flow',
                    'flow_id'       => $flowId,
                    'message'       => "Flow {$flowId} has high latency ({$avgLatency}ms avg) but good success rate. Consider increasing step timeouts.",
                    'safe_to_apply' => false,
                    'metrics'       => ['success_rate' => $successRate, 'avg_latency_ms' => $avgLatency],
                ];
            }

            // Recommend investigation for low success flows
            if ($total > 10 && $successRate < 0.5) {
                $recommendations[] = [
                    'id'            => "investigate_{$flowId}",
                    'type'          => 'flow',
                    'flow_id'       => $flowId,
                    'message'       => "Flow {$flowId} has low success rate ({$successRate}). Investigation recommended.",
                    'safe_to_apply' => false,
                    'metrics'       => ['success_rate' => $successRate, 'total' => $total],
                ];
            }
        }

        return ['recommendations' => $recommendations];
    }

    private function analyzeErrorPatterns(): array
    {
        $recommendations = [];

        // Get recent error events
        $errorEvents = $this->db->find(CxLLM::COLL_EVENTS, [
            'event_type' => ['$in' => ['auto_offline', 'failover', 'task_auto_disabled']],
            'timestamp'  => ['$gte' => gmdate('c', time() - 86400)],
        ], ['limit' => 100]);

        $errorCounts = [];
        foreach ($errorEvents as $event) {
            $type = (string) ($event['event_type'] ?? '');
            $errorCounts[$type] = ($errorCounts[$type] ?? 0) + 1;
        }

        // Recommend review if too many auto-offlines
        if (($errorCounts['auto_offline'] ?? 0) > 5) {
            $recommendations[] = [
                'id'            => 'review_node_stability',
                'type'          => 'infrastructure',
                'message'       => "High number of auto-offline events ({$errorCounts['auto_offline']}). Review node stability and network connectivity.",
                'safe_to_apply' => false,
                'metrics'       => $errorCounts,
            ];
        }

        // Recommend failover rule review
        if (($errorCounts['failover'] ?? 0) > 10) {
            $recommendations[] = [
                'id'            => 'review_failover_rules',
                'type'          => 'routing',
                'message'       => "High failover count ({$errorCounts['failover']}). Review routing rules and primary node capacity.",
                'safe_to_apply' => false,
                'metrics'       => $errorCounts,
            ];
        }

        return ['recommendations' => $recommendations];
    }

    // ─── Event triggers ─────────────────────────────────────────────────────

    /**
     * Trigger tasks registered for a specific event.
     *
     * @param string $eventType  Event type (e.g., 'health_degraded', 'flow_completed')
     * @param array  $eventData  Event payload data.
     *
     * @return int  Number of tasks triggered.
     */
    public function triggerEvent(string $eventType, array $eventData = []): int
    {
        try {
            $tasks = $this->db->find(self::COLL_TASKS, [
                'is_enabled'        => true,
                'triggers.event'    => $eventType,
            ]);
        } catch (\Throwable) {
            return 0;
        }

        $triggered = 0;
        foreach ($tasks as $task) {
            $taskId  = (string) ($task['task_id'] ?? '');
            $trigger = (array) ($task['triggers'] ?? []);

            // Check trigger conditions
            if ($this->evaluateTriggerConditions($trigger, $eventData)) {
                $this->executeTask($taskId, self::TRIGGER_EVENT);
                $triggered++;
            }
        }

        return $triggered;
    }

    /**
     * Handle webhook trigger.
     *
     * @param string $webhookId   Webhook identifier.
     * @param array  $payload     Webhook payload.
     * @param string $signature   Optional HMAC signature for verification.
     *
     * @return array{triggered: int, tasks: list<string>}
     */
    public function handleWebhook(string $webhookId, array $payload, string $signature = ''): array
    {
        try {
            $tasks = $this->db->find(self::COLL_TASKS, [
                'is_enabled'           => true,
                'triggers.webhook_id'  => $webhookId,
            ]);
        } catch (\Throwable) {
            return ['triggered' => 0, 'tasks' => []];
        }

        $triggered = 0;
        $taskIds   = [];

        foreach ($tasks as $task) {
            $taskId  = (string) ($task['task_id'] ?? '');
            $trigger = (array) ($task['triggers'] ?? []);

            // Verify webhook signature if secret is configured
            $secret = (string) ($trigger['webhook_secret'] ?? '');
            if ($secret !== '' && $signature !== '') {
                $expected = hash_hmac('sha256', json_encode($payload), $secret);
                if (!hash_equals($expected, $signature)) {
                    continue;  // Signature mismatch
                }
            }

            // Inject webhook payload into task config
            $taskConfig = (array) ($task['config'] ?? []);
            $taskConfig['_webhook_payload'] = $payload;

            $result = $this->executeTask($taskId, self::TRIGGER_WEBHOOK);
            if ($result['status'] !== 'skipped') {
                $triggered++;
                $taskIds[] = $taskId;
            }
        }

        return ['triggered' => $triggered, 'tasks' => $taskIds];
    }

    private function evaluateTriggerConditions(array $trigger, array $eventData): bool
    {
        $conditions = (array) ($trigger['conditions'] ?? []);
        if (empty($conditions)) {
            return true;  // No conditions = always trigger
        }

        foreach ($conditions as $field => $expected) {
            $actual = $eventData[$field] ?? null;

            if (is_array($expected)) {
                // Operator-based condition
                $op    = (string) ($expected['op'] ?? 'eq');
                $value = $expected['value'] ?? null;

                $pass = match ($op) {
                    'eq'       => $actual === $value,
                    'neq'      => $actual !== $value,
                    'gt'       => $actual > $value,
                    'gte'      => $actual >= $value,
                    'lt'       => $actual < $value,
                    'lte'      => $actual <= $value,
                    'in'       => is_array($value) && in_array($actual, $value, true),
                    'contains' => is_string($actual) && is_string($value) && str_contains($actual, $value),
                    default    => true,
                };

                if (!$pass) {
                    return false;
                }
            } else {
                // Simple equality
                if ($actual !== $expected) {
                    return false;
                }
            }
        }

        return true;
    }

    // ─── Execution history ──────────────────────────────────────────────────

    private function recordHistory(
        string $taskId,
        string $trigger,
        string $status,
        mixed $output,
        int $durationMs,
        ?string $error,
    ): void {
        try {
            $this->db->insertOne(self::COLL_HISTORY, [
                'task_id'     => $taskId,
                'trigger'     => $trigger,
                'status'      => $status,
                'output'      => $output,
                'duration_ms' => $durationMs,
                'error'       => $error,
                'executed_at' => gmdate('c'),
            ]);
        } catch (\Throwable) {
            // Non-critical
        }
    }

    /**
     * Get execution history for a task.
     *
     * @return list<array<string,mixed>>
     */
    public function getTaskHistory(string $taskId, int $limit = 20): array
    {
        try {
            return $this->db->find(self::COLL_HISTORY, ['task_id' => $taskId], [
                'sort'  => ['executed_at' => -1],
                'limit' => $limit,
            ]);
        } catch (\Throwable) {
            return [];
        }
    }

    /**
     * Get scheduler statistics.
     *
     * @return array<string,mixed>
     */
    public function getStats(): array
    {
        try {
            $tasks   = $this->db->find(self::COLL_TASKS, []);
            $enabled = 0;
            $running = 0;
            $failed  = 0;

            foreach ($tasks as $task) {
                if ($task['is_enabled'] ?? false) {
                    $enabled++;
                }
                if ($task['is_running'] ?? false) {
                    $running++;
                }
                if (($task['consecutive_fails'] ?? 0) > 0) {
                    $failed++;
                }
            }

            $recentHistory = $this->db->aggregate(self::COLL_HISTORY, [
                ['$match' => ['executed_at' => ['$gte' => gmdate('c', time() - 86400)]]],
                ['$group' => [
                    '_id'           => '$status',
                    'count'         => ['$sum' => 1],
                    'avg_duration'  => ['$avg' => '$duration_ms'],
                ]],
            ]);

            $historyStats = [];
            foreach ($recentHistory as $stat) {
                $historyStats[(string) ($stat['_id'] ?? 'unknown')] = [
                    'count'        => (int) ($stat['count'] ?? 0),
                    'avg_duration' => round((float) ($stat['avg_duration'] ?? 0), 1),
                ];
            }

            return [
                'total_tasks'       => count($tasks),
                'enabled_tasks'     => $enabled,
                'running_tasks'     => $running,
                'failing_tasks'     => $failed,
                'last_24h'          => $historyStats,
            ];
        } catch (\Throwable) {
            return [
                'total_tasks'   => 0,
                'enabled_tasks' => 0,
                'running_tasks' => 0,
                'failing_tasks' => 0,
                'last_24h'      => [],
            ];
        }
    }

    // ─── Pre-built automation tasks ─────────────────────────────────────────

    /**
     * Register default automation tasks.
     *
     * @return int  Number of tasks registered.
     */
    public function registerDefaultTasks(): int
    {
        $tasks = [
            [
                'task_id'  => 'auto-health-check',
                'name'     => 'Automatic Health Check',
                'type'     => self::TYPE_HEALTH,
                'config'   => [],
                'schedule' => ['interval_seconds' => 60],
            ],
            [
                'task_id'  => 'auto-cleanup',
                'name'     => 'Automatic Data Cleanup',
                'type'     => self::TYPE_CLEANUP,
                'config'   => ['retention_days' => 30],
                'schedule' => ['interval_seconds' => 86400],  // Daily
            ],
            [
                'task_id'  => 'auto-optimize',
                'name'     => 'Autonomous Optimization',
                'type'     => self::TYPE_OPTIMIZE,
                'config'   => ['auto_apply' => false],
                'schedule' => ['interval_seconds' => 3600],  // Hourly
            ],
            [
                'task_id'  => 'auto-full-cycle',
                'name'     => 'Full Automation Cycle',
                'type'     => self::TYPE_AUTOMATION,
                'config'   => [],
                'schedule' => ['interval_seconds' => 300],  // Every 5 minutes
            ],
        ];

        $count = 0;
        foreach ($tasks as $t) {
            if ($this->scheduleTask(
                $t['task_id'],
                $t['name'],
                $t['type'],
                $t['config'],
                $t['schedule'],
            )) {
                $count++;
            }
        }

        return $count;
    }

    // ─── Helpers ────────────────────────────────────────────────────────────

    private function calculateNextRun(array $schedule): string
    {
        $now = time();

        // One-time execution
        if (isset($schedule['run_at'])) {
            return (string) $schedule['run_at'];
        }

        // Interval-based
        if (isset($schedule['interval_seconds'])) {
            $interval = max(10, (int) $schedule['interval_seconds']);
            return gmdate('c', $now + $interval);
        }

        // Cron expression (simplified: minute hour day month weekday)
        if (isset($schedule['cron'])) {
            // For now, default to 1 hour from now for cron
            // Full cron parsing would require additional implementation
            return gmdate('c', $now + 3600);
        }

        // Default: 1 hour from now
        return gmdate('c', $now + 3600);
    }

    /**
     * @return array{task_id: string, status: string, output: mixed, duration_ms: int, error: string|null}
     */
    private function taskResult(string $taskId, string $status, mixed $output, int $durationMs, ?string $error): array
    {
        return [
            'task_id'     => $taskId,
            'status'      => $status,
            'output'      => $output,
            'duration_ms' => $durationMs,
            'error'       => $error,
        ];
    }
}
