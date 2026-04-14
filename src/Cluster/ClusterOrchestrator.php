<?php

declare(strict_types=1);

namespace CxAI\CxPHP\Cluster;

use CxAI\CxPHP\Acp\CxModelBridge;
use CxAI\CxPHP\Database\CxLLM;

/**
 * Cluster Orchestrator — MongoDB-backed node management, health monitoring,
 * intelligent routing with automated failover, and full automation.
 *
 * Uses the CxLLM MongoDB connector for all persistence.
 */
class ClusterOrchestrator
{
    /** Health check thresholds (ms). */
    private const LATENCY_HEALTHY  = 2000;
    private const LATENCY_DEGRADED = 5000;

    /** Minimum seconds between auto health checks for the same node. */
    private const HEALTH_CHECK_INTERVAL = 30;

    /** In-memory node cache. */
    private array $nodeCache = [];

    /** Round-robin counters per model_id. */
    private array $rrCounters = [];

    public function __construct(private readonly CxLLM $db) {}

    /**
     * Create from singleton MongoDB connection.
     */
    public static function fromEnv(): self
    {
        return new self(CxLLM::getInstance());
    }

    /**
     * Check if the MongoDB cluster backend is available.
     */
    public function isAvailable(): bool
    {
        return $this->db->isConfigured();
    }

    // ─── Node registration ──────────────────────────────────────────────────

    /**
     * Register or update a cluster node.
     *
     * @return string|null  The node_id on success, null on failure.
     */
    public function registerNode(
        string $nodeId,
        string $modelId,
        string $provider,
        string $endpoint = '',
        string $region = 'default',
        int $weight = 100,
        int $maxRps = 60,
        array $capabilities = [],
        array $meta = [],
    ): ?string {
        try {
            $this->db->updateOne(
                CxLLM::COLL_NODES,
                ['node_id' => $nodeId],
                [
                    '$set' => [
                        'node_id'      => $nodeId,
                        'model_id'     => $modelId,
                        'provider'     => $provider,
                        'endpoint'     => $endpoint,
                        'region'       => $region,
                        'status'       => 'active',
                        'weight'       => $weight,
                        'max_rps'      => $maxRps,
                        'current_rps'  => 0,
                        'priority'     => 0,
                        'capabilities' => $capabilities,
                        'meta'         => $meta,
                    ],
                ],
                true,
            );
            $this->nodeCache = [];
            return $nodeId;
        } catch (\Throwable) {
            return null;
        }
    }

    /**
     * Auto-register all models from config/cx_models.php as cluster nodes.
     *
     * @return int  Number of nodes registered.
     */
    public function autoRegisterFromConfig(): int
    {
        $configPath = dirname(__DIR__, 2) . '/config/cx_models.php';
        if (!is_file($configPath)) {
            return 0;
        }

        try {
            $config = (array) (require $configPath);
        } catch (\Throwable) {
            return 0;
        }

        $count = 0;
        foreach ($config['models'] ?? [] as $modelId => $cfg) {
            $provider = (string) ($cfg['provider'] ?? '');
            if ($provider === '') {
                continue;
            }

            $nodeId = "auto-{$modelId}";
            $result = $this->registerNode(
                $nodeId,
                (string) $modelId,
                $provider,
                '',
                'default',
                100,
                (int) ($cfg['max_rps'] ?? 60),
                (array) ($cfg['capabilities'] ?? []),
                [
                    'model_name'    => (string) ($cfg['model_id'] ?? ''),
                    'max_tokens'    => $cfg['max_tokens'] ?? null,
                    'context_window' => $cfg['context_window'] ?? null,
                    'auto_registered' => true,
                ],
            );
            if ($result !== null) {
                $count++;
            }
        }

        $this->logEvent('auto_register', "Registered {$count} nodes from config");
        return $count;
    }

    /**
     * Get all registered nodes, optionally filtered.
     *
     * @return list<array<string,mixed>>
     */
    public function getNodes(
        ?string $modelId = null,
        ?string $status = null,
        ?string $region = null,
    ): array {
        $filter = [];
        if ($modelId !== null) {
            $filter['model_id'] = $modelId;
        }
        if ($status !== null) {
            $filter['status'] = $status;
        }
        if ($region !== null) {
            $filter['region'] = $region;
        }

        try {
            return $this->db->find(CxLLM::COLL_NODES, $filter, [
                'sort' => ['priority' => -1, 'weight' => -1],
            ]);
        } catch (\Throwable) {
            return [];
        }
    }

    /**
     * Update a node's status.
     */
    public function setNodeStatus(string $nodeId, string $status): bool
    {
        $valid = ['active', 'draining', 'unhealthy', 'offline', 'warming'];
        if (!in_array($status, $valid, true)) {
            return false;
        }

        try {
            $this->db->updateOne(
                CxLLM::COLL_NODES,
                ['node_id' => $nodeId],
                ['$set' => ['status' => $status]],
                false,
            );
            $this->nodeCache = [];
            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * Remove a node from the cluster.
     */
    public function removeNode(string $nodeId): bool
    {
        try {
            $this->db->deleteOne(CxLLM::COLL_NODES, ['node_id' => $nodeId]);
            $this->nodeCache = [];
            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    // ─── Health monitoring ──────────────────────────────────────────────────

    /**
     * Record a health check result for a node.
     */
    public function recordHealth(
        string $nodeId,
        string $status,
        ?int $latencyMs = null,
        ?string $error = null,
        array $meta = [],
    ): bool {
        try {
            $this->db->insertOne(CxLLM::COLL_HEALTH, [
                'node_id'    => $nodeId,
                'status'     => $status,
                'latency_ms' => $latencyMs,
                'error'      => $error,
                'meta'       => $meta,
                'checked_at' => gmdate('c'),
            ]);

            // Update node status based on health result
            $nodeStatus = match ($status) {
                'healthy', 'degraded' => 'active',
                'unhealthy', 'timeout' => 'unhealthy',
                default => null,
            };

            if ($nodeStatus !== null) {
                $this->db->updateOne(
                    CxLLM::COLL_NODES,
                    ['node_id' => $nodeId],
                    ['$set' => [
                        'status'      => $nodeStatus,
                        'last_health' => gmdate('c'),
                    ]],
                    false,
                );
            }

            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * Probe a single node by making a lightweight LLM call.
     *
     * @return array{status: string, latency_ms: int, error: string|null}
     */
    public function checkNodeHealth(string $nodeId): array
    {
        $node = $this->db->findOne(CxLLM::COLL_NODES, ['node_id' => $nodeId]);
        if ($node === null) {
            return ['status' => 'unhealthy', 'latency_ms' => 0, 'error' => 'Node not found'];
        }

        $modelId = (string) ($node['model_id'] ?? '');
        try {
            $bridge  = CxModelBridge::forModel($modelId);
            $session = new \CxAI\CxPHP\Session\InMemorySession('health-' . $nodeId);
            $startMs = (int) (microtime(true) * 1000);
            $resp    = $bridge->chat($session, 'ping');
            $latency = (int) (microtime(true) * 1000) - $startMs;

            if ($resp->isSuccess()) {
                $status = $latency <= self::LATENCY_HEALTHY ? 'healthy'
                    : ($latency <= self::LATENCY_DEGRADED ? 'degraded' : 'unhealthy');
            } else {
                $status = 'unhealthy';
            }

            $this->recordHealth($nodeId, $status, $latency);
            return ['status' => $status, 'latency_ms' => $latency, 'error' => null];
        } catch (\Throwable $e) {
            $this->recordHealth($nodeId, 'unhealthy', 0, $e->getMessage());
            return ['status' => 'unhealthy', 'latency_ms' => 0, 'error' => $e->getMessage()];
        }
    }

    /**
     * Check health of all active nodes.
     *
     * @return list<array{node_id: string, model_id: string, status: string, latency_ms: int}>
     */
    public function checkAllNodes(): array
    {
        $nodes   = $this->getNodes(status: 'active');
        $results = [];

        foreach ($nodes as $node) {
            $nid    = (string) ($node['node_id'] ?? '');
            $check  = $this->checkNodeHealth($nid);
            $results[] = [
                'node_id'    => $nid,
                'model_id'   => (string) ($node['model_id'] ?? ''),
                'status'     => $check['status'],
                'latency_ms' => $check['latency_ms'],
            ];
        }

        return $results;
    }

    /**
     * Get health history for a node.
     *
     * @return list<array<string,mixed>>
     */
    public function getHealthHistory(string $nodeId, int $limit = 50): array
    {
        try {
            return $this->db->find(CxLLM::COLL_HEALTH, ['node_id' => $nodeId], [
                'sort'  => ['checked_at' => -1],
                'limit' => $limit,
            ]);
        } catch (\Throwable) {
            return [];
        }
    }

    /**
     * Get a cluster-wide health summary via aggregation.
     *
     * @return list<array<string,mixed>>
     */
    public function getHealthSummary(int $hours = 24): array
    {
        try {
            $cutoff = gmdate('c', time() - ($hours * 3600));
            return $this->db->aggregate(CxLLM::COLL_HEALTH, [
                ['$match' => ['checked_at' => ['$gte' => $cutoff]]],
                ['$group' => [
                    '_id'            => '$node_id',
                    'total_checks'   => ['$sum' => 1],
                    'healthy_checks' => ['$sum' => ['$cond' => [['$eq' => ['$status', 'healthy']], 1, 0]]],
                    'avg_latency_ms' => ['$avg' => '$latency_ms'],
                    'last_check'     => ['$max' => '$checked_at'],
                ]],
                ['$sort' => ['_id' => 1]],
            ]);
        } catch (\Throwable) {
            return [];
        }
    }

    // ─── Intelligent routing ────────────────────────────────────────────────

    /**
     * Select the best node for a model using the configured routing strategy.
     *
     * Supports: weighted, round_robin, least_latency, failover, capability.
     *
     * @return array<string,mixed>|null  The selected node document, or null.
     */
    public function selectNode(
        string $modelId,
        ?string $region = null,
        ?string $capability = null,
        string $strategy = 'weighted',
    ): ?array {
        // Check for a routing rule that overrides the default strategy
        $rule = $this->getRoutingRule($modelId);
        if ($rule !== null) {
            $strategy = (string) ($rule['strategy'] ?? $strategy);
        }

        $filter = ['model_id' => $modelId, 'status' => 'active'];
        if ($region !== null) {
            $filter['region'] = $region;
        }
        if ($capability !== null) {
            $filter['capabilities'] = $capability;
        }

        try {
            $nodes = $this->db->find(CxLLM::COLL_NODES, $filter, [
                'sort' => ['priority' => -1, 'weight' => -1],
            ]);
        } catch (\Throwable) {
            return null;
        }

        if ($nodes === []) {
            // Auto-failover: try fallback models from the routing rule
            if ($rule !== null) {
                foreach ((array) ($rule['fallbacks'] ?? []) as $fallbackModel) {
                    $fallback = $this->selectNode((string) $fallbackModel, $region, $capability, $strategy);
                    if ($fallback !== null) {
                        $this->logEvent('failover', "Routed {$modelId} → {$fallbackModel}");
                        return $fallback;
                    }
                }
            }
            return null;
        }

        return match ($strategy) {
            'round_robin'    => $this->routeRoundRobin($modelId, $nodes),
            'least_latency'  => $this->routeLeastLatency($nodes),
            'failover'       => $nodes[0] ?? null,
            'capability'     => $nodes[0] ?? null,
            default          => $this->routeWeighted($nodes), // 'weighted'
        };
    }

    /**
     * Weighted random selection based on node weight.
     */
    private function routeWeighted(array $nodes): ?array
    {
        $totalWeight = 0;
        foreach ($nodes as $n) {
            $totalWeight += (int) ($n['weight'] ?? 100);
        }
        if ($totalWeight <= 0) {
            return $nodes[0] ?? null;
        }

        $rand = random_int(1, $totalWeight);
        $cumulative = 0;
        foreach ($nodes as $n) {
            $cumulative += (int) ($n['weight'] ?? 100);
            if ($rand <= $cumulative) {
                return $n;
            }
        }
        return $nodes[0] ?? null;
    }

    /**
     * Round-robin selection.
     */
    private function routeRoundRobin(string $modelId, array $nodes): ?array
    {
        $idx = ($this->rrCounters[$modelId] ?? 0) % count($nodes);
        $this->rrCounters[$modelId] = $idx + 1;
        return $nodes[$idx] ?? null;
    }

    /**
     * Select the node with the lowest recent latency.
     */
    private function routeLeastLatency(array $nodes): ?array
    {
        $best = null;
        $bestLatency = PHP_INT_MAX;

        foreach ($nodes as $n) {
            $nid = (string) ($n['node_id'] ?? '');
            try {
                $recent = $this->db->find(CxLLM::COLL_HEALTH, [
                    'node_id' => $nid,
                    'status'  => 'healthy',
                ], ['sort' => ['checked_at' => -1], 'limit' => 1]);

                $lat = (int) ($recent[0]['latency_ms'] ?? PHP_INT_MAX);
                if ($lat < $bestLatency) {
                    $bestLatency = $lat;
                    $best = $n;
                }
            } catch (\Throwable) {
                continue;
            }
        }

        return $best ?? ($nodes[0] ?? null);
    }

    // ─── Routing rules ──────────────────────────────────────────────────────

    /**
     * Get the routing rule for a model.
     *
     * @return array<string,mixed>|null
     */
    public function getRoutingRule(string $modelId): ?array
    {
        try {
            return $this->db->findOne(CxLLM::COLL_ROUTING, [
                'model_id'  => $modelId,
                'is_active' => true,
            ]);
        } catch (\Throwable) {
            return null;
        }
    }

    /**
     * Set or update a routing rule.
     */
    public function setRoutingRule(
        string $ruleId,
        string $name,
        string $strategy,
        ?string $modelId = null,
        array $conditions = [],
        array $fallbacks = [],
        int $priority = 0,
    ): bool {
        $validStrategies = ['weighted', 'round_robin', 'least_latency', 'failover', 'capability'];
        if (!in_array($strategy, $validStrategies, true)) {
            return false;
        }

        try {
            $this->db->updateOne(
                CxLLM::COLL_ROUTING,
                ['rule_id' => $ruleId],
                [
                    '$set' => [
                        'rule_id'    => $ruleId,
                        'name'       => $name,
                        'strategy'   => $strategy,
                        'model_id'   => $modelId,
                        'conditions' => $conditions,
                        'fallbacks'  => $fallbacks,
                        'is_active'  => true,
                        'priority'   => $priority,
                    ],
                ],
            );
            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * Get all routing rules.
     *
     * @return list<array<string,mixed>>
     */
    public function getRoutingRules(): array
    {
        try {
            return $this->db->find(CxLLM::COLL_ROUTING, [], [
                'sort' => ['priority' => -1, 'rule_id' => 1],
            ]);
        } catch (\Throwable) {
            return [];
        }
    }

    // ─── Cluster status / automation ────────────────────────────────────────

    /**
     * Get full cluster status (nodes + health + routing).
     *
     * @return array<string,mixed>
     */
    public function getClusterStatus(): array
    {
        $nodes     = $this->getNodes();
        $summary   = $this->getHealthSummary();
        $rules     = $this->getRoutingRules();
        $flowCount = $this->countFlows();

        $active   = 0;
        $unhealthy = 0;
        foreach ($nodes as $n) {
            if (($n['status'] ?? '') === 'active') {
                $active++;
            }
            if (($n['status'] ?? '') === 'unhealthy') {
                $unhealthy++;
            }
        }

        return [
            'cluster_name'  => $this->db->getClusterName(),
            'database'      => $this->db->getDatabase(),
            'transport'     => $this->db->getTransport(),
            'total_nodes'   => count($nodes),
            'active_nodes'  => $active,
            'unhealthy_nodes' => $unhealthy,
            'routing_rules' => count($rules),
            'agentic_flows' => $flowCount,
            'nodes'         => $nodes,
            'health_summary' => $summary,
            'rules'         => $rules,
        ];
    }

    /**
     * Run a full automation cycle:
     *   1. Auto-register nodes from config
     *   2. Health-check all active nodes
     *   3. Auto-failover unhealthy nodes
     *   4. Log the cycle event
     *
     * @return array{registered: int, checked: int, failovers: int}
     */
    public function runAutomationCycle(): array
    {
        $registered = $this->autoRegisterFromConfig();
        $checks     = $this->checkAllNodes();
        $failovers  = 0;

        // Auto-failover: mark nodes with consecutive failures as unhealthy
        foreach ($checks as $check) {
            if ($check['status'] === 'unhealthy') {
                $history = $this->getHealthHistory($check['node_id'], 3);
                $consecutiveFails = 0;
                foreach ($history as $h) {
                    if (in_array($h['status'] ?? '', ['unhealthy', 'timeout'], true)) {
                        $consecutiveFails++;
                    } else {
                        break;
                    }
                }
                if ($consecutiveFails >= 3) {
                    $this->setNodeStatus($check['node_id'], 'offline');
                    $failovers++;
                    $this->logEvent('auto_offline', "Node {$check['node_id']} taken offline after 3 consecutive failures");
                }
            }
        }

        $this->logEvent('automation_cycle', json_encode([
            'registered' => $registered,
            'checked'    => count($checks),
            'failovers'  => $failovers,
        ]));

        return [
            'registered' => $registered,
            'checked'    => count($checks),
            'failovers'  => $failovers,
        ];
    }

    // ─── Flow helpers (counts only — engine is separate) ────────────────────

    /**
     * Count active agentic flows.
     */
    public function countFlows(): int
    {
        try {
            return $this->db->count(CxLLM::COLL_FLOWS, ['is_active' => true]);
        } catch (\Throwable) {
            return 0;
        }
    }

    // ─── Event logging ──────────────────────────────────────────────────────

    /**
     * Log a cluster event.
     */
    public function logEvent(string $type, string $message, array $meta = []): void
    {
        try {
            $this->db->insertOne(CxLLM::COLL_EVENTS, [
                'event_type' => $type,
                'message'    => $message,
                'meta'       => $meta,
                'timestamp'  => gmdate('c'),
            ]);
        } catch (\Throwable) {
            // Events are non-critical — don't break the caller.
        }
    }

    /**
     * Get recent cluster events.
     *
     * @return list<array<string,mixed>>
     */
    public function getEvents(int $limit = 50, ?string $type = null): array
    {
        $filter = [];
        if ($type !== null) {
            $filter['event_type'] = $type;
        }
        try {
            return $this->db->find(CxLLM::COLL_EVENTS, $filter, [
                'sort'  => ['timestamp' => -1],
                'limit' => $limit,
            ]);
        } catch (\Throwable) {
            return [];
        }
    }

    // ─── Self-healing and diagnostics ───────────────────────────────────────

    /**
     * Run self-healing recovery process.
     *
     * Performs:
     *   1. Attempts to recover unhealthy nodes (retry health check)
     *   2. Reactivates offline nodes that have been offline > recovery_minutes
     *   3. Clears stale health data
     *   4. Logs recovery actions
     *
     * @param int $recoveryMinutes  Minimum offline time before auto-recovery attempt.
     * @return array{recovered: int, reactivated: int, cleared: int}
     */
    public function runSelfHealing(int $recoveryMinutes = 30): array
    {
        $recovered   = 0;
        $reactivated = 0;
        $cleared     = 0;

        // 1. Attempt to recover unhealthy nodes
        try {
            $unhealthyNodes = $this->getNodes(null, 'unhealthy');
            foreach ($unhealthyNodes as $node) {
                $nodeId = (string) ($node['node_id'] ?? '');
                if ($nodeId === '') {
                    continue;
                }

                // Attempt a fresh health check
                $check = $this->checkNodeHealth($nodeId);
                if (($check['status'] ?? '') === 'healthy') {
                    $recovered++;
                    $this->logEvent('self_heal_recovered', "Node {$nodeId} recovered to healthy status");
                }
            }
        } catch (\Throwable) {
            // Continue
        }

        // 2. Reactivate nodes that have been offline for extended time
        try {
            $cutoff      = gmdate('c', time() - ($recoveryMinutes * 60));
            $offlineNodes = $this->db->find(CxLLM::COLL_NODES, [
                'status'     => 'offline',
            ]);

            foreach ($offlineNodes as $node) {
                $nodeId    = (string) ($node['node_id'] ?? '');
                $updatedAt = $node['_updated_at'] ?? $node['updated_at'] ?? null;

                if ($nodeId === '' || $updatedAt === null) {
                    continue;
                }

                // If offline for longer than recovery window, attempt reactivation
                if ($updatedAt < $cutoff) {
                    // First set to warming, then try health check
                    $this->setNodeStatus($nodeId, 'warming');
                    $check = $this->checkNodeHealth($nodeId);

                    if (in_array($check['status'] ?? '', ['healthy', 'degraded'], true)) {
                        $reactivated++;
                        $this->logEvent('self_heal_reactivated', "Node {$nodeId} reactivated after offline period");
                    }
                }
            }
        } catch (\Throwable) {
            // Continue
        }

        // 3. Clear stale health data (older than 7 days)
        try {
            $staleTime = gmdate('c', time() - (7 * 86400));
            $cleared   = $this->db->deleteMany(CxLLM::COLL_HEALTH, [
                'checked_at' => ['$lt' => $staleTime],
            ]);
        } catch (\Throwable) {
            // Non-critical
        }

        $this->logEvent('self_healing_cycle', json_encode([
            'recovered'        => $recovered,
            'reactivated'      => $reactivated,
            'stale_cleared'    => $cleared,
            'recovery_minutes' => $recoveryMinutes,
        ]));

        return [
            'recovered'   => $recovered,
            'reactivated' => $reactivated,
            'cleared'     => $cleared,
        ];
    }

    /**
     * Run comprehensive diagnostics on the cluster.
     *
     * @return array<string,mixed>  Diagnostic report.
     */
    public function runDiagnostics(): array
    {
        $diagnostics = [
            'timestamp'         => gmdate('c'),
            'database_status'   => $this->db->isConfigured() ? 'connected' : 'disconnected',
            'node_summary'      => [],
            'health_trends'     => [],
            'routing_analysis'  => [],
            'recommendations'   => [],
            'warnings'          => [],
        ];

        // Node status summary
        try {
            $nodes = $this->getNodes();
            $statusCounts = ['active' => 0, 'unhealthy' => 0, 'offline' => 0, 'warming' => 0, 'draining' => 0];

            foreach ($nodes as $node) {
                $status = $node['status'] ?? 'unknown';
                if (isset($statusCounts[$status])) {
                    $statusCounts[$status]++;
                }
            }

            $diagnostics['node_summary'] = [
                'total'  => count($nodes),
                'by_status' => $statusCounts,
            ];

            // Warn if too many unhealthy
            $unhealthyRatio = count($nodes) > 0
                ? ($statusCounts['unhealthy'] + $statusCounts['offline']) / count($nodes)
                : 0;
            if ($unhealthyRatio > 0.3) {
                $diagnostics['warnings'][] = 'High ratio of unhealthy/offline nodes (' . round($unhealthyRatio * 100) . '%)';
            }
        } catch (\Throwable) {
            $diagnostics['node_summary'] = ['error' => 'Failed to fetch nodes'];
        }

        // Health trends (last 24 hours)
        try {
            $healthSummary = $this->getHealthSummary(24);
            $totalChecks   = 0;
            $healthyChecks = 0;
            $avgLatency    = 0;

            foreach ($healthSummary as $s) {
                $totalChecks   += (int) ($s['total_checks'] ?? 0);
                $healthyChecks += (int) ($s['healthy_checks'] ?? 0);
                $avgLatency    += (float) ($s['avg_latency_ms'] ?? 0);
            }

            $avgLatency = count($healthSummary) > 0 ? $avgLatency / count($healthSummary) : 0;
            $healthRate = $totalChecks > 0 ? ($healthyChecks / $totalChecks) : 0;

            $diagnostics['health_trends'] = [
                'period_hours'     => 24,
                'total_checks'     => $totalChecks,
                'healthy_checks'   => $healthyChecks,
                'health_rate'      => round($healthRate, 4),
                'avg_latency_ms'   => round($avgLatency, 1),
            ];

            if ($healthRate < 0.9 && $totalChecks > 10) {
                $diagnostics['warnings'][] = 'Health rate below 90% over last 24 hours';
            }
            if ($avgLatency > 3000) {
                $diagnostics['warnings'][] = 'Average latency is high (' . round($avgLatency) . 'ms)';
            }
        } catch (\Throwable) {
            $diagnostics['health_trends'] = ['error' => 'Failed to fetch health trends'];
        }

        // Routing rule analysis
        try {
            $rules = $this->getRoutingRules();
            $diagnostics['routing_analysis'] = [
                'total_rules' => count($rules),
                'rules'       => array_map(fn($r) => [
                    'rule_id'  => $r['rule_id'] ?? '',
                    'strategy' => $r['strategy'] ?? '',
                    'model_id' => $r['model_id'] ?? '',
                ], $rules),
            ];

            // Check for models without routing rules
            $modelIds = array_unique(array_column($nodes ?? [], 'model_id'));
            $ruledModels = array_column($rules, 'model_id');
            $unruledModels = array_diff($modelIds, $ruledModels);

            if (!empty($unruledModels)) {
                $diagnostics['recommendations'][] = 'Consider adding routing rules for: ' . implode(', ', $unruledModels);
            }
        } catch (\Throwable) {
            $diagnostics['routing_analysis'] = ['error' => 'Failed to fetch routing rules'];
        }

        // Generate recommendations
        if (($diagnostics['node_summary']['by_status']['active'] ?? 0) < 2) {
            $diagnostics['recommendations'][] = 'Add more active nodes for redundancy';
        }
        if (empty($diagnostics['warnings'])) {
            $diagnostics['recommendations'][] = 'Cluster is healthy — no immediate action required';
        }

        return $diagnostics;
    }

    /**
     * Get performance metrics for the cluster.
     *
     * @return array<string,mixed>  Performance metrics.
     */
    public function getPerformanceMetrics(int $hours = 24): array
    {
        $metrics = [
            'timestamp'           => gmdate('c'),
            'period_hours'        => $hours,
            'node_metrics'        => [],
            'aggregate_metrics'   => [],
        ];

        try {
            $cutoff = gmdate('c', time() - ($hours * 3600));

            // Per-node metrics
            $nodeMetrics = $this->db->aggregate(CxLLM::COLL_HEALTH, [
                ['$match' => ['checked_at' => ['$gte' => $cutoff]]],
                ['$group' => [
                    '_id'            => '$node_id',
                    'total_checks'   => ['$sum' => 1],
                    'healthy_checks' => ['$sum' => ['$cond' => [['$eq' => ['$status', 'healthy']], 1, 0]]],
                    'avg_latency_ms' => ['$avg' => '$latency_ms'],
                    'min_latency_ms' => ['$min' => '$latency_ms'],
                    'max_latency_ms' => ['$max' => '$latency_ms'],
                    'error_count'    => ['$sum' => ['$cond' => [['$ne' => ['$error', null]], 1, 0]]],
                ]],
                ['$sort' => ['_id' => 1]],
            ]);

            $totalChecks   = 0;
            $totalLatency  = 0;
            $totalHealthy  = 0;
            $totalErrors   = 0;

            foreach ($nodeMetrics as $nm) {
                $nodeId = (string) ($nm['_id'] ?? '');
                $checks = (int) ($nm['total_checks'] ?? 0);

                $metrics['node_metrics'][$nodeId] = [
                    'total_checks'   => $checks,
                    'healthy_checks' => (int) ($nm['healthy_checks'] ?? 0),
                    'health_rate'    => $checks > 0 ? round(($nm['healthy_checks'] ?? 0) / $checks, 4) : 0,
                    'avg_latency_ms' => round((float) ($nm['avg_latency_ms'] ?? 0), 1),
                    'min_latency_ms' => (int) ($nm['min_latency_ms'] ?? 0),
                    'max_latency_ms' => (int) ($nm['max_latency_ms'] ?? 0),
                    'error_count'    => (int) ($nm['error_count'] ?? 0),
                ];

                $totalChecks  += $checks;
                $totalHealthy += (int) ($nm['healthy_checks'] ?? 0);
                $totalLatency += (float) ($nm['avg_latency_ms'] ?? 0) * $checks;
                $totalErrors  += (int) ($nm['error_count'] ?? 0);
            }

            $metrics['aggregate_metrics'] = [
                'total_checks'     => $totalChecks,
                'total_healthy'    => $totalHealthy,
                'total_errors'     => $totalErrors,
                'overall_health'   => $totalChecks > 0 ? round($totalHealthy / $totalChecks, 4) : 0,
                'avg_latency_ms'   => $totalChecks > 0 ? round($totalLatency / $totalChecks, 1) : 0,
                'node_count'       => count($nodeMetrics),
            ];
        } catch (\Throwable) {
            $metrics['error'] = 'Failed to compute metrics';
        }

        return $metrics;
    }
}

