<?php

declare(strict_types=1);

namespace CxAI\CxPHP\Cluster;

use CxAI\CxPHP\Acp\CxModelBridge;
use CxAI\CxPHP\Acp\Provider\ProviderInterface;

/**
 * HealthWatchdog — continuous model health monitoring with self-healing.
 *
 * Provides zero-intervention health tracking:
 *   - Monitors all cx-model-* providers
 *   - Detects degradation (latency, errors)
 *   - Triggers automatic failover when models become unhealthy
 *   - Attempts recovery probes for unhealthy models
 *   - Emits events for EventDispatcher + ChatAutomationBridge
 *
 * Designed to run inline (per-request) or via AutonomousOrchestrator tick.
 */
class HealthWatchdog
{
    /** Maximum tracked history per model. */
    private const MAX_HISTORY = 50;

    /** Latency threshold (ms) for degraded status. */
    private const LATENCY_DEGRADED = 5000;

    /** Latency threshold (ms) for unhealthy status. */
    private const LATENCY_UNHEALTHY = 15000;

    /** Consecutive failures before marking unhealthy. */
    private const FAILURE_THRESHOLD = 3;

    /** Recovery probe interval (seconds). */
    private const RECOVERY_PROBE_INTERVAL = 60;

    /** @var array<string, list<array{timestamp: float, healthy: bool, latency_ms: int, error?: string}>> */
    private array $history = [];

    /** @var array<string, string> Model statuses: healthy, degraded, unhealthy, recovering */
    private array $statuses = [];

    /** @var array<string, float> Last recovery probe time per model. */
    private array $lastRecoveryProbe = [];

    /** @var ?EventDispatcher For emitting health events. */
    private ?EventDispatcher $dispatcher;

    public function __construct(?EventDispatcher $dispatcher = null)
    {
        $this->dispatcher = $dispatcher;
        $this->initializeModels();
    }

    /**
     * Initialize with all models from config.
     */
    private function initializeModels(): void
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
            $this->statuses[(string) $modelId] = 'healthy';
            $this->history[(string) $modelId] = [];
        }
    }

    /**
     * Record a health observation for a model.
     */
    public function record(string $modelId, bool $healthy, int $latencyMs, string $error = ''): void
    {
        if (!isset($this->statuses[$modelId])) {
            $this->statuses[$modelId] = 'healthy';
            $this->history[$modelId] = [];
        }

        $entry = [
            'timestamp'  => microtime(true),
            'healthy'    => $healthy,
            'latency_ms' => $latencyMs,
        ];
        if ($error !== '') {
            $entry['error'] = $error;
        }

        $this->history[$modelId][] = $entry;

        // Trim history
        if (count($this->history[$modelId]) > self::MAX_HISTORY) {
            $this->history[$modelId] = array_slice($this->history[$modelId], -self::MAX_HISTORY);
        }

        $this->evaluateModelStatus($modelId);
    }

    /**
     * Evaluate and update a model's status based on recent history.
     */
    private function evaluateModelStatus(string $modelId): void
    {
        $recent = array_slice($this->history[$modelId] ?? [], -5);
        if (empty($recent)) {
            return;
        }

        $failures = 0;
        $totalLatency = 0;
        $count = count($recent);

        foreach ($recent as $entry) {
            if (!$entry['healthy']) {
                $failures++;
            }
            $totalLatency += $entry['latency_ms'];
        }

        $avgLatency = $count > 0 ? $totalLatency / $count : 0;
        $previousStatus = $this->statuses[$modelId];

        if ($failures >= self::FAILURE_THRESHOLD) {
            $this->statuses[$modelId] = 'unhealthy';
        } elseif ($avgLatency > self::LATENCY_UNHEALTHY) {
            $this->statuses[$modelId] = 'unhealthy';
        } elseif ($avgLatency > self::LATENCY_DEGRADED || $failures > 0) {
            $this->statuses[$modelId] = 'degraded';
        } else {
            $this->statuses[$modelId] = 'healthy';
        }

        // Emit event on status change
        if ($previousStatus !== $this->statuses[$modelId] && $this->dispatcher !== null) {
            $this->dispatcher->dispatch('model.health_changed', [
                'model_id'        => $modelId,
                'previous_status' => $previousStatus,
                'new_status'      => $this->statuses[$modelId],
                'avg_latency_ms'  => (int) $avgLatency,
                'failure_count'   => $failures,
            ]);
        }
    }

    /**
     * Run recovery probes for unhealthy models.
     *
     * @return array<string, array{probed: bool, recovered: bool, latency_ms: int, error?: string}>
     */
    public function runRecoveryProbes(): array
    {
        $results = [];
        $now = microtime(true);

        foreach ($this->statuses as $modelId => $status) {
            if ($status !== 'unhealthy' && $status !== 'degraded') {
                continue;
            }

            $lastProbe = $this->lastRecoveryProbe[$modelId] ?? 0.0;
            if (($now - $lastProbe) < self::RECOVERY_PROBE_INTERVAL) {
                $results[$modelId] = ['probed' => false, 'recovered' => false, 'latency_ms' => 0];
                continue;
            }

            $this->lastRecoveryProbe[$modelId] = $now;

            try {
                $bridge   = CxModelBridge::forModel($modelId);
                $provider = $this->extractProvider($bridge);

                if ($provider === null) {
                    $results[$modelId] = ['probed' => true, 'recovered' => false, 'latency_ms' => 0, 'error' => 'No provider'];
                    continue;
                }

                $health = $provider->healthCheck();
                $this->record($modelId, $health['healthy'], $health['latency_ms'] ?? 0, $health['error'] ?? '');

                $results[$modelId] = [
                    'probed'     => true,
                    'recovered'  => $health['healthy'],
                    'latency_ms' => $health['latency_ms'] ?? 0,
                ];

                if ($health['healthy']) {
                    $this->statuses[$modelId] = 'recovering';
                }
            } catch (\Throwable $e) {
                $this->record($modelId, false, 0, $e->getMessage());
                $results[$modelId] = ['probed' => true, 'recovered' => false, 'latency_ms' => 0, 'error' => $e->getMessage()];
            }
        }

        return $results;
    }

    /**
     * Get the status of all models.
     *
     * @return array<string, string>
     */
    public function getStatuses(): array
    {
        return $this->statuses;
    }

    /**
     * Get the status of a specific model.
     */
    public function getModelStatus(string $modelId): string
    {
        return $this->statuses[$modelId] ?? 'unknown';
    }

    /**
     * Get healthy model IDs.
     *
     * @return list<string>
     */
    public function getHealthyModels(): array
    {
        return array_keys(array_filter($this->statuses, fn($s) => $s === 'healthy' || $s === 'recovering'));
    }

    /**
     * Get full summary report.
     *
     * @return array{models: array<string, array{status: string, avg_latency_ms: int, recent_errors: int}>, healthy_count: int, degraded_count: int, unhealthy_count: int}
     */
    public function getSummary(): array
    {
        $models = [];
        $healthyCount = 0;
        $degradedCount = 0;
        $unhealthyCount = 0;

        foreach ($this->statuses as $modelId => $status) {
            $recent = array_slice($this->history[$modelId] ?? [], -10);
            $totalLatency = 0;
            $errors = 0;
            foreach ($recent as $entry) {
                $totalLatency += $entry['latency_ms'];
                if (!$entry['healthy']) {
                    $errors++;
                }
            }

            $models[$modelId] = [
                'status'         => $status,
                'avg_latency_ms' => count($recent) > 0 ? (int) ($totalLatency / count($recent)) : 0,
                'recent_errors'  => $errors,
            ];

            match ($status) {
                'healthy', 'recovering' => $healthyCount++,
                'degraded' => $degradedCount++,
                'unhealthy' => $unhealthyCount++,
                default => null,
            };
        }

        return [
            'models'          => $models,
            'healthy_count'   => $healthyCount,
            'degraded_count'  => $degradedCount,
            'unhealthy_count' => $unhealthyCount,
        ];
    }

    /**
     * Extract provider from bridge via reflection.
     */
    private function extractProvider(CxModelBridge $bridge): ?ProviderInterface
    {
        try {
            $ref = new \ReflectionClass($bridge);
            $prop = $ref->getProperty('provider');
            return $prop->getValue($bridge);
        } catch (\Throwable) {
            return null;
        }
    }
}
