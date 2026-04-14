<?php

declare(strict_types=1);

namespace CxAI\CxPHP\Cluster;

use CxAI\CxPHP\Acp\CxModelBridge;
use CxAI\CxPHP\Database\CxLLM;
use CxAI\CxPHP\Session\SessionManager;
use CxAI\CxPHP\Supabase\MemoryPipeline;
use CxAI\CxPHP\Supabase\MLPipeline;

/**
 * Agentic Flow Engine — defines, stores, and executes multi-step LLM pipelines.
 *
 * Flows are stored in MongoDB as documents in the `agentic_flows` collection.
 * Each execution is tracked in `flow_executions` with per-step telemetry,
 * token usage, latency, and error details.
 *
 * Supports:
 *   - Multi-step sequential pipelines (step N output → step N+1 input)
 *   - Per-step model routing via ClusterOrchestrator
 *   - Memory pipeline integration (PRE/POST per step)
 *   - ML pipeline integration (cost/metrics per step)
 *   - Configurable retry logic per step
 *   - Automatic context passing between steps
 *   - Timeout and cancellation
 */
class AgenticFlowEngine
{
    /** Max steps per flow to prevent infinite loops. */
    private const MAX_STEPS = 20;

    /** Default step timeout in seconds. */
    private const DEFAULT_STEP_TIMEOUT = 120;

    /** Optional memory pipeline for flow step augmentation + extraction. */
    private ?MemoryPipeline $memoryPipeline = null;

    /** Optional ML pipeline for flow step cost/metrics tracking. */
    private ?MLPipeline $mlPipeline = null;

    public function __construct(
        private readonly CxLLM $db,
        private readonly ClusterOrchestrator $cluster,
        private readonly SessionManager $sessions,
    ) {}

    /**
     * Inject the memory pipeline for per-step augmentation and extraction.
     */
    public function setMemoryPipeline(?MemoryPipeline $pipeline): void
    {
        $this->memoryPipeline = $pipeline;
    }

    /**
     * Inject the ML pipeline for per-step cost and metrics tracking.
     */
    public function setMLPipeline(?MLPipeline $pipeline): void
    {
        $this->mlPipeline = $pipeline;
    }

    /**
     * Create from environment.
     */
    public static function fromEnv(SessionManager $sessions): self
    {
        $db = CxLLM::getInstance();
        return new self($db, new ClusterOrchestrator($db), $sessions);
    }

    // ─── Flow definitions ───────────────────────────────────────────────────

    /**
     * Create or update an agentic flow definition.
     *
     * @param string $flowId   Unique flow identifier.
     * @param string $name     Human-readable name.
     * @param list<array{model_id?: string, system_prompt?: string, name?: string, retries?: int, timeout?: int}> $steps
     * @param array  $config   Additional flow config (e.g. max_tokens, temperature).
     */
    public function defineFlow(
        string $flowId,
        string $name,
        string $description,
        array $steps,
        array $config = [],
    ): bool {
        if (count($steps) > self::MAX_STEPS) {
            return false;
        }

        // Normalise steps
        $normalised = [];
        foreach ($steps as $i => $step) {
            $normalised[] = [
                'index'         => $i,
                'name'          => $step['name'] ?? "Step {$i}",
                'model_id'      => $step['model_id'] ?? '',
                'system_prompt' => $step['system_prompt'] ?? '',
                'retries'       => max(0, min(5, (int) ($step['retries'] ?? 1))),
                'timeout'       => max(10, min(600, (int) ($step['timeout'] ?? self::DEFAULT_STEP_TIMEOUT))),
            ];
        }

        try {
            $this->db->updateOne(
                CxLLM::COLL_FLOWS,
                ['flow_id' => $flowId],
                [
                    '$set' => [
                        'flow_id'     => $flowId,
                        'name'        => $name,
                        'description' => $description,
                        'steps'       => $normalised,
                        'config'      => $config,
                        'is_active'   => true,
                    ],
                    '$inc' => ['version' => 1],
                ],
            );
            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * Get a flow definition.
     *
     * @return array<string,mixed>|null
     */
    public function getFlow(string $flowId): ?array
    {
        try {
            return $this->db->findOne(CxLLM::COLL_FLOWS, ['flow_id' => $flowId]);
        } catch (\Throwable) {
            return null;
        }
    }

    /**
     * List all flows.
     *
     * @return list<array<string,mixed>>
     */
    public function listFlows(bool $activeOnly = true): array
    {
        $filter = $activeOnly ? ['is_active' => true] : [];
        try {
            return $this->db->find(CxLLM::COLL_FLOWS, $filter, [
                'sort' => ['name' => 1],
            ]);
        } catch (\Throwable) {
            return [];
        }
    }

    /**
     * Deactivate a flow.
     */
    public function deactivateFlow(string $flowId): bool
    {
        try {
            $this->db->updateOne(
                CxLLM::COLL_FLOWS,
                ['flow_id' => $flowId],
                ['$set' => ['is_active' => false]],
                false,
            );
            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * Delete a flow and all its executions.
     */
    public function deleteFlow(string $flowId): bool
    {
        try {
            $this->db->deleteOne(CxLLM::COLL_FLOWS, ['flow_id' => $flowId]);
            $this->db->deleteMany(CxLLM::COLL_EXECUTIONS, ['flow_id' => $flowId]);
            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    // ─── Flow execution ─────────────────────────────────────────────────────

    /**
     * Execute a flow: run each step sequentially, feeding output forward.
     *
     * @param string $flowId    The flow to execute.
     * @param string $input     The initial user prompt / input.
     * @param string $sessionId Optional session to bind to.
     *
     * @return array{execution_id: string, status: string, output: string, steps_log: list<array>, total_tokens: int, total_latency_ms: int, error: string|null}
     */
    public function execute(string $flowId, string $input, string $sessionId = ''): array
    {
        $flow = $this->getFlow($flowId);
        if ($flow === null || !($flow['is_active'] ?? false)) {
            return $this->errorResult($flowId, 'Flow not found or inactive');
        }

        $executionId = $this->generateExecutionId();
        $steps       = (array) ($flow['steps'] ?? []);

        if ($steps === []) {
            return $this->errorResult($flowId, 'Flow has no steps');
        }

        // Create session if needed
        $session = null;
        if ($sessionId !== '') {
            $session = $this->sessions->get($sessionId);
        }
        if ($session === null) {
            $session = $this->sessions->create('flow-' . $flowId);
            $sessionId = $session->getId();
        }

        // Record execution start
        $this->db->insertOne(CxLLM::COLL_EXECUTIONS, [
            'execution_id' => $executionId,
            'flow_id'      => $flowId,
            'session_id'   => $sessionId,
            'status'       => 'running',
            'input'        => $input,
            'output'       => '',
            'steps_log'    => [],
            'total_tokens' => 0,
            'total_latency_ms' => 0,
            'error'        => null,
            'started_at'   => gmdate('c'),
        ]);

        $stepsLog      = [];
        $totalTokens   = 0;
        $totalLatency  = 0;
        $currentInput  = $input;
        $currentOutput = '';
        $error         = null;
        $status        = 'completed';

        foreach ($steps as $step) {
            $stepName   = (string) ($step['name'] ?? 'unnamed');
            $modelId    = (string) ($step['model_id'] ?? '');
            $sysProm    = (string) ($step['system_prompt'] ?? '');
            $retries    = (int) ($step['retries'] ?? 1);

            $stepLog = [
                'step'       => $stepName,
                'model_id'   => $modelId,
                'status'     => 'pending',
                'input'      => mb_substr($currentInput, 0, 500),
                'output'     => '',
                'tokens'     => 0,
                'latency_ms' => 0,
                'error'      => null,
                'attempts'   => 0,
            ];

            // Route model through cluster if available
            $bridge = null;
            if ($modelId !== '' && $this->cluster->isAvailable()) {
                $node = $this->cluster->selectNode($modelId);
                if ($node !== null) {
                    $modelId = (string) ($node['model_id'] ?? $modelId);
                }
            }

            try {
                if ($modelId !== '') {
                    $bridge = CxModelBridge::forModel($modelId);
                }
            } catch (\Throwable) {
                // Fall back to default bridge
            }

            if ($bridge === null) {
                $bridge = CxModelBridge::fromEnv();
            }

            // ── Memory augmentation PRE-STEP ──
            if ($this->memoryPipeline !== null && $sessionId !== '') {
                try {
                    $memCtx = $this->memoryPipeline->augmentPromptAdaptive(
                        $sessionId,
                        $currentInput,
                        $sysProm,
                    );
                    if (!empty($memCtx['augmented_prompt'])) {
                        $currentInput = $memCtx['augmented_prompt'];
                    }
                    if (!empty($memCtx['system_context'])) {
                        $sysProm .= "\n" . $memCtx['system_context'];
                    }
                } catch (\Throwable) {
                    // Non-fatal — continue without augmentation
                }
            }

            // Execute step with retries
            $success = false;
            for ($attempt = 1; $attempt <= max(1, $retries); $attempt++) {
                $stepLog['attempts'] = $attempt;

                $options = [];
                if ($sysProm !== '') {
                    $options['system_prompt'] = $sysProm;
                }

                $startMs = (int) (microtime(true) * 1000);
                try {
                    $resp    = $bridge->chat($session, $currentInput, $options);
                    $latency = (int) (microtime(true) * 1000) - $startMs;

                    if ($resp->isSuccess()) {
                        $usage  = $resp->getUsage();
                        $tokens = (int) ($usage['total_tokens'] ?? 0);

                        $stepLog['status']     = 'completed';
                        $stepLog['output']     = $resp->getContent();
                        $stepLog['tokens']     = $tokens;
                        $stepLog['latency_ms'] = $latency;
                        $stepLog['model']      = $resp->getModel();

                        $currentOutput  = $resp->getContent();
                        $currentInput   = $currentOutput; // feed forward
                        $totalTokens   += $tokens;
                        $totalLatency  += $latency;
                        $success        = true;

                        // ── ML pipeline POST-STEP ──
                        if ($this->mlPipeline !== null) {
                            try {
                                $stepProvider = '';
                                if (method_exists($bridge, 'getProvider')) {
                                    $stepProvider = (string) $bridge->getProvider();
                                }
                                $tokensIn  = (int) ($usage['prompt_tokens'] ?? 0);
                                $tokensOut = (int) ($usage['completion_tokens'] ?? 0);
                                $this->mlPipeline->afterChat(
                                    $sessionId,
                                    $resp->getModel() ?: $modelId,
                                    $stepProvider,
                                    $currentInput,
                                    $resp->getContent(),
                                    $tokensIn,
                                    $tokensOut,
                                    $latency,
                                );
                            } catch (\Throwable) {
                                // Non-critical
                            }
                        }

                        // ── Memory extraction POST-STEP ──
                        if ($this->memoryPipeline !== null && $sessionId !== '' && $session !== null) {
                            try {
                                $this->memoryPipeline->afterChatEnhanced(
                                    $sessionId,
                                    $currentInput,
                                    $resp->getContent(),
                                    $session,
                                    ['flow_step' => $stepName, 'flow_id' => $flowId],
                                );
                            } catch (\Throwable) {
                                // Non-critical
                            }
                        }

                        break;
                    }

                    $stepLog['error'] = $resp->getErrorMessage() ?: 'Provider error';
                } catch (\Throwable $e) {
                    $latency = (int) (microtime(true) * 1000) - $startMs;
                    $stepLog['latency_ms'] = $latency;
                    $totalLatency += $latency;
                    $stepLog['error'] = $e->getMessage();
                }
            }

            if (!$success) {
                $stepLog['status'] = 'failed';
                $error  = "Step '{$stepName}' failed after {$retries} attempts: " . ($stepLog['error'] ?? 'unknown');
                $status = 'failed';
                $stepsLog[] = $stepLog;
                break;
            }

            $stepsLog[] = $stepLog;
        }

        // Update execution record
        try {
            $this->db->updateOne(
                CxLLM::COLL_EXECUTIONS,
                ['execution_id' => $executionId],
                ['$set' => [
                    'status'           => $status,
                    'output'           => $currentOutput,
                    'steps_log'        => $stepsLog,
                    'total_tokens'     => $totalTokens,
                    'total_latency_ms' => $totalLatency,
                    'error'            => $error,
                    'completed_at'     => gmdate('c'),
                ]],
                false,
            );
        } catch (\Throwable) {
            // Non-critical
        }

        $this->cluster->logEvent('flow_execution', json_encode([
            'execution_id' => $executionId,
            'flow_id'      => $flowId,
            'status'       => $status,
            'steps'        => count($stepsLog),
            'total_tokens' => $totalTokens,
        ]));

        return [
            'execution_id'     => $executionId,
            'flow_id'          => $flowId,
            'session_id'       => $sessionId,
            'status'           => $status,
            'output'           => $currentOutput,
            'steps_log'        => $stepsLog,
            'total_tokens'     => $totalTokens,
            'total_latency_ms' => $totalLatency,
            'error'            => $error,
        ];
    }

    // ─── Execution history ──────────────────────────────────────────────────

    /**
     * Get execution details by execution ID.
     *
     * @return array<string,mixed>|null
     */
    public function getExecution(string $executionId): ?array
    {
        try {
            return $this->db->findOne(CxLLM::COLL_EXECUTIONS, ['execution_id' => $executionId]);
        } catch (\Throwable) {
            return null;
        }
    }

    /**
     * List executions for a flow.
     *
     * @return list<array<string,mixed>>
     */
    public function listExecutions(string $flowId, int $limit = 20): array
    {
        try {
            return $this->db->find(CxLLM::COLL_EXECUTIONS, ['flow_id' => $flowId], [
                'sort'  => ['created_at' => -1],
                'limit' => $limit,
            ]);
        } catch (\Throwable) {
            return [];
        }
    }

    /**
     * Get execution stats for a flow.
     *
     * @return array{total: int, completed: int, failed: int, avg_tokens: float, avg_latency_ms: float}
     */
    public function getFlowStats(string $flowId): array
    {
        try {
            $stats = $this->db->aggregate(CxLLM::COLL_EXECUTIONS, [
                ['$match' => ['flow_id' => $flowId]],
                ['$group' => [
                    '_id'            => '$status',
                    'count'          => ['$sum' => 1],
                    'avg_tokens'     => ['$avg' => '$total_tokens'],
                    'avg_latency_ms' => ['$avg' => '$total_latency_ms'],
                ]],
            ]);

            $total = 0;
            $completed = 0;
            $failed = 0;
            $avgTokens = 0.0;
            $avgLatency = 0.0;

            foreach ($stats as $s) {
                $count = (int) ($s['count'] ?? 0);
                $total += $count;
                if ($s['_id'] === 'completed') {
                    $completed  = $count;
                    $avgTokens  = (float) ($s['avg_tokens'] ?? 0);
                    $avgLatency = (float) ($s['avg_latency_ms'] ?? 0);
                } elseif ($s['_id'] === 'failed') {
                    $failed = $count;
                }
            }

            return [
                'total'          => $total,
                'completed'      => $completed,
                'failed'         => $failed,
                'avg_tokens'     => round($avgTokens, 1),
                'avg_latency_ms' => round($avgLatency, 1),
            ];
        } catch (\Throwable) {
            return ['total' => 0, 'completed' => 0, 'failed' => 0, 'avg_tokens' => 0, 'avg_latency_ms' => 0];
        }
    }

    // ─── Pre-built flows ────────────────────────────────────────────────────

    /**
     * Register the built-in agentic flows that ship with CxPHP.
     *
     * @return int  Number of flows registered.
     */
    public function registerBuiltInFlows(): int
    {
        $flows = [
            [
                'flow_id'     => 'research-pipeline',
                'name'        => 'Research Pipeline',
                'description' => 'Multi-model research: Cohere (research) → NVIDIA (synthesise) → Mistral (QA review)',
                'steps'       => [
                    ['name' => 'Research',    'model_id' => 'cx-model-2.0', 'system_prompt' => 'You are a research analyst. Gather comprehensive information about the topic. Be thorough and cite reasoning.'],
                    ['name' => 'Synthesise',  'model_id' => 'cx-model-3.0', 'system_prompt' => 'You are a synthesis expert. Take the research and produce a clear, well-structured analysis with actionable insights.'],
                    ['name' => 'QA Review',   'model_id' => 'cx-model-1.5', 'system_prompt' => 'You are a QA reviewer. Check the analysis for accuracy, completeness, and clarity. Provide a final polished version.'],
                ],
            ],
            [
                'flow_id'     => 'code-review-pipeline',
                'name'        => 'Code Review Pipeline',
                'description' => 'Code analysis: NVIDIA (review) → Cohere (security audit) → Mistral (final report)',
                'steps'       => [
                    ['name' => 'Code Review',    'model_id' => 'cx-model-3.0', 'system_prompt' => 'You are a senior software engineer. Review the code for bugs, performance issues, and best practices. Be specific.'],
                    ['name' => 'Security Audit', 'model_id' => 'cx-model-2.0', 'system_prompt' => 'You are a security expert. Analyse the code review and check for OWASP Top 10 vulnerabilities, injection risks, and data exposure.'],
                    ['name' => 'Final Report',   'model_id' => 'cx-model-1.5', 'system_prompt' => 'You are a technical writer. Combine the code review and security audit into a concise, actionable report.'],
                ],
            ],
            [
                'flow_id'     => 'safe-generation',
                'name'        => 'Safe Generation (Guardrails)',
                'description' => 'NemoClaw guardrails: Generate with NemoClaw (input/output guardrails enforced)',
                'steps'       => [
                    ['name' => 'Generate with Guardrails', 'model_id' => 'cx-model-3.5', 'system_prompt' => 'You are a helpful assistant. Respond accurately and safely. Do not include personal information or harmful content.', 'retries' => 2],
                ],
            ],
        ];

        $count = 0;
        foreach ($flows as $f) {
            if ($this->defineFlow($f['flow_id'], $f['name'], $f['description'], $f['steps'])) {
                $count++;
            }
        }
        return $count;
    }

    // ─── Helpers ────────────────────────────────────────────────────────────

    private function generateExecutionId(): string
    {
        return 'exec-' . bin2hex(random_bytes(8));
    }

    /**
     * @return array{execution_id: string, flow_id: string, session_id: string, status: string, output: string, steps_log: list<array>, total_tokens: int, total_latency_ms: int, error: string|null}
     */
    private function errorResult(string $flowId, string $error): array
    {
        return [
            'execution_id'     => '',
            'flow_id'          => $flowId,
            'session_id'       => '',
            'status'           => 'failed',
            'output'           => '',
            'steps_log'        => [],
            'total_tokens'     => 0,
            'total_latency_ms' => 0,
            'error'            => $error,
        ];
    }
}
