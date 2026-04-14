<?php

declare(strict_types=1);

namespace CxAI\CxPHP\Cluster;

use CxAI\CxPHP\Session\SessionManager;
use CxAI\CxPHP\Acp\AcpBridgeInterface;
use CxAI\CxPHP\Acp\CxModelBridge;

/**
 * ChatAutomationBridge — wires chat operations to the automation pipeline.
 *
 * This bridge:
 *   - Listens to chat events and triggers scheduled automation tasks
 *   - Enables per-chat triggers for flows, health checks, and custom callbacks
 *   - Supports conditional automation based on chat properties (model, tokens, latency)
 *   - Integrates with the EventDispatcher for decoupled event handling
 *
 * Usage:
 * ```php
 * $automation = new ChatAutomationBridge($scheduler, $dispatcher);
 *
 * // Register automation rules
 * $automation->registerChatTrigger([
 *     'name' => 'high-token-flow',
 *     'conditions' => ['total_tokens' => ['op' => 'gt', 'value' => 1000]],
 *     'action' => ['type' => 'flow', 'flow_id' => 'summarize-long-chat'],
 * ]);
 *
 * // After chat completion, trigger automations
 * $automation->afterChat($sessionId, $message, $reply, $model, $tokensIn, $tokensOut, $latencyMs);
 * ```
 */
class ChatAutomationBridge
{
    /** Task type constant for chat-triggered tasks. */
    public const TYPE_CHAT_TRIGGER = 'chat_trigger';

    /** Trigger type for chat-based triggers. */
    public const TRIGGER_CHAT = 'chat';

    /** Default prefix for auto-generated chat task IDs. */
    private const TASK_PREFIX = 'chat-auto-';

    /** @var list<array{name: string, conditions: array, action: array, enabled: bool}> */
    private array $chatTriggers = [];

    /** @var bool Whether automation is enabled globally. */
    private bool $enabled = true;

    /** @var int Maximum automations per chat (rate limiting). */
    private int $maxTriggersPerChat = 10;

    public function __construct(
        private readonly AutonomousScheduler $scheduler,
        private readonly EventDispatcher $dispatcher,
        private readonly ?AgenticFlowEngine $flowEngine = null,
        private readonly ?SessionManager $sessions = null,
    ) {
        $this->registerEventListeners();
    }

    /**
     * Create from environment.
     */
    public static function fromEnv(
        AutonomousScheduler $scheduler,
        EventDispatcher $dispatcher,
        ?AgenticFlowEngine $flowEngine = null,
        ?SessionManager $sessions = null,
    ): self {
        return new self($scheduler, $dispatcher, $flowEngine, $sessions);
    }

    // ─── Configuration ───────────────────────────────────────────────────────

    /**
     * Enable or disable chat automation globally.
     */
    public function setEnabled(bool $enabled): void
    {
        $this->enabled = $enabled;
    }

    /**
     * Set maximum triggers per chat (rate limiting).
     */
    public function setMaxTriggersPerChat(int $max): void
    {
        $this->maxTriggersPerChat = max(1, $max);
    }

    // ─── Chat Trigger Registration ───────────────────────────────────────────

    /**
     * Register a chat trigger rule.
     *
     * @param array $trigger Trigger configuration:
     *   - name: string - Unique trigger name
     *   - conditions: array - Conditions to match (see evaluateConditions)
     *   - action: array - Action to take:
     *     - type: 'flow'|'task'|'callback'|'event'|'webhook'
     *     - flow_id: string (for type=flow)
     *     - task_id: string (for type=task)
     *     - callback: callable (for type=callback)
     *     - event: string (for type=event)
     *     - webhook_url: string (for type=webhook)
     *   - enabled: bool - Whether trigger is active (default: true)
     *   - priority: int - Higher runs first (default: 0)
     *
     * @return bool Success/failure.
     */
    public function registerChatTrigger(array $trigger): bool
    {
        $name = (string) ($trigger['name'] ?? '');
        if ($name === '') {
            return false;
        }

        // Check for duplicate
        foreach ($this->chatTriggers as $existing) {
            if ($existing['name'] === $name) {
                return false;
            }
        }

        $this->chatTriggers[] = [
            'name' => $name,
            'conditions' => (array) ($trigger['conditions'] ?? []),
            'action' => (array) ($trigger['action'] ?? []),
            'enabled' => (bool) ($trigger['enabled'] ?? true),
            'priority' => (int) ($trigger['priority'] ?? 0),
        ];

        // Sort by priority (higher first)
        usort($this->chatTriggers, fn($a, $b) => $b['priority'] <=> $a['priority']);

        return true;
    }

    /**
     * Unregister a chat trigger by name.
     */
    public function unregisterChatTrigger(string $name): bool
    {
        $initialCount = count($this->chatTriggers);
        $this->chatTriggers = array_values(array_filter(
            $this->chatTriggers,
            fn($t) => $t['name'] !== $name,
        ));
        return count($this->chatTriggers) < $initialCount;
    }

    /**
     * Get all registered chat triggers.
     *
     * @return list<array{name: string, conditions: array, action: array, enabled: bool}>
     */
    public function getChatTriggers(): array
    {
        return $this->chatTriggers;
    }

    /**
     * Enable or disable a specific trigger.
     */
    public function setTriggerEnabled(string $name, bool $enabled): bool
    {
        foreach ($this->chatTriggers as &$trigger) {
            if ($trigger['name'] === $name) {
                $trigger['enabled'] = $enabled;
                return true;
            }
        }
        return false;
    }

    // ─── Chat Lifecycle Hooks ────────────────────────────────────────────────

    /**
     * Hook to call before chat processing.
     *
     * Can be used for:
     *   - Pre-chat logging
     *   - Input validation/sanitization automation
     *   - Rate limiting checks
     *
     * @return array Augmented options (e.g., system_prompt additions).
     */
    public function beforeChat(
        string $sessionId,
        string $message,
        string $modelId = '',
        array $options = [],
    ): array {
        if (!$this->enabled) {
            return $options;
        }

        // Dispatch pre-chat event
        $this->dispatcher->chatRequested($sessionId, $message, $modelId, $options);

        // Could add pre-chat automations here (input filtering, etc.)

        return $options;
    }

    /**
     * Hook to call after successful chat completion.
     *
     * Triggers all matching chat automation rules.
     *
     * @return array{triggered: int, actions: list<string>} Automation results.
     */
    public function afterChat(
        string $sessionId,
        string $userMessage,
        string $aiReply,
        string $modelId,
        int $tokensIn,
        int $tokensOut,
        int $latencyMs,
        ?string $messageId = null,
        array $extra = [],
    ): array {
        if (!$this->enabled) {
            return ['triggered' => 0, 'actions' => []];
        }

        // Build event data for condition evaluation
        $chatData = [
            'session_id' => $sessionId,
            'user_message' => $userMessage,
            'ai_reply' => $aiReply,
            'model_id' => $modelId,
            'tokens_in' => $tokensIn,
            'tokens_out' => $tokensOut,
            'total_tokens' => $tokensIn + $tokensOut,
            'latency_ms' => $latencyMs,
            'message_id' => $messageId,
            'reply_length' => strlen($aiReply),
            'message_length' => strlen($userMessage),
            'timestamp' => gmdate('c'),
            ...$extra,
        ];

        // Dispatch chat.completed event (auto-triggers scheduler tasks)
        $this->dispatcher->chatCompleted(
            $sessionId,
            $userMessage,
            $aiReply,
            $modelId,
            $tokensIn,
            $tokensOut,
            $latencyMs,
            $messageId,
            $extra,
        );

        // Run chat triggers
        $triggered = 0;
        $actions = [];

        foreach ($this->chatTriggers as $trigger) {
            if ($triggered >= $this->maxTriggersPerChat) {
                break;
            }

            if (!$trigger['enabled']) {
                continue;
            }

            if (!$this->evaluateConditions($trigger['conditions'], $chatData)) {
                continue;
            }

            $actionResult = $this->executeAction($trigger['action'], $chatData);
            if ($actionResult !== null) {
                $triggered++;
                $actions[] = $trigger['name'];
            }
        }

        return [
            'triggered' => $triggered,
            'actions' => $actions,
        ];
    }

    /**
     * Hook to call on chat failure.
     */
    public function onChatError(
        string $sessionId,
        string $message,
        string $error,
        string $modelId = '',
        int $latencyMs = 0,
    ): void {
        if (!$this->enabled) {
            return;
        }

        $this->dispatcher->chatFailed($sessionId, $message, $error, $modelId, $latencyMs);
    }

    // ─── Channel Automation Hooks ────────────────────────────────────────────

    /**
     * Hook for channel creation.
     */
    public function onChannelCreated(
        string $sessionId,
        string $channelId,
        string $channelName,
    ): void {
        if (!$this->enabled) {
            return;
        }

        $this->dispatcher->channelCreated($sessionId, $channelId, $channelName);
    }

    /**
     * Hook for channel message.
     */
    public function onChannelMessage(
        string $sessionId,
        string $channelId,
        string $content,
        ?string $aiReply = null,
        ?string $modelId = null,
    ): void {
        if (!$this->enabled) {
            return;
        }

        $this->dispatcher->channelMessage($sessionId, $channelId, $content, $aiReply, $modelId);
    }

    /**
     * Hook for channel broadcast.
     */
    public function onChannelBroadcast(
        string $sessionId,
        string $channelId,
        string $content,
        array $responses,
    ): void {
        if (!$this->enabled) {
            return;
        }

        $this->dispatcher->channelBroadcast($sessionId, $channelId, $content, $responses);
    }

    // ─── Pre-built Automation Rules ──────────────────────────────────────────

    /**
     * Register default chat automation rules.
     *
     * @return int Number of rules registered.
     */
    public function registerDefaultTriggers(): int
    {
        $triggers = [
            // Long conversation summarization trigger
            [
                'name' => 'auto-summarize-long',
                'conditions' => [
                    'total_tokens' => ['op' => 'gt', 'value' => 2000],
                ],
                'action' => [
                    'type' => 'event',
                    'event' => 'chat.needs_summary',
                ],
                'priority' => 10,
            ],

            // High-latency notification
            [
                'name' => 'auto-notify-slow',
                'conditions' => [
                    'latency_ms' => ['op' => 'gt', 'value' => 10000],
                ],
                'action' => [
                    'type' => 'event',
                    'event' => 'chat.slow_response',
                ],
                'priority' => 5,
            ],

            // Model usage tracking
            [
                'name' => 'auto-track-model',
                'conditions' => [],
                'action' => [
                    'type' => 'event',
                    'event' => 'chat.model_used',
                ],
                'priority' => 0,
            ],
        ];

        $count = 0;
        foreach ($triggers as $trigger) {
            if ($this->registerChatTrigger($trigger)) {
                $count++;
            }
        }

        return $count;
    }

    /**
     * Register a flow trigger that executes when chat matches conditions.
     */
    public function registerFlowTrigger(
        string $name,
        string $flowId,
        array $conditions = [],
        int $priority = 0,
    ): bool {
        return $this->registerChatTrigger([
            'name' => $name,
            'conditions' => $conditions,
            'action' => [
                'type' => 'flow',
                'flow_id' => $flowId,
            ],
            'priority' => $priority,
        ]);
    }

    /**
     * Register a task trigger that executes a scheduled task.
     */
    public function registerTaskTrigger(
        string $name,
        string $taskId,
        array $conditions = [],
        int $priority = 0,
    ): bool {
        return $this->registerChatTrigger([
            'name' => $name,
            'conditions' => $conditions,
            'action' => [
                'type' => 'task',
                'task_id' => $taskId,
            ],
            'priority' => $priority,
        ]);
    }

    /**
     * Register a callback trigger for custom automation.
     */
    public function registerCallbackTrigger(
        string $name,
        callable $callback,
        array $conditions = [],
        int $priority = 0,
    ): bool {
        return $this->registerChatTrigger([
            'name' => $name,
            'conditions' => $conditions,
            'action' => [
                'type' => 'callback',
                'callback' => $callback,
            ],
            'priority' => $priority,
        ]);
    }

    // ─── Scheduled Task Integration ──────────────────────────────────────────

    /**
     * Schedule a task that triggers on specific chat events.
     *
     * This creates a scheduler task with event-based triggers.
     */
    public function scheduleOnChatEvent(
        string $taskId,
        string $name,
        string $taskType,
        array $config,
        array $conditions = [],
    ): bool {
        return $this->scheduler->scheduleTask(
            $taskId,
            $name,
            $taskType,
            $config,
            [], // No schedule - event-driven only
            [
                'event' => EventDispatcher::EVENT_CHAT_COMPLETED,
                'conditions' => $conditions,
            ],
        );
    }

    /**
     * Schedule a flow to run after chats matching conditions.
     */
    public function scheduleFlowOnChat(
        string $flowId,
        string $name,
        array $conditions = [],
    ): bool {
        return $this->scheduleOnChatEvent(
            self::TASK_PREFIX . "flow-{$flowId}",
            $name,
            AutonomousScheduler::TYPE_FLOW,
            ['flow_id' => $flowId],
            $conditions,
        );
    }

    // ─── Statistics ──────────────────────────────────────────────────────────

    /**
     * Get automation statistics.
     *
     * @return array<string, mixed>
     */
    public function getStats(): array
    {
        return [
            'enabled' => $this->enabled,
            'total_triggers' => count($this->chatTriggers),
            'active_triggers' => count(array_filter(
                $this->chatTriggers,
                fn($t) => $t['enabled'],
            )),
            'max_per_chat' => $this->maxTriggersPerChat,
            'event_log_size' => count($this->dispatcher->getEventLog()),
            'scheduler_connected' => true,
            'flow_engine_connected' => $this->flowEngine !== null,
        ];
    }

    // ─── Internal Methods ────────────────────────────────────────────────────

    /**
     * Register event listeners with the dispatcher.
     */
    private function registerEventListeners(): void
    {
        // Log all chat events for debugging/analytics
        $this->dispatcher->on(EventDispatcher::EVENT_CHAT_COMPLETED, function (array $data): void {
            // Could add persistent logging here
        }, -100);

        $this->dispatcher->on(EventDispatcher::EVENT_CHAT_FAILED, function (array $data): void {
            // Could trigger alerting/recovery here
        }, -100);
    }

    /**
     * Evaluate trigger conditions against chat data.
     */
    private function evaluateConditions(array $conditions, array $chatData): bool
    {
        if (empty($conditions)) {
            return true; // No conditions = always match
        }

        foreach ($conditions as $field => $expected) {
            $actual = $chatData[$field] ?? null;

            if (is_array($expected)) {
                // Operator-based condition
                $op = (string) ($expected['op'] ?? 'eq');
                $value = $expected['value'] ?? null;

                $pass = match ($op) {
                    'eq' => $actual === $value,
                    'neq' => $actual !== $value,
                    'gt' => is_numeric($actual) && $actual > $value,
                    'gte' => is_numeric($actual) && $actual >= $value,
                    'lt' => is_numeric($actual) && $actual < $value,
                    'lte' => is_numeric($actual) && $actual <= $value,
                    'in' => is_array($value) && in_array($actual, $value, true),
                    'contains' => is_string($actual) && is_string($value) && str_contains($actual, $value),
                    'regex' => is_string($actual) && is_string($value) && preg_match($value, $actual) === 1,
                    'exists' => $actual !== null,
                    'not_exists' => $actual === null,
                    default => true,
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

    /**
     * Execute a trigger action.
     *
     * @return mixed Action result, or null if action failed/skipped.
     */
    private function executeAction(array $action, array $chatData): mixed
    {
        $type = (string) ($action['type'] ?? '');

        return match ($type) {
            'flow' => $this->executeFlowAction($action, $chatData),
            'task' => $this->executeTaskAction($action, $chatData),
            'callback' => $this->executeCallbackAction($action, $chatData),
            'event' => $this->executeEventAction($action, $chatData),
            'webhook' => $this->executeWebhookAction($action, $chatData),
            default => null,
        };
    }

    private function executeFlowAction(array $action, array $chatData): ?array
    {
        if ($this->flowEngine === null) {
            return null;
        }

        $flowId = (string) ($action['flow_id'] ?? '');
        if ($flowId === '') {
            return null;
        }

        $input = (string) ($action['input'] ?? $chatData['ai_reply'] ?? '');

        try {
            return $this->flowEngine->execute($flowId, $input);
        } catch (\Throwable) {
            return null;
        }
    }

    private function executeTaskAction(array $action, array $chatData): ?array
    {
        $taskId = (string) ($action['task_id'] ?? '');
        if ($taskId === '') {
            return null;
        }

        try {
            return $this->scheduler->executeTask($taskId, self::TRIGGER_CHAT);
        } catch (\Throwable) {
            return null;
        }
    }

    private function executeCallbackAction(array $action, array $chatData): mixed
    {
        $callback = $action['callback'] ?? null;
        if (!is_callable($callback)) {
            return null;
        }

        try {
            return $callback($chatData, $this);
        } catch (\Throwable) {
            return null;
        }
    }

    private function executeEventAction(array $action, array $chatData): int
    {
        $event = (string) ($action['event'] ?? '');
        if ($event === '') {
            return 0;
        }

        return $this->dispatcher->dispatch($event, $chatData);
    }

    private function executeWebhookAction(array $action, array $chatData): ?bool
    {
        $url = (string) ($action['webhook_url'] ?? '');
        if ($url === '' || !filter_var($url, FILTER_VALIDATE_URL)) {
            return null;
        }

        // SSRF protection: prevent requests to internal IP ranges
        if (!$this->isExternalUrl($url)) {
            return null;
        }

        $payload = json_encode([
            'event' => 'chat.automation',
            'data' => $chatData,
            'timestamp' => gmdate('c'),
        ]);

        $context = stream_context_create([
            'http' => [
                'method' => 'POST',
                'header' => "Content-Type: application/json\r\n",
                'content' => $payload,
                'timeout' => 5,
            ],
        ]);

        try {
            $result = @file_get_contents($url, false, $context);
            return $result !== false;
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * Check if a URL points to an external host (SSRF protection).
     *
     * Blocks requests to:
     * - localhost / 127.0.0.0/8
     * - Private networks: 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
     * - Link-local: 169.254.0.0/16
     * - IPv6 loopback and link-local
     */
    private function isExternalUrl(string $url): bool
    {
        $parsed = parse_url($url);
        if ($parsed === false || !isset($parsed['host'])) {
            return false;
        }

        $host = strtolower($parsed['host']);

        // Block localhost variations
        if ($host === 'localhost' || $host === '127.0.0.1' || $host === '::1') {
            return false;
        }

        // Resolve hostname to IP for additional checks
        $ip = gethostbyname($host);

        // If gethostbyname returns the same value, it means:
        // 1. DNS resolution failed, OR
        // 2. The input was already an IP address
        // In either case, check if it's a valid IP and if so, check if it's private
        if ($ip === $host) {
            // Check if the host is an IP address (not a hostname)
            if (filter_var($host, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4)) {
                // It's a valid IPv4 address - check if private
                return !$this->isPrivateIp($host);
            }
            // Check for IPv6
            if (filter_var($host, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6)) {
                // Block IPv6 loopback and link-local
                if ($host === '::1' || strpos($host, 'fe80:') === 0) {
                    return false;
                }
                return true;
            }
            // DNS resolution failed for a hostname - be cautious
            if (preg_match('/^(localhost|internal|local|private)/i', $host)) {
                return false;
            }
            return true;
        }

        // Check for private/internal IP ranges
        return !$this->isPrivateIp($ip);
    }

    /**
     * Check if an IP address is private/internal.
     */
    private function isPrivateIp(string $ip): bool
    {
        // IPv4 private ranges
        $privateRanges = [
            ['127.0.0.0', '127.255.255.255'],    // Loopback
            ['10.0.0.0', '10.255.255.255'],      // Class A private
            ['172.16.0.0', '172.31.255.255'],    // Class B private
            ['192.168.0.0', '192.168.255.255'],  // Class C private
            ['169.254.0.0', '169.254.255.255'],  // Link-local
            ['0.0.0.0', '0.255.255.255'],        // Reserved
        ];

        $ipLong = ip2long($ip);
        if ($ipLong === false) {
            // IPv6 or invalid - be cautious with IPv6 loopback
            if ($ip === '::1' || strpos($ip, 'fe80:') === 0) {
                return true;
            }
            return false;
        }

        foreach ($privateRanges as [$start, $end]) {
            $startLong = ip2long($start);
            $endLong = ip2long($end);
            if ($ipLong >= $startLong && $ipLong <= $endLong) {
                return true;
            }
        }

        return false;
    }
}
