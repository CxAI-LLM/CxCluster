<?php

declare(strict_types=1);

namespace CxAI\CxPHP\Cluster;

use CxAI\CxPHP\Database\CxLLM;

/**
 * EventDispatcher — centralized event broadcasting for automation triggers.
 *
 * Provides a pub/sub mechanism for:
 *   - Chat lifecycle events (chat.requested, chat.completed, chat.failed)
 *   - Channel events (channel.created, channel.message, channel.broadcast)
 *   - Session events (session.created, session.destroyed)
 *   - Scheduler events (task.completed, task.failed)
 *   - Plugin events (custom plugin hooks)
 *
 * Usage:
 * ```php
 * $dispatcher = new EventDispatcher();
 *
 * // Register listener
 * $dispatcher->on('chat.completed', function (array $event) {
 *     // Handle completed chat
 * });
 *
 * // Dispatch event
 * $dispatcher->dispatch('chat.completed', [
 *     'session_id' => $sessionId,
 *     'message' => $message,
 *     'reply' => $reply,
 * ]);
 * ```
 */
class EventDispatcher
{
    // ─── Standard Event Types ────────────────────────────────────────────────

    // Chat lifecycle events
    public const EVENT_CHAT_REQUESTED  = 'chat.requested';
    public const EVENT_CHAT_COMPLETED  = 'chat.completed';
    public const EVENT_CHAT_FAILED     = 'chat.failed';

    // Channel events
    public const EVENT_CHANNEL_CREATED   = 'channel.created';
    public const EVENT_CHANNEL_MESSAGE   = 'channel.message';
    public const EVENT_CHANNEL_BROADCAST = 'channel.broadcast';

    // Session events
    public const EVENT_SESSION_CREATED   = 'session.created';
    public const EVENT_SESSION_DESTROYED = 'session.destroyed';

    // Scheduler events
    public const EVENT_TASK_SCHEDULED = 'task.scheduled';
    public const EVENT_TASK_COMPLETED = 'task.completed';
    public const EVENT_TASK_FAILED    = 'task.failed';

    // Agent events
    public const EVENT_AGENT_INVOKED   = 'agent.invoked';
    public const EVENT_AGENT_COMPLETED = 'agent.completed';

    // Flow events
    public const EVENT_FLOW_STARTED   = 'flow.started';
    public const EVENT_FLOW_COMPLETED = 'flow.completed';
    public const EVENT_FLOW_FAILED    = 'flow.failed';

    /** @var array<string, list<callable>> Event listeners keyed by event type. */
    private array $listeners = [];

    /** @var array<string, list<array{callable: callable, priority: int}>> Prioritized listeners. */
    private array $prioritizedListeners = [];

    /** @var bool Whether priorities have been sorted. */
    private bool $sorted = true;

    /** @var ?AutonomousScheduler Scheduler reference for automatic task triggering. */
    private ?AutonomousScheduler $scheduler = null;

    /** @var bool Whether to propagate events to the scheduler automatically. */
    private bool $autoTriggerScheduler = true;

    /** @var list<array{event: string, data: array, timestamp: string}> Event log for debugging. */
    private array $eventLog = [];

    /** @var int Maximum event log size. */
    private int $maxLogSize = 100;

    // ─── Scheduler Integration ───────────────────────────────────────────────

    /**
     * Set the scheduler instance for automatic event-to-task triggering.
     */
    public function setScheduler(AutonomousScheduler $scheduler): void
    {
        $this->scheduler = $scheduler;
    }

    /**
     * Enable or disable automatic scheduler triggering on events.
     */
    public function setAutoTriggerScheduler(bool $enabled): void
    {
        $this->autoTriggerScheduler = $enabled;
    }

    // ─── Listener Registration ───────────────────────────────────────────────

    /**
     * Register a listener for an event type.
     *
     * @param string   $eventType  Event type to listen for (e.g., 'chat.completed').
     * @param callable $listener   Callback: fn(array $eventData): void
     * @param int      $priority   Higher priority listeners execute first (default: 0).
     */
    public function on(string $eventType, callable $listener, int $priority = 0): void
    {
        $this->prioritizedListeners[$eventType][] = [
            'callable' => $listener,
            'priority' => $priority,
        ];
        $this->sorted = false;
    }

    /**
     * Register a one-time listener that auto-removes after first invocation.
     */
    public function once(string $eventType, callable $listener, int $priority = 0): void
    {
        $wrapper = null;
        $wrapper = function (array $data) use ($eventType, $listener, &$wrapper): void {
            $this->off($eventType, $wrapper);
            $listener($data);
        };
        $this->on($eventType, $wrapper, $priority);
    }

    /**
     * Remove a listener for an event type.
     */
    public function off(string $eventType, callable $listener): void
    {
        if (!isset($this->prioritizedListeners[$eventType])) {
            return;
        }

        $this->prioritizedListeners[$eventType] = array_filter(
            $this->prioritizedListeners[$eventType],
            fn(array $entry) => $entry['callable'] !== $listener,
        );

        // Mark as unsorted so the listeners cache is rebuilt
        $this->sorted = false;
    }

    /**
     * Remove all listeners for an event type (or all listeners if no type specified).
     */
    public function removeAllListeners(?string $eventType = null): void
    {
        if ($eventType === null) {
            $this->prioritizedListeners = [];
            $this->listeners = [];
        } else {
            unset($this->prioritizedListeners[$eventType], $this->listeners[$eventType]);
        }
    }

    // ─── Event Dispatching ───────────────────────────────────────────────────

    /**
     * Dispatch an event to all registered listeners.
     *
     * @param string $eventType  Event type (e.g., 'chat.completed').
     * @param array  $eventData  Event payload data.
     *
     * @return int Number of listeners that were invoked.
     */
    public function dispatch(string $eventType, array $eventData = []): int
    {
        // Ensure listeners are sorted by priority
        if (!$this->sorted) {
            $this->sortListeners();
        }

        // Add standard metadata
        $eventData['_event_type'] = $eventType;
        $eventData['_timestamp'] = gmdate('c');
        $eventData['_dispatch_id'] = bin2hex(random_bytes(8));

        // Log event for debugging
        $this->logEvent($eventType, $eventData);

        // Persist event to MongoDB for audit trail
        try {
            $mongo = CxLLM::getInstance();
            if ($mongo->isConfigured()) {
                $mongo->logAutomationEvent([
                    'event_type' => $eventType,
                    'trigger'    => $eventData['trigger'] ?? 'dispatch',
                    'action'     => $eventData['action'] ?? null,
                    'model_id'   => $eventData['model_id'] ?? $eventData['model'] ?? null,
                    'session_id' => $eventData['session_id'] ?? null,
                    'payload'    => array_diff_key($eventData, array_flip(['_event_type', '_timestamp', '_dispatch_id'])),
                ]);
            }
        } catch (\Throwable) {
            // MongoDB persistence is best-effort
        }

        // Invoke listeners
        $listeners = $this->listeners[$eventType] ?? [];
        $invoked = 0;

        foreach ($listeners as $listener) {
            try {
                $listener($eventData);
                $invoked++;
            } catch (\Throwable $e) {
                // Log but don't interrupt other listeners
                $this->logEvent("{$eventType}.listener_error", [
                    'error' => $e->getMessage(),
                    'original_data' => $eventData,
                ]);
            }
        }

        // Auto-trigger scheduler tasks
        if ($this->autoTriggerScheduler && $this->scheduler !== null) {
            try {
                $this->scheduler->triggerEvent($eventType, $eventData);
            } catch (\Throwable) {
                // Non-critical
            }
        }

        return $invoked;
    }

    /**
     * Dispatch an event asynchronously (non-blocking).
     *
     * Note: In PHP synchronous context, this still runs immediately but
     * captures and suppresses any exceptions from listeners.
     *
     * @return string Dispatch ID for tracking.
     */
    public function dispatchAsync(string $eventType, array $eventData = []): string
    {
        $dispatchId = bin2hex(random_bytes(8));
        $eventData['_dispatch_id'] = $dispatchId;

        // In synchronous PHP, we run immediately but catch all exceptions
        try {
            $this->dispatch($eventType, $eventData);
        } catch (\Throwable) {
            // Suppressed for async semantics
        }

        return $dispatchId;
    }

    /**
     * Dispatch multiple events in sequence.
     *
     * @param list<array{type: string, data: array}> $events
     *
     * @return int Total listeners invoked.
     */
    public function dispatchBatch(array $events): int
    {
        $total = 0;
        foreach ($events as $event) {
            $total += $this->dispatch(
                (string) ($event['type'] ?? ''),
                (array) ($event['data'] ?? []),
            );
        }
        return $total;
    }

    // ─── Chat Events (Convenience Methods) ───────────────────────────────────

    /**
     * Dispatch chat.requested event.
     */
    public function chatRequested(
        string $sessionId,
        string $message,
        string $modelId = '',
        array $options = [],
    ): void {
        $this->dispatch(self::EVENT_CHAT_REQUESTED, [
            'session_id' => $sessionId,
            'message' => $message,
            'model_id' => $modelId,
            'options' => $options,
        ]);
    }

    /**
     * Dispatch chat.completed event with full context.
     */
    public function chatCompleted(
        string $sessionId,
        string $userMessage,
        string $aiReply,
        string $modelId,
        int $tokensIn,
        int $tokensOut,
        int $latencyMs,
        ?string $messageId = null,
        array $extra = [],
    ): void {
        $this->dispatch(self::EVENT_CHAT_COMPLETED, array_merge([
            'session_id' => $sessionId,
            'user_message' => $userMessage,
            'ai_reply' => $aiReply,
            'model_id' => $modelId,
            'tokens_in' => $tokensIn,
            'tokens_out' => $tokensOut,
            'total_tokens' => $tokensIn + $tokensOut,
            'latency_ms' => $latencyMs,
            'message_id' => $messageId,
        ], $extra));
    }

    /**
     * Dispatch chat.failed event.
     */
    public function chatFailed(
        string $sessionId,
        string $message,
        string $error,
        string $modelId = '',
        int $latencyMs = 0,
    ): void {
        $this->dispatch(self::EVENT_CHAT_FAILED, [
            'session_id' => $sessionId,
            'message' => $message,
            'error' => $error,
            'model_id' => $modelId,
            'latency_ms' => $latencyMs,
        ]);
    }

    // ─── Channel Events (Convenience Methods) ────────────────────────────────

    /**
     * Dispatch channel.created event.
     */
    public function channelCreated(
        string $sessionId,
        string $channelId,
        string $channelName,
    ): void {
        $this->dispatch(self::EVENT_CHANNEL_CREATED, [
            'session_id' => $sessionId,
            'channel_id' => $channelId,
            'channel_name' => $channelName,
        ]);
    }

    /**
     * Dispatch channel.message event.
     */
    public function channelMessage(
        string $sessionId,
        string $channelId,
        string $content,
        ?string $aiReply = null,
        ?string $modelId = null,
    ): void {
        $this->dispatch(self::EVENT_CHANNEL_MESSAGE, [
            'session_id' => $sessionId,
            'channel_id' => $channelId,
            'content' => $content,
            'ai_reply' => $aiReply,
            'model_id' => $modelId,
        ]);
    }

    /**
     * Dispatch channel.broadcast event.
     */
    public function channelBroadcast(
        string $sessionId,
        string $channelId,
        string $content,
        array $responses,
    ): void {
        $this->dispatch(self::EVENT_CHANNEL_BROADCAST, [
            'session_id' => $sessionId,
            'channel_id' => $channelId,
            'content' => $content,
            'responses' => $responses,
            'response_count' => count($responses),
        ]);
    }

    // ─── Agent/Flow Events (Convenience Methods) ─────────────────────────────

    /**
     * Dispatch agent.invoked event.
     */
    public function agentInvoked(
        string $sessionId,
        string $agentId,
        string $input,
        array $config = [],
    ): void {
        $this->dispatch(self::EVENT_AGENT_INVOKED, [
            'session_id' => $sessionId,
            'agent_id' => $agentId,
            'input' => $input,
            'config' => $config,
        ]);
    }

    /**
     * Dispatch flow.completed event.
     */
    public function flowCompleted(
        string $flowId,
        string $sessionId,
        string $input,
        string $output,
        int $totalSteps,
        int $latencyMs,
        array $telemetry = [],
    ): void {
        $this->dispatch(self::EVENT_FLOW_COMPLETED, [
            'flow_id' => $flowId,
            'session_id' => $sessionId,
            'input' => $input,
            'output' => $output,
            'total_steps' => $totalSteps,
            'latency_ms' => $latencyMs,
            'telemetry' => $telemetry,
        ]);
    }

    // ─── Introspection ───────────────────────────────────────────────────────

    /**
     * Check if any listeners are registered for an event type.
     */
    public function hasListeners(string $eventType): bool
    {
        return !empty($this->prioritizedListeners[$eventType]);
    }

    /**
     * Get the number of listeners for an event type.
     */
    public function listenerCount(string $eventType): int
    {
        return count($this->prioritizedListeners[$eventType] ?? []);
    }

    /**
     * Get all event types with registered listeners.
     *
     * @return list<string>
     */
    public function getEventTypes(): array
    {
        return array_keys($this->prioritizedListeners);
    }

    /**
     * Get recent event log for debugging.
     *
     * @return list<array{event: string, data: array, timestamp: string}>
     */
    public function getEventLog(int $limit = 50): array
    {
        return array_slice($this->eventLog, -$limit);
    }

    /**
     * Clear the event log.
     */
    public function clearEventLog(): void
    {
        $this->eventLog = [];
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    /**
     * Sort listeners by priority (higher priority first).
     */
    private function sortListeners(): void
    {
        foreach ($this->prioritizedListeners as $eventType => &$entries) {
            usort($entries, fn(array $a, array $b) => $b['priority'] <=> $a['priority']);
            $this->listeners[$eventType] = array_map(
                fn(array $entry) => $entry['callable'],
                $entries,
            );
        }
        unset($entries); // break reference
        $this->sorted = true;
    }

    /**
     * Log an event for debugging.
     */
    private function logEvent(string $eventType, array $eventData): void
    {
        $this->eventLog[] = [
            'event' => $eventType,
            'data' => $eventData,
            'timestamp' => gmdate('c'),
        ];

        // Trim log if too large
        if (count($this->eventLog) > $this->maxLogSize) {
            $this->eventLog = array_slice($this->eventLog, -$this->maxLogSize);
        }
    }

    /**
     * Get standard event types for documentation.
     *
     * @return array<string, string> Event type => description
     */
    public static function getStandardEvents(): array
    {
        return [
            self::EVENT_CHAT_REQUESTED => 'Fired before a chat message is processed',
            self::EVENT_CHAT_COMPLETED => 'Fired after a successful chat completion',
            self::EVENT_CHAT_FAILED => 'Fired when a chat request fails',
            self::EVENT_CHANNEL_CREATED => 'Fired when a new channel is created',
            self::EVENT_CHANNEL_MESSAGE => 'Fired when a message is sent to a channel',
            self::EVENT_CHANNEL_BROADCAST => 'Fired when a broadcast message is sent',
            self::EVENT_SESSION_CREATED => 'Fired when a new session is created',
            self::EVENT_SESSION_DESTROYED => 'Fired when a session is destroyed',
            self::EVENT_TASK_SCHEDULED => 'Fired when a task is scheduled',
            self::EVENT_TASK_COMPLETED => 'Fired when a scheduled task completes',
            self::EVENT_TASK_FAILED => 'Fired when a scheduled task fails',
            self::EVENT_AGENT_INVOKED => 'Fired when an agent is invoked',
            self::EVENT_AGENT_COMPLETED => 'Fired when an agent completes',
            self::EVENT_FLOW_STARTED => 'Fired when a multi-step flow starts',
            self::EVENT_FLOW_COMPLETED => 'Fired when a multi-step flow completes',
            self::EVENT_FLOW_FAILED => 'Fired when a multi-step flow fails',
        ];
    }
}
