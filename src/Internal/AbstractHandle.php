<?php

namespace Amp\Postgres\Internal;

use Amp\DeferredFuture;
use Amp\Pipeline\Queue;
use Amp\Postgres\Handle;
use Amp\Sql\ConnectionException;
use Revolt\EventLoop;

/**
 * @internal
 */
abstract class AbstractHandle implements Handle
{
    protected ?DeferredFuture $pendingOperation = null;

    /** @var array<string, Queue> */
    protected array $listeners = [];

    protected int $lastUsedAt = 0;

    public function __construct(
        protected readonly string $poll,
        protected readonly string $await,
        private readonly DeferredFuture $onClose,
    ) {
        $this->lastUsedAt = \time();
    }

    public function __destruct()
    {
        if (!$this->isClosed()) {
            $this->close();
        }
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    public function close(): void
    {
        self::shutdown($this->listeners, $this->pendingOperation, $this->onClose);

        EventLoop::cancel($this->poll);
        EventLoop::cancel($this->await);
    }

    /**
     * @param array<string, Queue> $listeners
     */
    protected static function shutdown(
        array &$listeners,
        ?DeferredFuture &$pendingOperation,
        DeferredFuture $onClose,
        ?\Throwable $exception = null,
    ): void {
        if (!empty($listeners)) {
            $exception ??= new ConnectionException("The connection was closed");
            foreach ($listeners as $listener) {
                $listener->error($exception);
            }
            $listeners = [];
        }

        $pendingOperation?->error($exception ?? new ConnectionException("The connection was closed"));
        $pendingOperation = null;

        if (!$onClose->isComplete()) {
            $onClose->complete();
        }
    }
}
