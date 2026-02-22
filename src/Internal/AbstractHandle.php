<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\DeferredFuture;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Pipeline\Queue;
use Amp\Postgres\PostgresConfig;
use Amp\Sql\SqlConnectionException;
use Revolt\EventLoop;

/**
 * @internal
 */
abstract class AbstractHandle implements PostgresHandle
{
    use ForbidCloning;
    use ForbidSerialization;

    protected ?DeferredFuture $pendingOperation = null;

    /** @var array<non-empty-string, Queue> */
    protected array $listeners = [];

    protected int $lastUsedAt = 0;

    public function __construct(
        private readonly PostgresConfig $config,
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

    #[\Override]
    public function getConfig(): PostgresConfig
    {
        return $this->config;
    }

    #[\Override]
    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    #[\Override]
    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    #[\Override]
    public function close(): void
    {
        self::shutdown($this->listeners, $this->pendingOperation, $this->onClose);

        EventLoop::cancel($this->poll);
        EventLoop::cancel($this->await);
    }

    /**
     * @param array<non-empty-string, Queue> $listeners
     */
    protected static function shutdown(
        array &$listeners,
        ?DeferredFuture &$pendingOperation,
        DeferredFuture $onClose,
        ?\Throwable $exception = null,
    ): void {
        if (!empty($listeners)) {
            $exception ??= new SqlConnectionException("The connection was closed");
            foreach ($listeners as $listener) {
                $listener->error($exception);
            }
            $listeners = [];
        }

        $pendingOperation?->error($exception ?? new SqlConnectionException("The connection was closed"));
        $pendingOperation = null;

        if (!$onClose->isComplete()) {
            $onClose->complete();
        }
    }

    protected function encodeParam(mixed $value): string|int|float|null
    {
        return encodeParam($this, $value);
    }

    #[\Override]
    public function commit(): void
    {
        $this->query("COMMIT");
    }

    #[\Override]
    public function rollback(): void
    {
        $this->query("ROLLBACK");
    }

    #[\Override]
    public function createSavepoint(string $identifier): void
    {
        $this->query("SAVEPOINT " . $this->quoteIdentifier($identifier));
    }

    #[\Override]
    public function rollbackTo(string $identifier): void
    {
        $this->query("ROLLBACK TO " . $this->quoteIdentifier($identifier));
    }

    #[\Override]
    public function releaseSavepoint(string $identifier): void
    {
        $this->query("RELEASE SAVEPOINT " . $this->quoteIdentifier($identifier));
    }
}
