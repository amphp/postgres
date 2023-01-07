<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\DeferredFuture;
use Amp\Pipeline\Queue;
use Amp\Postgres\ByteA;
use Amp\Postgres\PostgresHandle;
use Amp\Postgres\PostgresResult;
use Amp\Postgres\PostgresStatement;
use Amp\Sql\ConnectionException;
use Revolt\EventLoop;

/**
 * @internal
 */
abstract class AbstractHandle implements PostgresHandle
{
    protected ?DeferredFuture $pendingOperation = null;

    /** @var array<non-empty-string, Queue> */
    protected array $listeners = [];

    protected int $lastUsedAt = 0;

    abstract public function query(string $sql): PostgresResult;

    abstract public function prepare(string $sql): PostgresStatement;

    abstract public function execute(string $sql, array $params = []): PostgresResult;

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
     * @param array<non-empty-string, Queue> $listeners
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

    protected function escapeParams(array $params): array
    {
        return \array_map(fn (mixed $param) => match (true) {
            $param instanceof ByteA => $this->escapeByteA($param->getData()),
            \is_array($param) => $this->escapeParams($param),
            default => $param,
        }, $params);
    }
}
