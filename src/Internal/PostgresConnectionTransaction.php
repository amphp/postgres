<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\DeferredFuture;
use Amp\Postgres\PostgresResult;
use Amp\Postgres\PostgresStatement;
use Amp\Postgres\PostgresTransaction;
use Amp\Sql\SqlException;
use Amp\Sql\TransactionError;
use Amp\Sql\TransactionIsolation;
use Revolt\EventLoop;

/** @internal  */
final class PostgresConnectionTransaction implements PostgresTransaction
{
    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    private readonly DeferredFuture $onClose;

    private ?DeferredFuture $busy = null;

    /** @var array<int, PostgresStatement> Reference statements so de-allocation occurs after commit/rollback. */
    private array $statements = [];

    /**
     * @param \Closure():void $release
     */
    public function __construct(
        private readonly PostgresHandle $handle,
        \Closure $release,
        private readonly TransactionIsolation $isolation,
    ) {
        $busy = &$this->busy;
        $refCount = &$this->refCount;
        $statements = &$this->statements;
        $this->release = static function () use (&$busy, &$refCount, &$statements, $release): void {
            $busy?->complete();
            $busy = null;

            if (--$refCount === 0) {
                $release();
                $statements = [];
            }
        };

        $this->onClose = new DeferredFuture();
        $this->onClose($this->release);
    }

    public function __destruct()
    {
        if ($this->onClose->isComplete()) {
            return;
        }

        $this->onClose->complete();

        if ($this->handle->isClosed()) {
            return;
        }

        $handle = $this->handle;
        EventLoop::queue(static function () use ($handle): void {
            try {
                !$handle->isClosed() && $handle->query('ROLLBACK');
            } catch (SqlException) {
                // Ignore failure if connection closes during query.
            }
        });
    }

    public function getLastUsedAt(): int
    {
        return $this->handle->getLastUsedAt();
    }

    public function isNestedTransaction(): bool
    {
        return false;
    }

    private function assertOpen(): void
    {
        if ($this->isClosed()) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }
    }

    /**
     * Rolls back all changes in the transaction if it has not been committed.
     */
    public function close(): void
    {
        if (!$this->isClosed()) {
            $this->rollback(); // Invokes $this->release callback.
        }
    }

    public function isClosed(): bool
    {
        return $this->onClose->isComplete();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    /**
     * @return bool True if the transaction is active, false if it has been committed or rolled back.
     */
    public function isActive(): bool
    {
        return !$this->isClosed();
    }

    public function getIsolationLevel(): TransactionIsolation
    {
        return $this->isolation;
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function query(string $sql): PostgresResult
    {
        $this->awaitPendingNestedTransaction();

        ++$this->refCount;
        try {
            $result = $this->handle->query($sql);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        return new PostgresPooledResult($result, $this->release);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function prepare(string $sql): PostgresStatement
    {
        $this->awaitPendingNestedTransaction();

        ++$this->refCount;
        try {
            $statement = $this->handle->prepare($sql);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        $this->statements[\spl_object_id($statement)] ??= $statement;

        return new PostgresPooledStatement($statement, $this->release);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, array $params = []): PostgresResult
    {
        $this->awaitPendingNestedTransaction();

        ++$this->refCount;
        try {
            $result = $this->handle->execute($sql, $params);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        return new PostgresPooledResult($result, $this->release);
    }

    public function beginTransaction(): PostgresTransaction
    {
        $this->awaitPendingNestedTransaction();

        ++$this->refCount;
        $this->busy = new DeferredFuture();
        try {
            $identifier = \bin2hex(\random_bytes(8));
            $this->handle->createSavepoint($identifier);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        return new PostgresNestedTransaction($this, $this->handle, $identifier, $this->release);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function notify(string $channel, string $payload = ""): PostgresResult
    {
        $this->awaitPendingNestedTransaction();
        return $this->handle->notify($channel, $payload);
    }

    /**
     * Commits the transaction and makes it inactive.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function commit(): void
    {
        $this->awaitPendingNestedTransaction();
        $this->onClose->complete();
        $this->handle->query("COMMIT");
    }

    /**
     * Rolls back the transaction and makes it inactive.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollback(): void
    {
        $this->awaitPendingNestedTransaction();
        $this->onClose->complete();
        $this->handle->query("ROLLBACK");
    }

    public function onCommit(\Closure $onCommit): void
    {
        // TODO: Implement onCommit() method.
    }

    public function onRollback(\Closure $onRollback): void
    {
        // TODO: Implement onRollback() method.
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function quoteString(string $data): string
    {
        $this->assertOpen();
        return $this->handle->quoteString($data);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function quoteName(string $name): string
    {
        $this->assertOpen();
        return $this->handle->quoteName($name);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function escapeByteA(string $data): string
    {
        $this->assertOpen();
        return $this->handle->escapeByteA($data);
    }

    private function awaitPendingNestedTransaction(): void
    {
        while ($this->busy) {
            $this->busy->getFuture()->await();
        }

        $this->assertOpen();
    }
}
