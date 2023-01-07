<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\DeferredFuture;
use Amp\Postgres\PostgresHandle;
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
    private readonly PostgresHandle $handle;

    private readonly TransactionIsolation $isolation;

    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    private readonly DeferredFuture $onClose;

    /**
     * @param \Closure():void $release
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(
        PostgresHandle $handle,
        \Closure $release,
        TransactionIsolation $isolation
    ) {
        $this->handle = $handle;
        $this->isolation = $isolation;

        $refCount =& $this->refCount;
        $this->release = static function () use (&$refCount, $release): void {
            if (--$refCount === 0) {
                $release();
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
        $this->assertOpen();

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
        $this->assertOpen();

        ++$this->refCount;
        try {
            $statement = $this->handle->prepare($sql);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        return new PostgresPooledStatement($statement, $this->release);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, array $params = []): PostgresResult
    {
        $this->assertOpen();

        ++$this->refCount;
        try {
            $result = $this->handle->execute($sql, $params);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        return new PostgresPooledResult($result, $this->release);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function notify(string $channel, string $payload = ""): PostgresResult
    {
        $this->assertOpen();
        return $this->handle->notify($channel, $payload);
    }

    /**
     * Commits the transaction and makes it inactive.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function commit(): void
    {
        $this->assertOpen();
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
        $this->assertOpen();
        $this->onClose->complete();
        $this->handle->query("ROLLBACK");
    }

    /**
     * Creates a savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function createSavepoint(string $identifier): void
    {
        $this->query("SAVEPOINT " . $this->quoteName($identifier));
    }

    /**
     * Rolls back to the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollbackTo(string $identifier): void
    {
        $this->query("ROLLBACK TO " . $this->quoteName($identifier));
    }

    /**
     * Releases the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function releaseSavepoint(string $identifier): void
    {
        $this->query("RELEASE SAVEPOINT " . $this->quoteName($identifier));
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
}
