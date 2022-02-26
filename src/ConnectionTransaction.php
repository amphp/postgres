<?php

namespace Amp\Postgres;

use Amp\Sql\Common\PooledResult;
use Amp\Sql\Common\PooledStatement;
use Amp\Sql\FailureException;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\TransactionError;
use Amp\Sql\TransactionIsolation;
use Revolt\EventLoop;

final class ConnectionTransaction implements Transaction
{
    private ?Handle $handle;

    private readonly TransactionIsolation $isolation;

    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    /**
     * @param \Closure():void $release
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(
        Handle $handle,
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
    }

    public function __destruct()
    {
        if ($this->handle && $this->handle->isAlive()) {
            $handle = $this->handle;
            EventLoop::queue(static function () use ($handle): void {
                try {
                    $handle->isAlive() && $handle->query('ROLLBACK');
                } catch (FailureException) {
                    // Ignore failure if connection closes during query.
                }
            });
        }
    }

    public function getLastUsedAt(): int
    {
        return $this->handle->getLastUsedAt();
    }

    /**
     * Closes and commits all changes in the transaction.
     */
    public function close(): void
    {
        if ($this->handle) {
            $this->rollback(); // Invokes $this->release callback.
        }
    }

    public function isAlive(): bool
    {
        return $this->handle && $this->handle->isAlive();
    }

    /**
     * @return bool True if the transaction is active, false if it has been committed or rolled back.
     */
    public function isActive(): bool
    {
        return $this->handle !== null;
    }

    public function getIsolationLevel(): TransactionIsolation
    {
        return $this->isolation;
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function query(string $sql): Result
    {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        ++$this->refCount;
        try {
            $result = $this->handle->query($sql);
        } finally {
            EventLoop::queue($this->release);
        }

        ++$this->refCount;
        return new PooledResult($result, $this->release);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function prepare(string $sql): Statement
    {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        ++$this->refCount;
        try {
            $statement = $this->handle->prepare($sql);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        return new PooledStatement($statement, $this->release);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, array $params = []): Result
    {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        ++$this->refCount;
        try {
            $result = $this->handle->execute($sql, $params);
        } finally {
            EventLoop::queue($this->release);
        }

        ++$this->refCount;
        return new PooledResult($result, $this->release);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function notify(string $channel, string $payload = ""): Result
    {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->handle->notify($channel, $payload);
    }

    /**
     * Commits the transaction and makes it inactive.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function commit(): void
    {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $handle = $this->handle;
        $this->handle = null;
        $handle->query("COMMIT");
    }

    /**
     * Rolls back the transaction and makes it inactive.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollback(): void
    {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $handle = $this->handle;
        $this->handle = null;
        $handle->query("ROLLBACK");
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
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->handle->quoteString($data);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function quoteName(string $name): string
    {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->handle->quoteName($name);
    }
}
