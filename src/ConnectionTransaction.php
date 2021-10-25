<?php

namespace Amp\Postgres;

use Amp\Sql\Common\PooledResult;
use Amp\Sql\Common\PooledStatement;
use Amp\Sql\FailureException;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\TransactionError;
use Revolt\EventLoop;

final class ConnectionTransaction implements Transaction
{
    private ?Handle $handle;

    private int $isolation;

    /** @var callable */
    private $release;

    private int $refCount = 1;

    /**
     * @param Handle $handle
     * @param callable $release
     * @param int $isolation
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(Handle $handle, callable $release, int $isolation = Transaction::ISOLATION_COMMITTED)
    {
        switch ($isolation) {
            case Transaction::ISOLATION_UNCOMMITTED:
            case Transaction::ISOLATION_COMMITTED:
            case Transaction::ISOLATION_REPEATABLE:
            case Transaction::ISOLATION_SERIALIZABLE:
                $this->isolation = $isolation;
                break;

            default:
                throw new \Error("Isolation must be a valid transaction isolation level");
        }

        $this->handle = $handle;

        $refCount =& $this->refCount;
        $this->release = static function () use (&$refCount, $release) {
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
                    $this->handle->isAlive() && $handle->query('ROLLBACK');
                } catch (FailureException) {
                    // Ignore failure if connection closes during query.
                }
            });
        }
    }

    /**
     * @inheritDoc
     */
    public function getLastUsedAt(): int
    {
        return $this->handle->getLastUsedAt();
    }

    /**
     * @inheritDoc
     *
     * Closes and commits all changes in the transaction.
     */
    public function close(): void
    {
        if ($this->handle) {
            $this->commit(); // Invokes $this->release callback.
        }
    }

    /**
     * @inheritDoc
     */
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

    /**
     * @return int
     */
    public function getIsolationLevel(): int
    {
        return $this->isolation;
    }

    /**
     * @inheritDoc
     *
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
     * @inheritDoc
     *
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
     * @inheritDoc
     *
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
     * @inheritDoc
     *
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
     * @inheritDoc
     *
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
     * @inheritDoc
     *
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
