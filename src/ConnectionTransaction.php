<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\Transaction as SqlTransaction;
use Amp\Sql\TransactionError;
use function Amp\call;

final class ConnectionTransaction implements Transaction
{
    /** @var Handle|null */
    private $handle;

    /** @var int */
    private $isolation;

    /** @var callable */
    private $release;

    /** @var int */
    private $refCount = 1;

    /**
     * @param Handle $handle
     * @param callable $release
     * @param int $isolation
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(Handle $handle, callable $release, int $isolation = SqlTransaction::ISOLATION_COMMITTED)
    {
        switch ($isolation) {
            case SqlTransaction::ISOLATION_UNCOMMITTED:
            case SqlTransaction::ISOLATION_COMMITTED:
            case SqlTransaction::ISOLATION_REPEATABLE:
            case SqlTransaction::ISOLATION_SERIALIZABLE:
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
            $this->rollback(); // Invokes $this->release callback.
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getLastUsedAt(): int
    {
        return $this->handle->getLastUsedAt();
    }

    /**
     * {@inheritdoc}
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
     * {@inheritdoc}
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
     * {@inheritdoc}
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function query(string $sql): Promise
    {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return call(function () use ($sql) {
            ++$this->refCount;
            try {
                $result = yield $this->handle->query($sql);
            } finally {
                ($this->release)();
            }

            if ($result instanceof ResultSet) {
                ++$this->refCount;
                return new PooledResultSet($result, $this->release);
            }

            return $result;
        });
    }

    /**
     * {@inheritdoc}
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function prepare(string $sql): Promise
    {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return call(function () use ($sql) {
            ++$this->refCount;
            try {
                $statement = yield $this->handle->prepare($sql);
            } catch (\Throwable $exception) {
                ($this->release)();
                throw $exception;
            }

            return new PooledStatement($statement, $this->release);
        });
    }

    /**
     * {@inheritdoc}
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, array $params = []): Promise
    {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return call(function () use ($sql, $params) {
            ++$this->refCount;
            try {
                $result = yield $this->handle->execute($sql, $params);
            } finally {
                ($this->release)();
            }

            if ($result instanceof ResultSet) {
                ++$this->refCount;
                return new PooledResultSet($result, $this->release);
            }

            return $result;
        });
    }


    /**
     * {@inheritdoc}
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function notify(string $channel, string $payload = ""): Promise
    {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->handle->notify($channel, $payload);
    }

    /**
     * Commits the transaction and makes it inactive.
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function commit(): Promise
    {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->handle->query("COMMIT");
        $this->handle = null;
        $promise->onResolve($this->release);

        return $promise;
    }

    /**
     * Rolls back the transaction and makes it inactive.
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollback(): Promise
    {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->handle->query("ROLLBACK");
        $this->handle = null;
        $promise->onResolve($this->release);

        return $promise;
    }

    /**
     * Creates a savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function createSavepoint(string $identifier): Promise
    {
        return $this->query("SAVEPOINT " . $this->quoteName($identifier));
    }

    /**
     * Rolls back to the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollbackTo(string $identifier): Promise
    {
        return $this->query("ROLLBACK TO " . $this->quoteName($identifier));
    }

    /**
     * Releases the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function releaseSavepoint(string $identifier): Promise
    {
        return $this->query("RELEASE SAVEPOINT " . $this->quoteName($identifier));
    }

    /**
     * {@inheritdoc}
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
     * {@inheritdoc}
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
