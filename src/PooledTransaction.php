<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\TransactionError;

final class PooledTransaction implements Transaction
{
    /** @var Transaction|null */
    private $transaction;

    /** @var callable|null */
    private $release;

    /**
     * PooledTransaction constructor.
     *
     * @param Transaction $transaction
     * @param callable    $release
     */
    public function __construct(Transaction $transaction, callable $release)
    {
        $this->transaction = $transaction;
        $this->release = $release;

        if (!$this->transaction->isActive()) {
            ($this->release)();
            $this->transaction = null;
            $this->release = null;
        }
    }

    public function __destruct()
    {
        if ($this->transaction && $this->transaction->isActive()) {
            $this->close(); // Invokes $this->release callback.
        }
    }

    public function query(string $sql): Promise
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->query($sql);
    }

    public function prepare(string $sql): Promise
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->prepare($sql);
    }

    public function execute(string $sql, array $params = []): Promise
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->execute($sql, $params);
    }

    public function notify(string $channel, string $payload = ""): Promise
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->notify($channel, $payload);
    }

    public function listen(string $channel): Promise
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->listen($channel);
    }

    public function isAlive(): bool
    {
        return $this->transaction && $this->transaction->isAlive();
    }

    public function lastUsedAt(): int
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->lastUsedAt();
    }

    public function close()
    {
        if (!$this->transaction) {
            return;
        }

        $promise = $this->transaction->commit();
        $promise->onResolve($this->release);

        $this->transaction = null;
    }

    public function quoteString(string $data): string
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->quoteString($data);
    }

    public function quoteName(string $name): string
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->quoteName($name);
    }

    public function getIsolationLevel(): int
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->getIsolationLevel();
    }

    public function isActive(): bool
    {
        return $this->transaction && $this->transaction->isActive();
    }

    public function commit(): Promise
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->transaction->commit();
        $promise->onResolve($this->release);

        $this->transaction = null;

        return $promise;
    }

    public function rollback(): Promise
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->transaction->rollback();
        $promise->onResolve($this->release);

        $this->transaction = null;

        return $promise;
    }

    public function createSavepoint(string $identifier): Promise
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->createSavepoint($identifier);
    }

    public function rollbackTo(string $identifier): Promise
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->rollbackTo($identifier);
    }

    public function releaseSavepoint(string $identifier): Promise
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->releaseSavepoint($identifier);
    }
}
