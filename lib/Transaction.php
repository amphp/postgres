<?php

namespace Amp\Postgres;

use Amp\Promise;

class Transaction implements Handle, Operation {
    const UNCOMMITTED  = 0;
    const COMMITTED    = 1;
    const REPEATABLE   = 2;
    const SERIALIZABLE = 4;

    /** @var \Amp\Postgres\Handle|null */
    private $handle;

    /** @var int */
    private $isolation;

    /** @var \Amp\Postgres\Internal\ReferenceQueue */
    private $queue;

    /**
     * @param \Amp\Postgres\Handle $handle
     * @param int $isolation
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(Handle $handle, int $isolation = self::COMMITTED) {
        switch ($isolation) {
            case self::UNCOMMITTED:
            case self::COMMITTED:
            case self::REPEATABLE:
            case self::SERIALIZABLE:
                $this->isolation = $isolation;
                break;

            default:
                throw new \Error("Isolation must be a valid transaction isolation level");
        }

        $this->handle = $handle;
        $this->queue = new Internal\ReferenceQueue;
    }

    public function __destruct() {
        if ($this->handle) {
            $this->rollback(); // Invokes $this->queue->complete().
        }
    }

    /**
     * {@inheritdoc}
     */
    public function onDestruct(callable $onComplete) {
        $this->queue->onDestruct($onComplete);
    }

    /**
     * {@inheritdoc}
     */
    public function isAlive(): bool {
        return $this->handle !== null && $this->handle->isAlive();
    }

    /**
     * @return bool True if the transaction is active, false if it has been committed or rolled back.
     */
    public function isActive(): bool {
        return $this->handle !== null;
    }

    /**
     * @return int
     */
    public function getIsolationLevel(): int {
        return $this->isolation;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function query(string $sql): Promise {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $this->queue->reference();

        $promise = $this->handle->query($sql);

        $promise->onResolve(function ($exception, $result) {
            if ($result instanceof Operation) {
                $result->onDestruct([$this->queue, "unreference"]);
                return;
            }

            $this->queue->unreference();
        });

        return $promise;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function prepare(string $sql): Promise {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $this->queue->reference();

        $promise = $this->handle->prepare($sql);

        $promise->onResolve(function ($exception, $statement) {
            if ($statement instanceof Statement) {
                $statement->onDestruct([$this->queue, "unreference"]);
                return;
            }

            $this->queue->unreference();
        });

        return $promise;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, array $params = []): Promise {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $this->queue->reference();

        $promise = $this->handle->execute($sql, $params);

        $promise->onResolve(function ($exception, $result) {
            if ($result instanceof Operation) {
                $result->onDestruct([$this->queue, "unreference"]);
                return;
            }

            $this->queue->unreference();
        });

        return $promise;
    }


    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function notify(string $channel, string $payload = ""): Promise {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->handle->notify($channel, $payload);
    }

    /**
     * Commits the transaction and makes it inactive.
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function commit(): Promise {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->handle->query("COMMIT");
        $this->handle = null;
        $promise->onResolve([$this->queue, "unreference"]);

        return $promise;
    }

    /**
     * Rolls back the transaction and makes it inactive.
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function rollback(): Promise {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->handle->query("ROLLBACK");
        $this->handle = null;
        $promise->onResolve([$this->queue, "unreference"]);

        return $promise;
    }

    /**
     * Creates a savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function savepoint(string $identifier): Promise {
        return $this->query("SAVEPOINT " . $this->quoteName($identifier));
    }

    /**
     * Rolls back to the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function rollbackTo(string $identifier): Promise {
        return $this->query("ROLLBACK TO " . $this->quoteName($identifier));
    }

    /**
     * Releases the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function release(string $identifier): Promise {
        return $this->query("RELEASE SAVEPOINT " . $this->quoteName($identifier));
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function quoteString(string $data): string {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->handle->quoteString($data);
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function quoteName(string $name): string {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->handle->quoteName($name);
    }
}
