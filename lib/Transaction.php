<?php

namespace Amp\Postgres;

use Amp\CallableMaker;
use Amp\Promise;

class Transaction implements Handle, Operation {
    use Internal\Operation, CallableMaker;

    const UNCOMMITTED  = 0;
    const COMMITTED    = 1;
    const REPEATABLE   = 2;
    const SERIALIZABLE = 4;

    /** @var \Amp\Postgres\Handle */
    private $handle;

    /** @var int */
    private $isolation;

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
    }

    public function __destruct() {
        if ($this->handle) {
            $this->rollback(); // Invokes $this->complete().
        }
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

        return $this->handle->query($sql);
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

        return $this->handle->prepare($sql);
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, ...$params): Promise {
        if ($this->handle === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->handle->execute($sql, ...$params);
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
        $promise->onResolve($this->callableFromInstanceMethod("complete"));

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
        $promise->onResolve($this->callableFromInstanceMethod("complete"));

        return $promise;
    }

    /**
     * Creates a savepoint with the given identifier. WARNING: Identifier is not sanitized, do not pass untrusted data.
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
     * Releases the savepoint with the given identifier. WARNING: Identifier is not sanitized, do not pass untrusted
     * data.
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
