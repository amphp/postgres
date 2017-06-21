<?php

namespace Amp\Postgres;

use Amp\CallableMaker;
use Amp\Promise;

class Transaction implements Executor, Operation {
    use Internal\Operation, CallableMaker;

    const UNCOMMITTED  = 0;
    const COMMITTED    = 1;
    const REPEATABLE   = 2;
    const SERIALIZABLE = 4;

    /** @var \Amp\Postgres\Executor */
    private $executor;

    /** @var int */
    private $isolation;

    /** @var callable */
    private $onResolve;

    /**
     * @param \Amp\Postgres\Executor $executor
     * @param int $isolation
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(Executor $executor, int $isolation = self::COMMITTED) {
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

        $this->executor = $executor;
        $this->onResolve = $this->callableFromInstanceMethod("complete");
    }

    /**
     * @return bool True if the transaction is active, false if it has been committed or rolled back.
     */
    public function isActive(): bool {
        return $this->executor !== null;
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
        if ($this->executor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->executor->query($sql);
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function prepare(string $sql): Promise {
        if ($this->executor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->executor->prepare($sql);
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, ...$params): Promise {
        if ($this->executor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->executor->execute($sql, ...$params);
    }


    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function notify(string $channel, string $payload = ""): Promise {
        if ($this->executor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->executor->notify($channel, $payload);
    }

    /**
     * Commits the transaction and makes it inactive.
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function commit(): Promise {
        if ($this->executor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->executor->query("COMMIT");
        $this->executor = null;
        $promise->onResolve($this->onResolve);

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
        if ($this->executor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->executor->query("ROLLBACK");
        $this->executor = null;
        $promise->onResolve($this->onResolve);

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
        return $this->query("SAVEPOINT " . $identifier);
    }

    /**
     * Rolls back to the savepoint with the given identifier. WARNING: Identifier is not sanitized, do not pass
     * untrusted data.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\TransactionError If the transaction has been committed or rolled back.
     */
    public function rollbackTo(string $identifier): Promise {
        return $this->query("ROLLBACK TO " . $identifier);
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
        return $this->query("RELEASE SAVEPOINT " . $identifier);
    }
}
