<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Amp\Coroutine;
use Interop\Async\Awaitable;

class Transaction implements Executor, Operation {
    use Internal\Operation;
    
    const UNCOMMITTED  = 0;
    const COMMITTED    = 1;
    const REPEATABLE   = 2;
    const SERIALIZABLE = 4;

    /** @var \Amp\Postgres\Executor */
    private $executor;

    /** @var int */
    private $isolation;

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
    }
    
    /**
     * @return bool
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
     */
    public function query(string $sql): Awaitable {
        if ($this->executor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->executor->query($sql);
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Awaitable {
        if ($this->executor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->executor->prepare($sql);
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, ...$params): Awaitable {
        if ($this->executor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->executor->execute($sql, ...$params);
    }
    
    
    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Awaitable {
        if ($this->executor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }
        
        return $this->executor->notify($channel, $payload);
    }

    /**
     * Commits the transaction and makes it inactive.
     *
     * @return \Interop\Async\Awaitable<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\TransactionError
     */
    public function commit(): Awaitable {
        return new Coroutine($this->doCommit());
    }
    
    private function doCommit(): \Generator {
        if ($this->executor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $executor = $this->executor;
        $this->executor = null;

        try {
            $result = yield $executor->query("COMMIT");
        } finally {
            $this->complete();
        }

        return $result;
    }

    /**
     * Rolls back the transaction and makes it inactive.
     *
     * @return \Interop\Async\Awaitable<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\TransactionError
     */
    public function rollback(): Awaitable {
        return new Coroutine($this->doRollback());
    }
    
    public function doRollback(): \Generator {
        if ($this->executor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $executor = $this->executor;
        $this->executor = null;

        try {
            $result = yield $executor->query("ROLLBACK");
        } finally {
            $this->complete();
        }

        return $result;
    }

    /**
     * Creates a savepoint with the given identifier. WARNING: Identifier is not sanitized, do not pass untrusted data.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return \Interop\Async\Awaitable<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\TransactionError
     */
    public function savepoint(string $identifier): Awaitable {
        return $this->query("SAVEPOINT " . $identifier);
    }

    /**
     * Rolls back to the savepoint with the given identifier. WARNING: Identifier is not sanitized, do not pass
     * untrusted data.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return \Interop\Async\Awaitable<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\TransactionError
     */
    public function rollbackTo(string $identifier): Awaitable {
        return $this->query("ROLLBACK TO " . $identifier);
    }

    /**
     * Releases the savepoint with the given identifier. WARNING: Identifier is not sanitized, do not pass untrusted
     * data.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return \Interop\Async\Awaitable<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\TransactionError
     */
    public function release(string $identifier): Awaitable {
        return $this->query("RELEASE SAVEPOINT " . $identifier);
    }
}
