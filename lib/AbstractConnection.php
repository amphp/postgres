<?php

namespace Amp\Postgres;

use Amp\{ CallableMaker, Coroutine, Deferred, Promise, function call };

abstract class AbstractConnection implements Connection {
    use CallableMaker;

    /** @var \Amp\Postgres\Executor */
    private $executor;

    /** @var \Amp\Deferred|null Used to only allow one transaction at a time. */
    private $busy;

    /** @var callable */
    private $release;

    /**
     * @param string $connectionString
     * @param int $timeout Timeout until the connection attempt fails. 0 for no timeout.
     *
     * @return \Amp\Promise<\Amp\Postgres\Connection>
     */
    abstract public static function connect(string $connectionString, int $timeout = 0): Promise;

    /**
     * @param $executor;
     */
    public function __construct(Executor $executor) {
        $this->executor = $executor;
        $this->release = $this->callableFromInstanceMethod("release");
    }

    /**
     * @param string $methodName Method to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return \Amp\Promise
     *
     * @throws \Amp\Postgres\FailureException
     * @throws \Amp\Postgres\PendingOperationError
     */
    private function send(string $methodName, ...$args): Promise {
        if ($this->busy) {
            throw new PendingOperationError;
        }

        $this->busy = true;

        try {
            return $this->executor->{$methodName}(...$args);
        } finally {
            $this->busy = false;
        }
    }

    /**
     * Releases the transaction lock.
     */
    private function release() {
        $this->busy = false;
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql): Promise {
        return $this->send("query", $sql);
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, ...$params): Promise {
        return $this->send("execute", $sql, ...$params);
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Promise {
        return $this->send("prepare", $sql);
    }


    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Promise {
        return $this->send("notify", $channel, $payload);
    }

    /**
     * {@inheritdoc}
     */
    public function listen(string $channel): Promise {
        return $this->send("listen", $channel);
    }

    /**
     * {@inheritdoc}
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Promise {
        if ($this->busy) {
            throw new PendingOperationError;
        }

        $this->busy = true;

        switch ($isolation) {
            case Transaction::UNCOMMITTED:
                $promise = $this->executor->query("BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
                break;

            case Transaction::COMMITTED:
                $promise = $this->executor->query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
                break;

            case Transaction::REPEATABLE:
                $promise = $this->executor->query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                break;

            case Transaction::SERIALIZABLE:
                $promise = $this->executor->query("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                break;

            default:
                throw new \Error("Invalid transaction type");
        }

        return call(function () use ($promise, $isolation) {
            yield $promise;
            $transaction = new Transaction($this->executor, $isolation);
            $transaction->onComplete($this->release);
            return $transaction;
        });
    }
}
