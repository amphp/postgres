<?php

namespace Amp\Postgres;

use Amp\CallableMaker;
use Amp\CancellationToken;
use Amp\Coroutine;
use Amp\Deferred;
use Amp\Promise;
use function Amp\call;

abstract class AbstractConnection implements Connection {
    use CallableMaker;

    /** @var \Amp\Postgres\Executor */
    private $handle;

    /** @var \Amp\Deferred|null Used to only allow one transaction at a time. */
    private $busy;

    /** @var callable */
    private $release;

    /**
     * @param string $connectionString
     * @param \Amp\CancellationToken $token
     *
     * @return \Amp\Promise<\Amp\Postgres\Connection>
     */
    abstract public static function connect(string $connectionString, CancellationToken $token = null): Promise;

    /**
     * @param \Amp\Postgres\Handle $handle
     */
    public function __construct(Handle $handle) {
        $this->handle = $handle;
        $this->release = $this->callableFromInstanceMethod("release");
    }

    /**
     * {@inheritdoc}
     */
    public function isAlive(): bool {
        return $this->handle->isAlive();
    }

    /**
     * @param string $methodName Method to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return \Amp\Promise
     *
     * @throws \Amp\Postgres\FailureException
     */
    private function send(string $methodName, ...$args): \Generator {
        while ($this->busy) {
            yield $this->busy->promise();
        }

        return $this->handle->{$methodName}(...$args);
    }

    /**
     * Releases the transaction lock.
     */
    private function release() {
        $deferred = $this->busy;
        $this->busy = null;
        $deferred->resolve();
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql): Promise {
        return new Coroutine($this->send("query", $sql));
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, ...$params): Promise {
        return new Coroutine($this->send("execute", $sql, ...$params));
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Promise {
        return new Coroutine($this->send("prepare", $sql));
    }


    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Promise {
        return new Coroutine($this->send("notify", $channel, $payload));
    }

    /**
     * {@inheritdoc}
     */
    public function listen(string $channel): Promise {
        return new Coroutine($this->send("listen", $channel));
    }

    /**
     * {@inheritdoc}
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Promise {
        return call(function () use ($isolation) {
            switch ($isolation) {
                case Transaction::UNCOMMITTED:
                    yield $this->handle->query("BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
                    break;

                case Transaction::COMMITTED:
                    yield $this->handle->query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
                    break;

                case Transaction::REPEATABLE:
                    yield $this->handle->query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                    break;

                case Transaction::SERIALIZABLE:
                    yield $this->handle->query("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                    break;

                default:
                    throw new \Error("Invalid transaction type");
            }

            $this->busy = new Deferred;

            $transaction = new Transaction($this->handle, $isolation);
            $transaction->onComplete($this->release);
            return $transaction;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function quoteString(string $data): string {
        return $this->handle->quoteString($data);
    }

    /**
     * {@inheritdoc}
     */
    public function quoteName(string $name): string {
        return $this->handle->quoteName($name);
    }
}
