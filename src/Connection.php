<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\NullCancellationToken;
use Amp\Promise;
use Amp\Sql\ConnectionConfig;
use function Amp\call;

abstract class Connection implements Handle, Link {
    /** @var \Amp\Postgres\Handle */
    protected $handle;

    /** @var \Amp\Deferred|null Used to only allow one transaction at a time. */
    private $busy;

    /** @var callable */
    protected $release;

    /** @var ConnectionConfig */
    protected $config;

    /** @var CancellationToken */
    protected $token;

    /**
     * @param \Amp\Postgres\Handle $handle
     */
    public function __construct(ConnectionConfig $config, CancellationToken $token = null) {
        $this->config = $config;
        $this->token = $token ?? new NullCancellationToken();
    }

    abstract public function connect(): Promise;

    /**
     * {@inheritdoc}
     */
    final public function isAlive(): bool {
        return $this->handle->isAlive();
    }

    /**
     * {@inheritdoc}
     */
    final public function lastUsedAt(): int {
        return $this->handle->lastUsedAt();
    }

    /**
     * {@inheritdoc}
     */
    final public function close() {
        $this->handle->close();
    }

    /**
     * @param string $methodName Method to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return \Amp\Promise
     *
     * @throws \Amp\Sql\FailureException
     */
    private function send(string $methodName, ...$args): Promise {
        if ($this->busy) {
            return call(function () use ($methodName, $args) {
                while ($this->busy) {
                    yield $this->busy->promise();
                }

                return yield ([$this->handle, $methodName])(...$args);
            });
        }

        return ([$this->handle, $methodName])(...$args);
    }

    /**
     * Releases the transaction lock.
     */
    private function release() {
        \assert($this->busy !== null);

        $deferred = $this->busy;
        $this->busy = null;
        $deferred->resolve();
    }

    /**
     * {@inheritdoc}
     */
    final public function query(string $sql): Promise {
        return $this->send("query", $sql);
    }

    /**
     * {@inheritdoc}
     */
    final public function execute(string $sql, array $params = []): Promise {
        return $this->send("execute", $sql, $params);
    }

    /**
     * {@inheritdoc}
     *
     * Statement instances returned by this method must also implement Operation.
     */
    final public function prepare(string $sql): Promise {
        return $this->send("prepare", $sql);
    }


    /**
     * {@inheritdoc}
     */
    final public function notify(string $channel, string $payload = ""): Promise {
        return $this->send("notify", $channel, $payload);
    }

    /**
     * {@inheritdoc}
     */
    final public function listen(string $channel): Promise {
        return $this->send("listen", $channel);
    }

    /**
     * {@inheritdoc}
     */
    final public function transaction(int $isolation = Transaction::COMMITTED): Promise {
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
            $transaction->onDestruct($this->release);
            return $transaction;
        });
    }

    /**
     * {@inheritdoc}
     */
    final public function quoteString(string $data): string {
        return $this->handle->quoteString($data);
    }

    /**
     * {@inheritdoc}
     */
    final public function quoteName(string $name): string {
        return $this->handle->quoteName($name);
    }
}
