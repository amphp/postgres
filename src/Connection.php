<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\Promise;
use Amp\Sql\FailureException;
use Amp\Sql\Link;
use Amp\Sql\Transaction;
use function Amp\call;

abstract class Connection implements Link, Handle
{
    /** @var Handle */
    private $handle;

    /** @var Deferred|null Used to only allow one transaction at a time. */
    private $busy;

    /**
     * @param ConnectionConfig $connectionConfig
     * @param CancellationToken $token
     *
     * @return Promise<Connection>
     */
    abstract public static function connect(ConnectionConfig $connectionConfig, ?CancellationToken $token = null): Promise;

    /**
     * @param Handle $handle
     */
    public function __construct(Handle $handle)
    {
        $this->handle = $handle;
    }


    /**
     * {@inheritdoc}
     */
    final public function isAlive(): bool
    {
        return $this->handle->isAlive();
    }

    /**
     * {@inheritdoc}
     *
     * @throws FailureException
     */
    final public function getLastUsedAt(): int
    {
        if (! $this->handle) {
            throw new FailureException('Not connected');
        }

        return $this->handle->getLastUsedAt();
    }

    /**
     * {@inheritdoc}
     */
    final public function close(): void
    {
        if ($this->handle) {
            $this->handle->close();
        }
    }

    /**
     * @param string $methodName Method to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return Promise
     *
     * @throws FailureException
     */
    private function send(string $methodName, ...$args): Promise
    {
        if (! $this->handle) {
            throw new FailureException('Not connected');
        }

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
    private function release(): void
    {
        \assert($this->busy !== null);

        $deferred = $this->busy;
        $this->busy = null;
        $deferred->resolve();
    }

    /**
     * {@inheritdoc}
     */
    final public function query(string $sql): Promise
    {
        if (! $this->handle) {
            throw new FailureException('Not connected');
        }

        return $this->send("query", $sql);
    }

    /**
     * {@inheritdoc}
     */
    final public function execute(string $sql, array $params = []): Promise
    {
        if (! $this->handle) {
            throw new FailureException('Not connected');
        }

        return $this->send("execute", $sql, $params);
    }

    /**
     * {@inheritdoc}
     *
     * Statement instances returned by this method must also implement Operation.
     */
    final public function prepare(string $sql): Promise
    {
        if (! $this->handle) {
            throw new FailureException('Not connected');
        }

        return $this->send("prepare", $sql);
    }


    /**
     * {@inheritdoc}
     */
    final public function notify(string $channel, string $payload = ""): Promise
    {
        if (! $this->handle) {
            throw new FailureException('Not connected');
        }

        return $this->send("notify", $channel, $payload);
    }

    /**
     * {@inheritdoc}
     *
     * @throws FailureException
     */
    final public function listen(string $channel): Promise
    {
        if (! $this->handle) {
            throw new FailureException('Not connected');
        }

        return $this->send("listen", $channel);
    }

    /**
     * {@inheritdoc}
     *
     * @throws FailureException
     */
    final public function beginTransaction(int $isolation = Transaction::ISOLATION_COMMITTED): Promise
    {
        if (! $this->handle) {
            throw new FailureException('Not connected');
        }

        return call(function () use ($isolation) {
            switch ($isolation) {
                case Transaction::ISOLATION_UNCOMMITTED:
                    yield $this->handle->query("BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
                    break;

                case Transaction::ISOLATION_COMMITTED:
                    yield $this->handle->query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
                    break;

                case Transaction::ISOLATION_REPEATABLE:
                    yield $this->handle->query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                    break;

                case Transaction::ISOLATION_SERIALIZABLE:
                    yield $this->handle->query("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                    break;

                default:
                    throw new \Error("Invalid transaction type");
            }

            $this->busy = new Deferred;

            return new ConnectionTransaction($this->handle, \Closure::fromCallable([$this, 'release']), $isolation);
        });
    }

    /**
     * {@inheritdoc}
     *
     * @throws FailureException
     */
    final public function quoteString(string $data): string
    {
        if (! $this->handle) {
            throw new FailureException('Not connected');
        }

        return $this->handle->quoteString($data);
    }

    /**
     * {@inheritdoc}
     *
     * @throws FailureException
     */
    final public function quoteName(string $name): string
    {
        if (! $this->handle) {
            throw new FailureException('Not connected');
        }

        return $this->handle->quoteName($name);
    }
}
