<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\Promise;
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
     */
    final public function getLastUsedAt(): int
    {
        return $this->handle->getLastUsedAt();
    }

    /**
     * {@inheritdoc}
     */
    final public function close(): void
    {
        $this->handle->close();
    }

    /**
     * @param string $methodName Method to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return Promise
     */
    private function send(string $methodName, ...$args): Promise
    {
        if ($this->busy) {
            return call(function () use ($methodName, $args) {
                while ($this->busy) {
                    yield $this->busy->promise();
                }

                return yield $this->handle->{$methodName}(...$args);
            });
        }

        return $this->handle->{$methodName}(...$args);
    }

    /**
     * Reserves the connection for a transaction.
     */
    private function reserve(): void
    {
        \assert($this->busy === null);

        $this->busy = new Deferred;
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
        return $this->send("query", $sql);
    }

    /**
     * {@inheritdoc}
     */
    final public function execute(string $sql, array $params = []): Promise
    {
        return $this->send("execute", $sql, $params);
    }

    /**
     * {@inheritdoc}
     */
    final public function prepare(string $sql): Promise
    {
        return $this->send("prepare", $sql);
    }


    /**
     * {@inheritdoc}
     */
    final public function notify(string $channel, string $payload = ""): Promise
    {
        return $this->send("notify", $channel, $payload);
    }

    /**
     * {@inheritdoc}
     */
    final public function listen(string $channel): Promise
    {
        return $this->send("listen", $channel);
    }

    /**
     * {@inheritdoc}
     */
    final public function beginTransaction(int $isolation = Transaction::ISOLATION_COMMITTED): Promise
    {
        return call(function () use ($isolation) {
            $this->reserve();

            try {
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
            } catch (\Throwable $exception) {
                $this->release();
                throw $exception;
            }

            return new ConnectionTransaction($this->handle, \Closure::fromCallable([$this, 'release']), $isolation);
        });
    }

    /**
     * {@inheritdoc}
     */
    final public function quoteString(string $data): string
    {
        return $this->handle->quoteString($data);
    }

    /**
     * {@inheritdoc}
     */
    final public function quoteName(string $name): string
    {
        return $this->handle->quoteName($name);
    }
}
