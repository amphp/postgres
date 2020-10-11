<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\Sql\Link;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;

abstract class Connection implements Link, Handle
{
    /** @var Handle */
    private Handle $handle;

    /** @var Deferred|null Used to only allow one transaction at a time. */
    private ?Deferred $busy = null;

    /**
     * @param ConnectionConfig $connectionConfig
     * @param CancellationToken|null $token
     *
     * @return self
     */
    abstract public static function connect(ConnectionConfig $connectionConfig, ?CancellationToken $token = null): self;

    /**
     * @param Handle $handle
     */
    public function __construct(Handle $handle)
    {
        $this->handle = $handle;
    }


    /**
     * @inheritDoc
     */
    final public function isAlive(): bool
    {
        return $this->handle->isAlive();
    }

    /**
     * @inheritDoc
     */
    final public function getLastUsedAt(): int
    {
        return $this->handle->getLastUsedAt();
    }

    /**
     * @inheritDoc
     */
    final public function close(): void
    {
        $this->handle->close();
    }

    /**
     * @param string $methodName Method to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return Result|Statement|Listener
     */
    private function send(string $methodName, ...$args): Result|Statement|Listener
    {
        while ($this->busy) {
            await($this->busy->promise());
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
     * @inheritDoc
     */
    final public function query(string $sql): Result
    {
        while ($this->busy) {
            await($this->busy->promise());
        }

        return $this->handle->query($sql);
    }

    /**
     * @inheritDoc
     */
    final public function execute(string $sql, array $params = []): Result
    {
        while ($this->busy) {
            await($this->busy->promise());
        }

        return $this->handle->execute($sql, $params);
    }

    /**
     * @inheritDoc
     */
    final public function prepare(string $sql): Statement
    {
        while ($this->busy) {
            await($this->busy->promise());
        }

        return $this->handle->prepare($sql);
    }


    /**
     * @inheritDoc
     */
    final public function notify(string $channel, string $payload = ""): Result
    {
        while ($this->busy) {
            await($this->busy->promise());
        }

        return $this->handle->notify($channel, $payload);
    }

    /**
     * @inheritDoc
     */
    final public function listen(string $channel): Listener
    {
        while ($this->busy) {
            await($this->busy->promise());
        }

        return $this->handle->listen($channel);
    }

    /**
     * @inheritDoc
     */
    final public function beginTransaction(int $isolation = Transaction::ISOLATION_COMMITTED): Transaction
    {
        $this->reserve();

        try {
            switch ($isolation) {
                case Transaction::ISOLATION_UNCOMMITTED:
                    $this->handle->query("BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
                    break;

                case Transaction::ISOLATION_COMMITTED:
                    $this->handle->query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
                    break;

                case Transaction::ISOLATION_REPEATABLE:
                    $this->handle->query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                    break;

                case Transaction::ISOLATION_SERIALIZABLE:
                    $this->handle->query("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                    break;

                default:
                    throw new \Error("Invalid transaction type");
            }
        } catch (\Throwable $exception) {
            $this->release();
            throw $exception;
        }

        return new ConnectionTransaction($this->handle, \Closure::fromCallable([$this, 'release']), $isolation);
    }

    /**
     * @inheritDoc
     */
    final public function quoteString(string $data): string
    {
        return $this->handle->quoteString($data);
    }

    /**
     * @inheritDoc
     */
    final public function quoteName(string $name): string
    {
        return $this->handle->quoteName($name);
    }
}
