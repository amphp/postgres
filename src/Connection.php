<?php

namespace Amp\Postgres;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Sql\Link;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\TransactionIsolation;

abstract class Connection implements Link, Handle
{
    /** @var Handle */
    private readonly Handle $handle;

    /** @var DeferredFuture|null Used to only allow one transaction at a time. */
    private ?DeferredFuture $busy = null;

    /**
     * @param ConnectionConfig $connectionConfig
     * @param Cancellation|null $cancellation
     *
     * @return self
     */
    abstract public static function connect(ConnectionConfig $connectionConfig, ?Cancellation $cancellation = null): self;

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
        $this->awaitPending();
        return $this->handle->{$methodName}(...$args);
    }

    private function awaitPending(): void
    {
        while ($this->busy) {
            $this->busy->getFuture()->await();
        }
    }

    /**
     * Reserves the connection for a transaction.
     */
    private function reserve(): void
    {
        \assert($this->busy === null);
        $this->busy = new DeferredFuture;
    }

    /**
     * Releases the transaction lock.
     */
    private function release(): void
    {
        \assert($this->busy !== null);

        $this->busy->complete(null);
        $this->busy = null;
    }

    /**
     * @inheritDoc
     */
    final public function query(string $sql): Result
    {
        $this->awaitPending();
        return $this->handle->query($sql);
    }

    /**
     * @inheritDoc
     */
    final public function execute(string $sql, array $params = []): Result
    {
        $this->awaitPending();
        return $this->handle->execute($sql, $params);
    }

    /**
     * @inheritDoc
     */
    final public function prepare(string $sql): Statement
    {
        $this->awaitPending();
        return $this->handle->prepare($sql);
    }


    /**
     * @inheritDoc
     */
    final public function notify(string $channel, string $payload = ""): Result
    {
        $this->awaitPending();
        return $this->handle->notify($channel, $payload);
    }

    /**
     * @inheritDoc
     */
    final public function listen(string $channel): Listener
    {
        $this->awaitPending();
        return $this->handle->listen($channel);
    }

    /**
     * @inheritDoc
     */
    final public function beginTransaction(
        TransactionIsolation $isolation = TransactionIsolation::COMMITTED
    ): Transaction {
        $this->reserve();

        try {
            $this->handle->query(match ($isolation) {
                TransactionIsolation::UNCOMMITTED => "BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
                TransactionIsolation::COMMITTED => "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED",
                TransactionIsolation::REPEATABLE => "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                TransactionIsolation::SERIALIZABLE => "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            });
        } catch (\Throwable $exception) {
            $this->release();
            throw $exception;
        }

        return new ConnectionTransaction($this->handle, $this->release(...), $isolation);
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
