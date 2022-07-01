<?php

namespace Amp\Postgres;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Sql\ConnectionException;
use Amp\Sql\Link;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\TransactionIsolation;
use Amp\Sql\TransactionIsolationLevel;

abstract class Connection implements Link, Receiver, Quoter
{
    private readonly Handle $handle;

    /** @var DeferredFuture|null Used to only allow one transaction at a time. */
    private ?DeferredFuture $busy = null;

    /**
     * @throws ConnectionException
     */
    abstract public static function connect(PostgresConfig $connectionConfig, ?Cancellation $cancellation = null): self;

    protected function __construct(Handle $handle)
    {
        $this->handle = $handle;
    }

    final public function getLastUsedAt(): int
    {
        return $this->handle->getLastUsedAt();
    }

    final public function close(): void
    {
        $this->handle->close();
    }

    final public function isClosed(): bool
    {
        return $this->handle->isClosed();
    }

    final public function onClose(\Closure $onClose): void
    {
        $this->handle->onClose($onClose);
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

        $this->busy->complete();
        $this->busy = null;
    }

    final public function query(string $sql): Result
    {
        $this->awaitPending();
        return $this->handle->query($sql);
    }

    final public function execute(string $sql, array $params = []): Result
    {
        $this->awaitPending();
        return $this->handle->execute($sql, $params);
    }

    final public function prepare(string $sql): Statement
    {
        $this->awaitPending();
        return $this->handle->prepare($sql);
    }

    final public function notify(string $channel, string $payload = ""): Result
    {
        $this->awaitPending();
        return $this->handle->notify($channel, $payload);
    }

    final public function listen(string $channel): Listener
    {
        $this->awaitPending();
        return $this->handle->listen($channel);
    }

    final public function beginTransaction(
        TransactionIsolation $isolation = TransactionIsolationLevel::Committed
    ): Transaction {
        $this->reserve();

        try {
            $this->handle->query("BEGIN TRANSACTION ISOLATION LEVEL " . $isolation->toSql());
        } catch (\Throwable $exception) {
            $this->release();
            throw $exception;
        }

        return new ConnectionTransaction($this->handle, $this->release(...), $isolation);
    }

    final public function quoteString(string $data): string
    {
        return $this->handle->quoteString($data);
    }

    final public function quoteName(string $name): string
    {
        return $this->handle->quoteName($name);
    }
}
