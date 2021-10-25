<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\NullCancellationToken;
use Amp\Sql\ConnectionException;
use pq;
use Revolt\EventLoop;

final class PqConnection extends Connection implements Link
{
    private PqHandle $handle;

    /**
     * @param ConnectionConfig $connectionConfig
     * @param CancellationToken $token
     *
     * @return PqConnection
     */
    public static function connect(ConnectionConfig $connectionConfig, ?CancellationToken $token = null): self
    {
        try {
            $connection = new pq\Connection($connectionConfig->getConnectionString(), pq\Connection::ASYNC);
        } catch (pq\Exception $exception) {
            throw new ConnectionException("Could not connect to PostgreSQL server", 0, $exception);
        }

        $connection->nonblocking = true;
        $connection->unbuffered = true;

        $deferred = new Deferred;

        $callback = function () use ($connection, $deferred): void {
            if ($deferred->isComplete()) {
                return;
            }

            switch ($connection->poll()) {
                case pq\Connection::POLLING_READING: // Connection not ready, poll again.
                case pq\Connection::POLLING_WRITING: // Still writing...
                    return;

                case pq\Connection::POLLING_FAILED:
                    $deferred->error(new ConnectionException($connection->errorMessage));
                    return;

                case pq\Connection::POLLING_OK:
                    $deferred->complete(new self($connection));
                    return;
            }
        };

        $poll = EventLoop::onReadable($connection->socket, $callback);
        $await = EventLoop::onWritable($connection->socket, $callback);

        $future = $deferred->getFuture();

        $token = $token ?? new NullCancellationToken;
        $id = $token->subscribe([$deferred, "error"]);

        try {
            return $future->await();
        } finally {
            $token->unsubscribe($id);
            EventLoop::cancel($poll);
            EventLoop::cancel($await);
        }
    }

    /**
     * @param pq\Connection $handle
     */
    public function __construct(pq\Connection $handle)
    {
        $this->handle = new PqHandle($handle);
        parent::__construct($this->handle);
    }

    /**
     * @return bool True if result sets are buffered in memory, false if unbuffered.
     */
    public function isBufferingResults(): bool
    {
        return $this->handle->isBufferingResults();
    }

    /**
     * Sets result sets to be fully buffered in local memory.
     */
    public function shouldBufferResults(): void
    {
        $this->handle->shouldBufferResults();
    }

    /**
     * Sets result sets to be streamed from the database server.
     */
    public function shouldNotBufferResults(): void
    {
        $this->handle->shouldNotBufferResults();
    }
}
