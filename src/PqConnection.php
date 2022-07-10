<?php

namespace Amp\Postgres;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Sql\ConnectionException;
use pq;
use Revolt\EventLoop;

final class PqConnection extends PostgresConnection implements PostgresLink
{
    private readonly Internal\PqHandle $handle;

    public static function connect(PostgresConfig $connectionConfig, ?Cancellation $cancellation = null): self
    {
        try {
            $connection = new pq\Connection($connectionConfig->getConnectionString(), pq\Connection::ASYNC);
        } catch (pq\Exception $exception) {
            throw new ConnectionException("Could not connect to PostgreSQL server", 0, $exception);
        }

        $connection->nonblocking = true;
        $connection->unbuffered = true;

        $deferred = new DeferredFuture();
        $callback = function () use (&$poll, &$await, $connection, $deferred): void {
            if (!$deferred->isComplete()) {
                switch ($result = $connection->poll()) {
                    case pq\Connection::POLLING_READING:
                    case pq\Connection::POLLING_WRITING:
                        return; // Connection still reading or writing, so return and leave callback enabled.

                    case pq\Connection::POLLING_FAILED:
                        $deferred->error(new ConnectionException($connection->errorMessage));
                        break;

                    case pq\Connection::POLLING_OK:
                        $deferred->complete(new self($connection));
                        break;

                    default:
                        $deferred->error(new ConnectionException('Unexpected connection status value: ' . $result));
                        break;
                }
            }

            EventLoop::cancel($poll);
            EventLoop::cancel($await);
        };

        $poll = EventLoop::onReadable($connection->socket, $callback);
        $await = EventLoop::onWritable($connection->socket, $callback);

        return $deferred->getFuture()->await($cancellation);
    }

    protected function __construct(pq\Connection $handle)
    {
        $this->handle = new Internal\PqHandle($handle);
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
