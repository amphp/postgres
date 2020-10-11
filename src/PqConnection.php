<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\Failure;
use Amp\Loop;
use Amp\NullCancellationToken;
use Amp\Sql\ConnectionException;
use pq;
use function Amp\await;

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
            return new Failure(new ConnectionException("Could not connect to PostgreSQL server", 0, $exception));
        }

        $connection->nonblocking = true;
        $connection->unbuffered = true;

        $deferred = new Deferred;

        $callback = function () use ($connection, $deferred): void {
            if ($deferred->isResolved()) {
                return;
            }

            switch ($connection->poll()) {
                case pq\Connection::POLLING_READING: // Connection not ready, poll again.
                case pq\Connection::POLLING_WRITING: // Still writing...
                    return;

                case pq\Connection::POLLING_FAILED:
                    $deferred->fail(new ConnectionException($connection->errorMessage));
                    return;

                case pq\Connection::POLLING_OK:
                    $deferred->resolve(new self($connection));
                    return;
            }
        };

        $poll = Loop::onReadable($connection->socket, $callback);
        $await = Loop::onWritable($connection->socket, $callback);

        $promise = $deferred->promise();

        $token = $token ?? new NullCancellationToken;
        $id = $token->subscribe([$deferred, "fail"]);

        try {
            return await($promise);
        } finally {
            $token->unsubscribe($id);
            Loop::cancel($poll);
            Loop::cancel($await);
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
