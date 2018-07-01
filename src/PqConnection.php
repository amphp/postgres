<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\Failure;
use Amp\Loop;
use Amp\NullCancellationToken;
use Amp\Promise;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\ConnectionException;
use pq;

final class PqConnection extends Connection implements Link
{
    /**
     * @param ConnectionConfig $connectionConfig
     * @param CancellationToken $token
     *
     * @return Promise<PqConnection>
     */
    public static function connect(ConnectionConfig $connectionConfig, CancellationToken $token = null): Promise
    {
        $connectionString = \str_replace(";", " ", $connectionConfig->connectionString());

        try {
            $connection = new pq\Connection($connectionString, pq\Connection::ASYNC);
        } catch (pq\Exception $exception) {
            return new Failure(new ConnectionException("Could not connect to PostgreSQL server", 0, $exception));
        }

        $connection->nonblocking = true;
        $connection->unbuffered = true;

        $deferred = new Deferred;

        $callback = function () use ($connection, $deferred) {
            switch ($connection->poll()) {
                case pq\Connection::POLLING_READING:
                    return; // Connection not ready, poll again.

                case pq\Connection::POLLING_WRITING:
                    return; // Still writing...

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

        $token = $token ?? new NullCancellationToken();
        $id = $token->subscribe([$deferred, "fail"]);

        $promise->onResolve(function () use ($poll, $await, $id, $token) {
            $token->unsubscribe($id);
            Loop::cancel($poll);
            Loop::cancel($await);
        });

        return $promise;
    }

    /**
     * @param \pq\Connection $handle
     */
    public function __construct(pq\Connection $handle)
    {
        parent::__construct(new PqHandle($handle));
    }
}
