<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\Failure;
use Amp\Loop;
use Amp\NullCancellationToken;
use Amp\Promise;
use pq;

class PqConnection extends AbstractConnection {
    /**
     * @param string $connectionString
     * @param \Amp\CancellationToken $token
     *
     * @return \Amp\Promise<\Amp\Postgres\PgSqlConnection>
     */
    public static function connect(string $connectionString, CancellationToken $token = null): Promise {
        try {
            $connection = new pq\Connection($connectionString, pq\Connection::ASYNC);
        } catch (pq\Exception $exception) {
            return new Failure(new FailureException("Could not connect to PostgresSQL server", 0, $exception));
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
                    $deferred->fail(new FailureException($connection->errorMessage));
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
    public function __construct(pq\Connection $handle) {
        parent::__construct(new PqHandle($handle));
    }
}
