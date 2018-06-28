<?php

namespace Amp\Postgres;

use Amp\CallableMaker;
use Amp\Deferred;
use Amp\Failure;
use Amp\Loop;
use Amp\Promise;
use Amp\Sql\ConnectionException;
use pq;

final class PqConnection extends Connection {
    use CallableMaker;

    /**
     * @param string $connectionString
     * @param \Amp\CancellationToken $token
     *
     * @return \Amp\Promise<\Amp\Postgres\PgSqlConnection>
     */
    public function connect(): Promise {
        $connectionString = \str_replace(";", " ", $this->config->connectionString());

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
                    $this->handle = new PqHandle($connection);
                    $deferred->resolve();
                    return;
            }
        };

        $poll = Loop::onReadable($connection->socket, $callback);
        $await = Loop::onWritable($connection->socket, $callback);

        $promise = $deferred->promise();

        $id = $this->token->subscribe([$deferred, "fail"]);

        $promise->onResolve(function () use ($poll, $await, $id) {
            $this->token->unsubscribe($id);
            Loop::cancel($poll);
            Loop::cancel($await);
        });

        return $promise;
    }
}
