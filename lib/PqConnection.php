<?php

namespace Amp\Postgres;

use Amp\{ Deferred, Failure, Loop, Promise };
use pq;

class PqConnection extends AbstractConnection {
    /**
     * @param string $connectionString
     * @param int $timeout
     *
     * @return \Amp\Promise<\Amp\Postgres\PgSqlConnection>
     */
    public static function connect(string $connectionString, int $timeout = 0): Promise {
        try {
            $connection = new pq\Connection($connectionString, pq\Connection::ASYNC);
        } catch (pq\Exception $exception) {
            return new Failure(new FailureException("Could not connect to PostgresSQL server", 0, $exception));
        }
        $connection->resetAsync();
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
                    $deferred->fail(new FailureException("Could not connect to PostgreSQL server"));
                    return;

                case pq\Connection::POLLING_OK:
                case \PGSQL_POLLING_OK:
                    $deferred->resolve(new self($connection));
                    return;
            }
        };
    
        $poll = Loop::onReadable($connection->socket, $callback);
        $await = Loop::onWritable($connection->socket, $callback);

        $promise = $deferred->promise();

        if ($timeout !== 0) {
            $promise = Promise\timeout($promise, $timeout);
        }

        $promise->onResolve(function () use ($poll, $await) {
            Loop::cancel($poll);
            Loop::cancel($await);
        });

        return $promise;
    }
    
    /**
     * @param \pq\Connection $handle
     */
    public function __construct(pq\Connection $handle) {
        parent::__construct(new PqExecutor($handle));
    }
}
