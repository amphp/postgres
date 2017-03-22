<?php

namespace Amp\Postgres;

use Amp\{ Deferred, Failure, Loop, Promise };

class PgSqlConnection extends AbstractConnection {
    /**
     * @param string $connectionString
     * @param int $timeout
     *
     * @return \Amp\Promise<\Amp\Postgres\PgSqlConnection>
     */
    public static function connect(string $connectionString, int $timeout = 0): Promise {
        if (!$connection = @\pg_connect($connectionString, \PGSQL_CONNECT_ASYNC | \PGSQL_CONNECT_FORCE_NEW)) {
            return new Failure(new FailureException("Failed to create connection resource"));
        }
    
        if (\pg_connection_status($connection) === \PGSQL_CONNECTION_BAD) {
            return new Failure(new FailureException(\pg_last_error($connection)));
        }
    
        if (!$socket = \pg_socket($connection)) {
            return new Failure(new FailureException("Failed to access connection socket"));
        }
    
        $deferred = new Deferred;
    
        $callback = function ($watcher, $resource) use ($connection, $deferred) {
            switch (\pg_connect_poll($connection)) {
                case \PGSQL_POLLING_READING:
                    return; // Connection not ready, poll again.

                case \PGSQL_POLLING_WRITING:
                    return; // Still writing...

                case \PGSQL_POLLING_FAILED:
                    $deferred->fail(new FailureException("Could not connect to PostgreSQL server"));
                    return;

                case \PGSQL_POLLING_OK:
                    $deferred->resolve(new self($connection, $resource));
                    return;
            }
        };
    
        $poll = Loop::onReadable($socket, $callback);
        $await = Loop::onWritable($socket, $callback);

        $promise = $deferred->promise();

        if ($timeout !== 0) {
            $promise = Promise\timeout($promise, $timeout);
        }

        $promise->onResolve(function ($exception) use ($connection, $poll, $await) {
            if ($exception) {
                \pg_close($connection);
            }

            Loop::cancel($poll);
            Loop::cancel($await);
        });

        return $promise;
    }
    
    /**
     * @param resource $handle PostgreSQL connection handle.
     * @param resource $socket PostgreSQL connection stream socket.
     */
    public function __construct($handle, $socket) {
        parent::__construct(new PgSqlExecutor($handle, $socket));
    }
}
