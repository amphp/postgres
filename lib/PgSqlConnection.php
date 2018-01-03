<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\Failure;
use Amp\Loop;
use Amp\NullCancellationToken;
use Amp\Promise;

class PgSqlConnection extends Connection {
    /**
     * @param string $connectionString
     * @param \Amp\CancellationToken $token
     *
     * @return \Amp\Promise<\Amp\Postgres\PgSqlConnection>
     */
    public static function connect(string $connectionString, CancellationToken $token = null): Promise {
        $connectionString = \str_replace(";", " ", $connectionString);

        if (!$connection = @\pg_connect($connectionString, \PGSQL_CONNECT_ASYNC | \PGSQL_CONNECT_FORCE_NEW)) {
            return new Failure(new ConnectionException("Failed to create connection resource"));
        }

        if (\pg_connection_status($connection) === \PGSQL_CONNECTION_BAD) {
            return new Failure(new ConnectionException(\pg_last_error($connection)));
        }

        if (!$socket = \pg_socket($connection)) {
            return new Failure(new ConnectionException("Failed to access connection socket"));
        }

        $deferred = new Deferred;

        $callback = function ($watcher, $resource) use ($connection, $deferred) {
            switch (\pg_connect_poll($connection)) {
                case \PGSQL_POLLING_READING:
                    return; // Connection not ready, poll again.

                case \PGSQL_POLLING_WRITING:
                    return; // Still writing...

                case \PGSQL_POLLING_FAILED:
                    $deferred->fail(new ConnectionException(\pg_last_error($connection)));
                    return;

                case \PGSQL_POLLING_OK:
                    $deferred->resolve(new self($connection, $resource));
                    return;
            }
        };

        $poll = Loop::onReadable($socket, $callback);
        $await = Loop::onWritable($socket, $callback);

        $promise = $deferred->promise();

        $token = $token ?? new NullCancellationToken;
        $id = $token->subscribe([$deferred, "fail"]);

        $promise->onResolve(function ($exception) use ($connection, $poll, $await, $id, $token) {
            if ($exception) {
                \pg_close($connection);
            }

            $token->unsubscribe($id);
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
        parent::__construct(new PgSqlHandle($handle, $socket));
    }
}
