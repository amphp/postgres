<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\Failure;
use Amp\Loop;
use Amp\NullCancellationToken;
use Amp\Promise;
use Amp\Sql\ConnectionException;

final class PgSqlConnection extends Connection implements Link
{
    /**
     * @param ConnectionConfig $connectionConfig
     * @param CancellationToken $token
     *
     * @return Promise<PgSqlConnection>
     *
     * @throws \Error If pecl-ev is used as a loop extension.
     */
    public static function connect(ConnectionConfig $connectionConfig, ?CancellationToken $token = null): Promise
    {
        // @codeCoverageIgnoreStart
        if (Loop::get()->getHandle() instanceof \EvLoop) {
            throw new \Error('ext-pgsql is not compatible with pecl-ev; use pecl-pq or a different loop extension');
        } // @codeCoverageIgnoreEnd

        if (!$connection = @\pg_connect($connectionConfig->getConnectionString(), \PGSQL_CONNECT_ASYNC | \PGSQL_CONNECT_FORCE_NEW)) {
            return new Failure(new ConnectionException("Failed to create connection resource"));
        }

        if (\pg_connection_status($connection) === \PGSQL_CONNECTION_BAD) {
            return new Failure(new ConnectionException(\pg_last_error($connection)));
        }

        if (!$socket = \pg_socket($connection)) {
            return new Failure(new ConnectionException("Failed to access connection socket"));
        }

        $deferred = new Deferred;
        $id = \sha1($connectionConfig->getHost() . $connectionConfig->getPort() . $connectionConfig->getUser());

        $callback = function ($watcher, $resource) use ($connection, $deferred, $id): void {
            switch (\pg_connect_poll($connection)) {
                case \PGSQL_POLLING_READING: // Connection not ready, poll again.
                case \PGSQL_POLLING_WRITING: // Still writing...
                    return;

                case \PGSQL_POLLING_FAILED:
                    $deferred->fail(new ConnectionException(\pg_last_error($connection)));
                    return;

                case \PGSQL_POLLING_OK:
                    $deferred->resolve(new self($connection, $resource, $id));
                    return;
            }
        };

        $poll = Loop::onReadable($socket, $callback);
        $await = Loop::onWritable($socket, $callback);

        $promise = $deferred->promise();

        $token = $token ?? new NullCancellationToken;
        $id = $token->subscribe([$deferred, "fail"]);

        $promise->onResolve(function ($exception) use ($connection, $poll, $await, $id, $token): void {
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
     * @param string $id Connection identifier for determining which cached type table to use.
     */
    public function __construct($handle, $socket, string $id = '')
    {
        parent::__construct(new PgSqlHandle($handle, $socket, $id));
    }
}
