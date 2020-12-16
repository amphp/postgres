<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\Loop;
use Amp\NullCancellationToken;
use Amp\Sql\ConnectionException;
use function Amp\await;

final class PgSqlConnection extends Connection implements Link
{
    /**
     * @param ConnectionConfig $connectionConfig
     * @param CancellationToken $token
     *
     * @return PgSqlConnection
     *
     * @throws \Error If pecl-ev is used as a loop extension.
     */
    public static function connect(ConnectionConfig $connectionConfig, ?CancellationToken $token = null): self
    {
        // @codeCoverageIgnoreStart
        /** @psalm-suppress UndefinedClass */
        if (Loop::getDriver()->getHandle() instanceof \EvLoop) {
            throw new \Error('ext-pgsql is not compatible with pecl-ev; use pecl-pq or a different loop extension');
        } // @codeCoverageIgnoreEnd

        if (!$connection = @\pg_connect($connectionConfig->getConnectionString(), \PGSQL_CONNECT_ASYNC | \PGSQL_CONNECT_FORCE_NEW)) {
            throw new ConnectionException("Failed to create connection resource");
        }

        if (\pg_connection_status($connection) === \PGSQL_CONNECTION_BAD) {
            throw new ConnectionException(\pg_last_error($connection));
        }

        if (!$socket = \pg_socket($connection)) {
            throw new ConnectionException("Failed to access connection socket");
        }

        $deferred = new Deferred;

        $callback = function (string $watcher, $resource) use ($connection, $deferred): void {
            if ($deferred->isResolved()) {
                return;
            }

            switch (\pg_connect_poll($connection)) {
                case \PGSQL_POLLING_READING: // Connection not ready, poll again.
                case \PGSQL_POLLING_WRITING: // Still writing...
                    return;

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

        try {
            return await($promise);
        } catch (\Throwable $exception) {
            \pg_close($connection);
            throw $exception;
        } finally {
            $token->unsubscribe($id);
            Loop::cancel($poll);
            Loop::cancel($await);
        }
    }

    /**
     * @param resource $handle PostgreSQL connection handle.
     * @param resource $socket PostgreSQL connection stream socket.
     */
    public function __construct($handle, $socket)
    {
        parent::__construct(new PgSqlHandle($handle, $socket));
    }
}
