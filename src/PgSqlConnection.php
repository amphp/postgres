<?php

namespace Amp\Postgres;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\NullCancellation;
use Amp\Sql\ConnectionException;
use Revolt\EventLoop;

final class PgSqlConnection extends PostgresConnection implements PostgresLink
{
    /**
     * @throws \Error If pecl-ev is used as a loop extension.
     */
    public static function connect(PostgresConfig $connectionConfig, ?Cancellation $cancellation = null): self
    {
        // @codeCoverageIgnoreStart
        /** @psalm-suppress UndefinedClass */
        if (EventLoop::getDriver()->getHandle() instanceof \EvLoop) {
            throw new \Error('ext-pgsql is not compatible with pecl-ev; use pecl-pq or a different loop extension');
        } // @codeCoverageIgnoreEnd

        if (!$connection = \pg_connect($connectionConfig->getConnectionString(), \PGSQL_CONNECT_ASYNC | \PGSQL_CONNECT_FORCE_NEW)) {
            throw new ConnectionException("Failed to create connection resource");
        }

        if (\pg_connection_status($connection) === \PGSQL_CONNECTION_BAD) {
            throw new ConnectionException(\pg_last_error($connection));
        }

        if (!$socket = \pg_socket($connection)) {
            throw new ConnectionException("Failed to access connection socket");
        }

        $deferred = new DeferredFuture;
        $id = \sha1($connectionConfig->getHost() . $connectionConfig->getPort() . $connectionConfig->getUser());

        /** @psalm-suppress MissingClosureParamType $resource is a resource and cannot be inferred in this context */
        $callback = static function (string $watcher, $resource) use ($connection, $deferred, $id): void {
            if ($deferred->isComplete()) {
                return;
            }

            switch (\pg_connect_poll($connection)) {
                case \PGSQL_POLLING_READING: // Connection not ready, poll again.
                case \PGSQL_POLLING_WRITING: // Still writing...
                    return;

                case \PGSQL_POLLING_FAILED:
                    $deferred->error(new ConnectionException(\pg_last_error($connection)));
                    return;

                case \PGSQL_POLLING_OK:
                    $deferred->complete(new self($connection, $resource, $id));
                    return;
            }
        };

        $poll = EventLoop::onReadable($socket, $callback);
        $await = EventLoop::onWritable($socket, $callback);

        $future = $deferred->getFuture();

        $cancellation ??= new NullCancellation;
        $id = $cancellation->subscribe(static function (CancelledException $exception) use ($deferred): void {
            if (!$deferred->isComplete()) {
                $deferred->error($exception);
            }
        });

        try {
            return $future->await();
        } catch (\Throwable $exception) {
            \pg_close($connection);
            throw $exception;
        } finally {
            $cancellation->unsubscribe($id);
            EventLoop::cancel($poll);
            EventLoop::cancel($await);
        }
    }

    /**
     * @param \PgSql\Connection $handle PostgreSQL connection handle.
     * @param resource $socket PostgreSQL connection stream socket.
     * @param string $id Connection identifier for determining which cached type table to use.
     */
    protected function __construct($handle, $socket, string $id)
    {
        parent::__construct(new Internal\PgSqlHandle($handle, $socket, $id));
    }
}
