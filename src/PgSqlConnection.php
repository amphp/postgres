<?php declare(strict_types=1);

namespace Amp\Postgres;

use Amp\Cancellation;
use Amp\DeferredFuture;
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

        $hash = \sha1($connectionConfig->getHost() . $connectionConfig->getPort() . $connectionConfig->getUser());

        $deferred = new DeferredFuture();
        /** @psalm-suppress MissingClosureParamType $resource is a resource and cannot be inferred in this context */
        $callback = static function (string $callbackId, $resource) use (&$poll, &$await, $connection, $deferred, $hash): void {
            switch ($result = \pg_connect_poll($connection)) {
                case \PGSQL_POLLING_READING:
                case \PGSQL_POLLING_WRITING:
                    return; // Connection still reading or writing, so return and leave callback enabled.

                case \PGSQL_POLLING_FAILED:
                    $deferred->error(new ConnectionException(\pg_last_error($connection)));
                    break;

                case \PGSQL_POLLING_OK:
                    $deferred->complete(new self($connection, $resource, $hash));
                    break;

                default:
                    $deferred->error(new ConnectionException('Unexpected connection status value: ' . $result));
                    break;
            }

            EventLoop::disable($poll);
            EventLoop::disable($await);
        };

        $poll = EventLoop::onReadable($socket, $callback);
        $await = EventLoop::onWritable($socket, $callback);

        try {
            return $deferred->getFuture()->await($cancellation);
        } finally {
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
