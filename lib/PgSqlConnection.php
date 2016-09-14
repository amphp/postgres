<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Amp\{ Deferred, TimeoutException };
use Interop\Async\{ Awaitable, Loop };

class PgSqlConnection extends AbstractConnection {
    
    /** @var \Amp\Postgres\PqConnection */
    private $executor;
    
    /** @var \Amp\Deferred|null */
    private $busy;
    
    /** @var callable */
    private $release;
    
    /**
     * @param string $connectionString
     * @param int|null $timeout
     *
     * @return \Interop\Async\Awaitable<\Amp\Postgres\PgSqlConnection>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public static function connect(string $connectionString, int $timeout = null): Awaitable {
        if (!$connection = @\pg_connect($connectionString, \PGSQL_CONNECT_ASYNC | \PGSQL_CONNECT_FORCE_NEW)) {
            throw new FailureException("Failed to create connection resource");
        }
    
        if (\pg_connection_status($connection) === \PGSQL_CONNECTION_BAD) {
            throw new FailureException(\pg_last_error($connection));
        }
    
        if (!$socket = \pg_socket($connection)) {
            throw new FailureException("Failed to access connection socket");
        }
    
        $deferred = new Deferred;
    
        $callback = function ($watcher, $resource) use (&$poll, &$await, $connection, $deferred) {
            try {
                switch (\pg_connect_poll($connection)) {
                    case \PGSQL_POLLING_READING:
                        return; // Connection not ready, poll again.
                
                    case \PGSQL_POLLING_WRITING:
                        return; // Still writing...
                
                    case \PGSQL_POLLING_FAILED:
                        throw new FailureException("Could not connect to PostgreSQL server");
                
                    case \PGSQL_POLLING_OK:
                        Loop::cancel($poll);
                        Loop::cancel($await);
                        $deferred->resolve(new self($connection, $resource));
                        return;
                }
            } catch (\Throwable $exception) {
                Loop::cancel($poll);
                Loop::cancel($await);
                \pg_close($connection);
                $deferred->fail($exception);
            }
        };
    
        $poll = Loop::onReadable($socket, $callback);
        $await = Loop::onWritable($socket, $callback);
    
        if ($timeout !== null) {
            return \Amp\capture(
                $deferred->getAwaitable(),
                TimeoutException::class,
                function (\Throwable $exception) use ($connection, $poll, $await) {
                    Loop::cancel($poll);
                    Loop::cancel($await);
                    \pg_close($connection);
                    throw $exception;
                }
            );
        }
    
        return $deferred->getAwaitable();
    }
    
    /**
     * Connection constructor.
     *
     * @param resource $handle PostgreSQL connection handle.
     * @param resource $socket PostgreSQL connection stream socket.
     */
    public function __construct($handle, $socket) {
        parent::__construct(new PgSqlExecutor($handle, $socket));
    }
}
