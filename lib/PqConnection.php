<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Amp\{ Deferred, TimeoutException };
use Interop\Async\{ Loop, Promise };
use pq;

class PqConnection extends AbstractConnection {
    /**
     * @param string $connectionString
     * @param int|null $timeout
     *
     * @return \Interop\Async\Promise<\Amp\Postgres\PgSqlConnection>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public static function connect(string $connectionString, int $timeout = null): Promise {
        try {
            $connection = new pq\Connection($connectionString, pq\Connection::ASYNC);
        } catch (pq\Exception $exception) {
            throw new FailureException("Could not connect to PostgresSQL server", 0, $exception);
        }
        $connection->resetAsync();
        $connection->nonblocking = true;
        $connection->unbuffered = true;
        
        $deferred = new Deferred;
    
        $callback = function ($watcher, $resource) use (&$poll, &$await, $connection, $deferred) {
            try {
                switch ($connection->poll()) {
                    case pq\Connection::POLLING_READING:
                        return; // Connection not ready, poll again.
    
                    case pq\Connection::POLLING_WRITING:
                        return; // Still writing...
    
                    case pq\Connection::POLLING_FAILED:
                        throw new FailureException("Could not connect to PostgreSQL server");
                    
                    case pq\Connection::POLLING_OK:
                    case \PGSQL_POLLING_OK:
                        Loop::cancel($poll);
                        Loop::cancel($await);
                        $deferred->resolve(new self($connection));
                        return;
                }
            } catch (\Throwable $exception) {
                Loop::cancel($poll);
                Loop::cancel($await);
                $deferred->fail($exception);
            }
        };
    
        $poll = Loop::onReadable($connection->socket, $callback);
        $await = Loop::onWritable($connection->socket, $callback);
    
        if ($timeout !== null) {
            return \Amp\capture(
                $deferred->promise(),
                TimeoutException::class,
                function (\Throwable $exception) use ($connection, $poll, $await) {
                    Loop::cancel($poll);
                    Loop::cancel($await);
                    throw $exception;
                }
            );
        }
    
        return $deferred->promise();
    }
    
    /**
     * Connection constructor.
     *
     * @param \pq\Connection $handle
     */
    public function __construct(pq\Connection $handle) {
        parent::__construct(new PqExecutor($handle));
    }
}
