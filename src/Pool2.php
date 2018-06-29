<?php

namespace Amp\Postgres;

use Amp\Promise;

interface Pool2 extends Link
{
    const DEFAULT_MAX_CONNECTIONS = 100;
    const DEFAULT_IDLE_TIMEOUT = 60;

    /**
     * @return Promise<Link>
     */
    public function extractConnection(): Promise;

    /**
     * @return int Total number of active connections in the pool.
     */
    public function getConnectionCount(): int;

    /**
     * @return int Total number of idle connections in the pool.
     */
    public function getIdleConnectionCount(): int;

    /**
     * @return int Maximum number of connections this pool will create.
     */
    public function getMaxConnections(): int;

    /**
     * @param bool $reset True to automatically reset a connection in the pool before using it for an operation.
     */
    public function resetConnections(bool $reset);

    /**
     * @return int Number of seconds a connection may remain idle before it is automatically closed.
     */
    public function getIdleTimeout(): int;

    /**
     * @param int $timeout Number of seconds a connection may remain idle before it is automatically closed.
     */
    public function setIdleTimeout(int $timeout);
}
