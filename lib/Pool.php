<?php

namespace Amp\Postgres;

use Amp\Promise;

interface Pool extends Link {
    /**
     * @return \Amp\Promise<\Amp\Postgres\Connection>
     */
    public function getConnection(): Promise;

    /**
     * @return int Current number of connections in the pool.
     */
    public function getConnectionCount(): int;

    /**
     * @return int Current number of idle connections in the pool.
     */
    public function getIdleConnectionCount(): int;

    /**
     * @return int Maximum number of connections.
     */
    public function getMaxConnections(): int;
}
