<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Promise;

/**
 * @param string $connectionString
 * @param \Amp\CancellationToken $token
 *
 * @return \Amp\Promise<\Amp\Postgres\Connection>
 *
 * @throws \Amp\Postgres\FailureException If connecting fails.
 * @throws \Error If neither ext-pgsql or pecl-pq is loaded.
 *
 * @codeCoverageIgnore
 */
function connect(string $connectionString, CancellationToken $token = null): Promise {
    if (\extension_loaded("pq")) {
        return PqConnection::connect($connectionString, $token);
    }

    if (\extension_loaded("pgsql")) {
        return PgSqlConnection::connect($connectionString, $token);
    }

    throw new \Error("amphp/postgres requires either pecl-pq or ext-pgsql");
}

/**
 * @param string $connectionString
 * @param int $maxConnections
 *
 * @return \Amp\Postgres\Pool
 */
function pool(string $connectionString, int $maxConnections = ConnectionPool::DEFAULT_MAX_CONNECTIONS): Pool {
    return new ConnectionPool($connectionString, $maxConnections);
}
