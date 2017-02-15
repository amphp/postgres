<?php

namespace Amp\Postgres;

use AsyncInterop\Promise;

/**
 * @param string $connectionString
 * @param int $timeout
 *
 * @return \AsyncInterop\Promise<\Amp\Postgres\Connection>
 *
 * @throws \Amp\Postgres\FailureException If connecting fails.
 * @throws \Error If neither ext-pgsql or pecl-pq is loaded.
 */
function connect(string $connectionString, int $timeout = 0): Promise {
    if (\extension_loaded("pq")) {
        return PqConnection::connect($connectionString, $timeout);
    }
    
    if (\extension_loaded("pgsql")) {
        return PgSqlConnection::connect($connectionString, $timeout);
    }
    
    throw new \Error("This lib requires either pecl-pq or ext-pgsql");
}

/**
 * @param string $connectionString
 * @param int $maxConnections
 * @param int $connectTimeout
 *
 * @return \Amp\Postgres\Pool
 */
function pool(
    string $connectionString,
    int $maxConnections = ConnectionPool::DEFAULT_MAX_CONNECTIONS,
    int $connectTimeout = ConnectionPool::DEFAULT_CONNECT_TIMEOUT
): Pool {
    return new ConnectionPool($connectionString, $maxConnections, $connectTimeout);
}
