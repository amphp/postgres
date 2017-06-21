<?php

namespace Amp\Postgres\Test;

use Amp\CancellationToken;
use Amp\Postgres\PgSqlConnection;
use Amp\Promise;

/**
 * @requires extension pgsql
 */
class PgSqlConnectTest extends AbstractConnectTest {
    public function connect(string $connectionString, CancellationToken $token = null): Promise {
        return PgSqlConnection::connect($connectionString, $token);
    }
}
