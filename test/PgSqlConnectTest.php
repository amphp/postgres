<?php

namespace Amp\Postgres\Test;

use Amp\{ CancellationToken, Promise };
use Amp\Postgres\PgSqlConnection;

/**
 * @requires extension pgsql
 */
class PgSqlConnectTest extends AbstractConnectTest {
    public function connect(string $connectionString, CancellationToken $token = null): Promise {
        return PgSqlConnection::connect($connectionString, $token);
    }
}
