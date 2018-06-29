<?php

namespace Amp\Postgres\Test;

use Amp\CancellationToken;
use Amp\Postgres\PgSqlConnection;
use Amp\Promise;
use Amp\Sql\ConnectionConfig;

/**
 * @requires extension pgsql
 */
class PgSqlConnectTest extends AbstractConnectTest {
    public function connect(ConnectionConfig $connectionConfig, CancellationToken $token = null): Promise {
        return PgSqlConnection::connect($connectionConfig, $token);
    }
}
