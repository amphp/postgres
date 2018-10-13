<?php

namespace Amp\Postgres\Test;

use Amp\CancellationToken;
use Amp\Loop;
use Amp\Postgres\PgSqlConnection;
use Amp\Promise;
use Amp\Sql\ConnectionConfig;

/**
 * @requires extension pgsql
 */
class PgSqlConnectTest extends AbstractConnectTest
{
    public function connect(ConnectionConfig $connectionConfig, CancellationToken $token = null): Promise
    {
        if (Loop::get()->getHandle() instanceof \EvLoop) {
            $this->markTestSkipped("ext-pgsql is not compatible with pecl-ev");
        }

        return PgSqlConnection::connect($connectionConfig, $token);
    }
}
