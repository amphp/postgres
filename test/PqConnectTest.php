<?php

namespace Amp\Postgres\Test;

use Amp\CancellationToken;
use Amp\Postgres\PqConnection;
use Amp\Sql\ConnectionConfig;

/**
 * @requires extension pq
 */
class PqConnectTest extends AbstractConnectTest
{
    public function connect(ConnectionConfig $connectionConfig, CancellationToken $token = null): PqConnection
    {
        return PqConnection::connect($connectionConfig, $token);
    }
}
