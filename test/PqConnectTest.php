<?php

namespace Amp\Postgres\Test;

use Amp\{ CancellationToken, Promise };
use Amp\Postgres\PqConnection;

/**
 * @requires extension pq
 */
class PqConnectTest extends AbstractConnectTest {
    public function connect(string $connectionString, CancellationToken $token = null): Promise {
        return PqConnection::connect($connectionString, $token);
    }
}
