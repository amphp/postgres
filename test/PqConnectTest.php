<?php

namespace Amp\Postgres\Test;

use Amp\CancellationToken;
use Amp\Postgres\PqConnection;
use Amp\Promise;

/**
 * @requires extension pq
 */
class PqConnectTest extends AbstractConnectTest {
    public function connect(string $connectionString, CancellationToken $token = null): Promise {
        return PqConnection::connect($connectionString, $token);
    }
}
