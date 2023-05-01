<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Cancellation;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PqConnection;

/**
 * @requires extension pq
 */
class PqCreateConnectionTest extends AbstractCreateConnectionTest
{
    public function connect(PostgresConfig $connectionConfig, ?Cancellation $cancellation = null): PqConnection
    {
        return PqConnection::connect($connectionConfig, $cancellation);
    }
}
