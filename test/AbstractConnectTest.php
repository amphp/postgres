<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Cancellation;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;

abstract class AbstractConnectTest extends AsyncTestCase
{
    abstract public function connect(
        PostgresConfig $connectionConfig,
        ?Cancellation $cancellation = null
    ): PostgresConnection;
}
