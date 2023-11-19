<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Cancellation;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\Internal\PostgresHandleConnection;
use Amp\Postgres\PostgresConfig;

abstract class AbstractConnectTest extends AsyncTestCase
{
    abstract public function connect(
        PostgresConfig $connectionConfig,
        ?Cancellation $cancellation = null
    ): PostgresHandleConnection;
}
