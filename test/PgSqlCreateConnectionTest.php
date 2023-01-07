<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Cancellation;
use Amp\Postgres\PgSqlConnection;
use Amp\Postgres\PostgresConfig;
use Revolt\EventLoop;

/**
 * @requires extension pgsql
 */
class PgSqlCreateConnectionTest extends AbstractCreateConnectionTest
{
    public function connect(PostgresConfig $connectionConfig, ?Cancellation $cancellation = null): PgSqlConnection
    {
        if (EventLoop::getDriver()->getHandle() instanceof \EvLoop) {
            $this->markTestSkipped("ext-pgsql is not compatible with pecl-ev");
        }

        return PgSqlConnection::connect($connectionConfig, $cancellation);
    }
}
