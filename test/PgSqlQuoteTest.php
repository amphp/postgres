<?php

namespace Amp\Postgres\Test;

use Amp\Cancellation;
use Amp\Postgres\PgSqlConnection;
use Amp\Postgres\PostgresConfig;
use Revolt\EventLoop;

class PgSqlQuoteTest extends AbstractQuoteTest
{
    public function connect(PostgresConfig $connectionConfig, ?Cancellation $cancellation = null): PgSqlConnection
    {
        if (EventLoop::getDriver()->getHandle() instanceof \EvLoop) {
            $this->markTestSkipped("ext-pgsql is not compatible with pecl-ev");
        }

        return PgSqlConnection::connect($connectionConfig, $cancellation);
    }
}
