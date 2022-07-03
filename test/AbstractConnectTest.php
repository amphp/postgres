<?php

namespace Amp\Postgres\Test;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
use Amp\Sql\SqlException;
use Amp\TimeoutCancellation;

abstract class AbstractConnectTest extends AsyncTestCase
{
    abstract public function connect(PostgresConfig $connectionConfig, Cancellation $cancellation = null): PostgresConnection;

    public function testConnect()
    {
        $connection = $this->connect(
            PostgresConfig::fromString('host=localhost user=postgres password=postgres'),
            new TimeoutCancellation(1)
        );
        $this->assertInstanceOf(PostgresConnection::class, $connection);
    }

    /**
     * @depends testConnect
     */
    public function testConnectCancellationBeforeConnect()
    {
        $this->expectException(CancelledException::class);

        $source = new DeferredCancellation;
        $cancellation = $source->getCancellation();
        $source->cancel();
        $this->connect(PostgresConfig::fromString('host=localhost user=postgres password=postgres'), $cancellation);
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     */
    public function testConnectCancellationAfterConnect()
    {
        $source = new DeferredCancellation;
        $cancellation = $source->getCancellation();
        $connection = $this->connect(PostgresConfig::fromString('host=localhost user=postgres password=postgres'), $cancellation);
        $this->assertInstanceOf(PostgresConnection::class, $connection);
        $source->cancel();
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     */
    public function testConnectInvalidUser()
    {
        $this->expectException(SqlException::class);

        $this->connect(PostgresConfig::fromString('host=localhost user=invalid password=invalid'), new TimeoutCancellation(100));
    }
}
