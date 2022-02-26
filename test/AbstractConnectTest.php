<?php

namespace Amp\Postgres\Test;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\Connection;
use Amp\Postgres\ConnectionConfig as PostgresConnectionConfig;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\FailureException;
use Amp\TimeoutCancellation;

abstract class AbstractConnectTest extends AsyncTestCase
{
    abstract public function connect(ConnectionConfig $connectionConfig, Cancellation $cancellation = null): Connection;

    public function testConnect()
    {
        $connection = $this->connect(
            PostgresConnectionConfig::fromString('host=localhost user=postgres password=postgres'),
            new TimeoutCancellation(1)
        );
        $this->assertInstanceOf(Connection::class, $connection);
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
        $this->connect(PostgresConnectionConfig::fromString('host=localhost user=postgres password=postgres'), $cancellation);
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     */
    public function testConnectCancellationAfterConnect()
    {
        $source = new DeferredCancellation;
        $cancellation = $source->getCancellation();
        $connection = $this->connect(PostgresConnectionConfig::fromString('host=localhost user=postgres password=postgres'), $cancellation);
        $this->assertInstanceOf(Connection::class, $connection);
        $source->cancel();
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     */
    public function testConnectInvalidUser()
    {
        $this->expectException(FailureException::class);

        $this->connect(PostgresConnectionConfig::fromString('host=localhost user=invalid password=invalid'), new TimeoutCancellation(100));
    }
}
