<?php

namespace Amp\Postgres\Test;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Postgres\PostgresConfig;
use Amp\Sql\SqlException;
use Amp\TimeoutCancellation;

abstract class AbstractCreateConnectionTest extends AbstractConnectTest
{
    public function testConnect()
    {
        $connection = $this->connect(
            PostgresConfig::fromString('host=localhost user=postgres password=postgres'),
            new TimeoutCancellation(1)
        );

        $this->assertFalse($connection->isClosed());

        $connection->onClose($this->createCallback(1));
        $connection->close();

        $this->assertTrue($connection->isClosed());
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
        $source->cancel();

        $this->assertFalse($connection->isClosed());
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
