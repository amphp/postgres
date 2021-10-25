<?php

namespace Amp\Postgres\Test;

use Amp\CancellationToken;
use Amp\CancellationTokenSource;
use Amp\CancelledException;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\Connection;
use Amp\Postgres\ConnectionConfig as PostgresConnectionConfig;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\FailureException;
use Amp\TimeoutCancellationToken;

abstract class AbstractConnectTest extends AsyncTestCase
{
    /**
     * @param ConnectionConfig $connectionConfig
     * @param CancellationToken|null $token
     *
     * @return Connection
     */
    abstract public function connect(ConnectionConfig $connectionConfig, CancellationToken $token = null): Connection;

    public function testConnect()
    {
        $connection = $this->connect(
            PostgresConnectionConfig::fromString('host=localhost user=postgres'),
            new TimeoutCancellationToken(1)
        );
        $this->assertInstanceOf(Connection::class, $connection);
    }

    /**
     * @depends testConnect
     */
    public function testConnectCancellationBeforeConnect()
    {
        $this->expectException(CancelledException::class);

        $source = new CancellationTokenSource;
        $token = $source->getToken();
        $source->cancel();
        $this->connect(PostgresConnectionConfig::fromString('host=localhost user=postgres'), $token);
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     */
    public function testConnectCancellationAfterConnect()
    {
        $source = new CancellationTokenSource;
        $token = $source->getToken();
        $connection = $this->connect(PostgresConnectionConfig::fromString('host=localhost user=postgres'), $token);
        $this->assertInstanceOf(Connection::class, $connection);
        $source->cancel();
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     */
    public function testConnectInvalidUser()
    {
        $this->expectException(FailureException::class);

        $this->connect(PostgresConnectionConfig::fromString('host=localhost user=invalid'), new TimeoutCancellationToken(100));
    }
}
