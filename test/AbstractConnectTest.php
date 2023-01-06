<?php

namespace Amp\Postgres\Test;

use Amp\CancellationToken;
use Amp\CancellationTokenSource;
use Amp\CancelledException;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\Connection;
use Amp\Postgres\ConnectionConfig as PostgresConnectionConfig;
use Amp\Promise;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\FailureException;
use Amp\TimeoutCancellationToken;

abstract class AbstractConnectTest extends AsyncTestCase
{
    /**
     * @param ConnectionConfig $connectionConfig
     * @param CancellationToken|null $token
     *
     * @return Promise
     */
    abstract public function connect(ConnectionConfig $connectionConfig, CancellationToken $token = null): Promise;

    public function testConnect(): \Generator
    {
        $connection = yield $this->connect(
            PostgresConnectionConfig::fromString('host=localhost user=postgres'),
            new TimeoutCancellationToken(100)
        );
        $this->assertInstanceOf(Connection::class, $connection);
    }

    /**
     * @depends testConnect
     */
    public function testConnectCancellationBeforeConnect(): Promise
    {
        $this->expectException(CancelledException::class);

        $source = new CancellationTokenSource;
        $token = $source->getToken();
        $source->cancel();
        return $this->connect(PostgresConnectionConfig::fromString('host=localhost user=postgres'), $token);
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     */
    public function testConnectCancellationAfterConnect(): \Generator
    {
        $source = new CancellationTokenSource;
        $token = $source->getToken();
        $connection = yield $this->connect(PostgresConnectionConfig::fromString('host=localhost user=postgres'), $token);
        $this->assertInstanceOf(Connection::class, $connection);
        $source->cancel();
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     */
    public function testConnectInvalidUser(): Promise
    {
        $this->expectException(FailureException::class);

        return $this->connect(PostgresConnectionConfig::fromString('host=localhost user=invalid'), new TimeoutCancellationToken(100));
    }

    public function testConnectionClose(): \Generator
    {
        $connection = yield $this->connect(PostgresConnectionConfig::fromString('host=localhost user=postgres'));
        $this->assertInstanceOf(Connection::class, $connection);

        $connection->execute('SELECT pg_sleep(10)');

        $start = microtime(true);
        $connection->close();
        $this->assertEquals(0, round(microtime(true) - $start));
    }
}
