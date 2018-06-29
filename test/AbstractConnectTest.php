<?php

namespace Amp\Postgres\Test;

use Amp\CancellationToken;
use Amp\CancellationTokenSource;
use Amp\Loop;
use Amp\Postgres\Connection;
use Amp\Promise;
use Amp\Sql\ConnectionConfig;
use Amp\TimeoutCancellationToken;
use PHPUnit\Framework\TestCase;

abstract class AbstractConnectTest extends TestCase
{
    /**
     * @param ConnectionConfig $connectionConfig
     * @param CancellationToken|null $token
     *
     * @return Promise
     */
    abstract public function connect(ConnectionConfig $connectionConfig, CancellationToken $token = null): Promise;

    public function testConnect()
    {
        Loop::run(function () {
            $connection = yield $this->connect(
                new \Amp\Postgres\ConnectionConfig(new \Amp\Postgres\ConnectionConfig('host=localhost user=postgres')),
                new TimeoutCancellationToken(100)
            );
            $this->assertInstanceOf(Connection::class, $connection);
        });
    }

    /**
     * @depends testConnect
     * @expectedException \Amp\CancelledException
     */
    public function testConnectCancellationBeforeConnect()
    {
        Loop::run(function () {
            $source = new CancellationTokenSource;
            $token = $source->getToken();
            $source->cancel();
            $connection = yield $this->connect(new \Amp\Postgres\ConnectionConfig('host=localhost user=postgres'), $token);
        });
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     */
    public function testConnectCancellationAfterConnect()
    {
        Loop::run(function () {
            $source = new CancellationTokenSource;
            $token = $source->getToken();
            $connection = yield $this->connect(new \Amp\Postgres\ConnectionConfig('host=localhost user=postgres'), $token);
            $this->assertInstanceOf(Connection::class, $connection);
            $source->cancel();
        });
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     * @expectedException \Amp\Sql\FailureException
     */
    public function testConnectInvalidUser()
    {
        Loop::run(function () {
            $connection = yield $this->connect(new \Amp\Postgres\ConnectionConfig('host=localhost user=invalid'), new TimeoutCancellationToken(100));
        });
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     * @expectedException \Amp\Sql\FailureException
     */
    public function testConnectInvalidConnectionString()
    {
        Loop::run(function () {
            $connection = yield $this->connect(new \Amp\Postgres\ConnectionConfig('invalid connection string'), new TimeoutCancellationToken(100));
        });
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     * @expectedException \Amp\Sql\FailureException
     */
    public function testConnectInvalidHost()
    {
        Loop::run(function () {
            $connection = yield $this->connect(new \Amp\Postgres\ConnectionConfig('hostaddr=invalid.host user=postgres'), new TimeoutCancellationToken(100));
        });
    }
}
