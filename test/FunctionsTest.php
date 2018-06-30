<?php

namespace Amp\Postgres\Test;

use Amp\Loop;
use Amp\Postgres\Connection;
use Amp\Postgres\ConnectionConfig;
use PHPUnit\Framework\TestCase;
use function Amp\Postgres\connect;

class FunctionsTest extends TestCase
{
    public function setUp()
    {
        if (!\extension_loaded('pgsql') && !\extension_loaded('pq')) {
            $this->markTestSkipped('This test requires either ext/pgsql or pecl/pq');
        }
    }

    public function testConnect()
    {
        Loop::run(function () {
            $connection = yield connect(new ConnectionConfig('host=localhost user=postgres'));
            $this->assertInstanceOf(Connection::class, $connection);
        });
    }

    /**
     * @expectedException \Amp\Sql\FailureException
     */
    public function testConnectInvalidUser()
    {
        Loop::run(function () {
            $connection = yield connect(new ConnectionConfig('host=localhost user=invalid'));
        });
    }

    /**
     * @expectedException \Amp\Sql\FailureException
     */
    public function testConnectInvalidConnectionString()
    {
        Loop::run(function () {
            $connection = yield connect(new ConnectionConfig('invalid connection string'));
        });
    }

    /**
     * @expectedException \Amp\Sql\FailureException
     */
    public function testConnectInvalidHost()
    {
        Loop::run(function () {
            $connection = yield connect(new ConnectionConfig('hostaddr=invalid.host user=postgres'));
        });
    }
}
