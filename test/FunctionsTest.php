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
            $connection = yield connect(ConnectionConfig::fromString('host=localhost user=postgres'));
            $this->assertInstanceOf(Connection::class, $connection);
        });
    }

    /**
     * @expectedException \Amp\Sql\FailureException
     */
    public function testConnectInvalidUser()
    {
        Loop::run(function () {
            $connection = yield connect(ConnectionConfig::fromString('host=localhost user=invalid'));
        });
    }
}
