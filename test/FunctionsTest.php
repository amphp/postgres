<?php

namespace Amp\Postgres\Test;

use Amp\Loop;
use Amp\Postgres\Connection;
use function Amp\Postgres\connect;

class FunctionsTest extends \PHPUnit_Framework_TestCase {
    public function setUp() {
        if (!\extension_loaded('pgsql') && !\extension_loaded('pq')) {
            $this->markTestSkipped('This test requires either ext/pgsql or pecl/pq');
        }
    }

    public function testConnect() {
        Loop::run(function () {
            $connection = yield connect('host=localhost user=postgres', 100);
            $this->assertInstanceOf(Connection::class, $connection);
        });
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidUser() {
        Loop::run(function () {
            $connection = yield connect('host=localhost user=invalid', 100);
        });
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidConnectionString() {
        Loop::run(function () {
            $connection = yield connect('invalid connection string', 100);
        });
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidHost() {
        Loop::run(function () {
            $connection = yield connect('hostaddr=invalid.host user=postgres', 100);
        });
    }
}
