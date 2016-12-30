<?php

namespace Amp\Postgres\Test;

use Amp\Postgres\{ Connection, function connect };

class FunctionsTest extends \PHPUnit_Framework_TestCase {
    public function setUp() {
        if (!\extension_loaded('pgsql') && !\extension_loaded('pq')) {
            $this->markTestSkipped('This test requires either ext/pgsql or pecl/pq');
        }
    }

    public function testConnect() {
        \Amp\execute(function () {
            $connection = yield connect('host=localhost user=postgres', 1);
            $this->assertInstanceOf(Connection::class, $connection);
        });
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidUser() {
        \Amp\execute(function () {
            $connection = yield connect('host=localhost user=invalid', 1);
        });
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidConnectionString() {
        \Amp\execute(function () {
            $connection = yield connect('invalid connection string', 1);
        });
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidHost() {
        \Amp\execute(function () {
            $connection = yield connect('hostaddr=invalid.host user=postgres', 1);
        });
    }
}
