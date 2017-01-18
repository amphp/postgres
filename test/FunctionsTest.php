<?php

namespace Amp\Postgres\Test;

use Amp\Postgres\{ Connection, function connect };
use AsyncInterop\Loop;

class FunctionsTest extends \PHPUnit_Framework_TestCase {
    public function setUp() {
        if (!\extension_loaded('pgsql') && !\extension_loaded('pq')) {
            $this->markTestSkipped('This test requires either ext/pgsql or pecl/pq');
        }
    }

    public function testConnect() {
        Loop::execute(\Amp\wrap(function () {
            $connection = yield connect('host=localhost user=postgres', 1);
            $this->assertInstanceOf(Connection::class, $connection);
        }));
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidUser() {
        Loop::execute(\Amp\wrap(function () {
            $connection = yield connect('host=localhost user=invalid', 1);
        }));
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidConnectionString() {
        Loop::execute(\Amp\wrap(function () {
            $connection = yield connect('invalid connection string', 1);
        }));
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidHost() {
        Loop::execute(\Amp\wrap(function () {
            $connection = yield connect('hostaddr=invalid.host user=postgres', 1);
        }));
    }
}
