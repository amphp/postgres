<?php

namespace Amp\Postgres\Test;

use Amp\Loop;
use Amp\Postgres\{ Connection, function connect };
use PHPUnit\Framework\TestCase;

class FunctionsTest extends TestCase {
    public function setUp() {
        if (!\extension_loaded('pgsql') && !\extension_loaded('pq')) {
            $this->markTestSkipped('This test requires either ext/pgsql or pecl/pq');
        }
    }

    public function testConnect() {
        Loop::run(function () {
            $connection = yield connect('host=localhost user=postgres');
            $this->assertInstanceOf(Connection::class, $connection);
        });
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidUser() {
        Loop::run(function () {
            $connection = yield connect('host=localhost user=invalid');
        });
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidConnectionString() {
        Loop::run(function () {
            $connection = yield connect('invalid connection string');
        });
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidHost() {
        Loop::run(function () {
            $connection = yield connect('hostaddr=invalid.host user=postgres');
        });
    }
}
