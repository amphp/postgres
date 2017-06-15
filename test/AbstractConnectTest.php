<?php

namespace Amp\Postgres\Test;

use Amp\{ CancellationToken, CancellationTokenSource, Loop, Promise };
use Amp\Postgres\Connection;
use PHPUnit\Framework\TestCase;

abstract class AbstractConnectTest extends TestCase {
    /**
     * @param string $connectionString
     * @param \Amp\CancellationToken|null $token
     *
     * @return \Amp\Promise
     */
    abstract public function connect(string $connectionString, CancellationToken $token = null): Promise;

    public function testConnect() {
        Loop::run(function () {
            $connection = yield $this->connect('host=localhost user=postgres');
            $this->assertInstanceOf(Connection::class, $connection);
        });
    }

    /**
     * @depends testConnect
     * @expectedException \Amp\CancelledException
     */
    public function testConnectCancellationBeforeConnect() {
        Loop::run(function () {
            $source = new CancellationTokenSource;
            $token = $source->getToken();
            $source->cancel();
            $connection = yield $this->connect('host=localhost user=postgres', $token);
        });
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     */
    public function testConnectCancellationAfterConnect() {
        Loop::run(function () {
            $source = new CancellationTokenSource;
            $token = $source->getToken();
            $connection = yield $this->connect('host=localhost user=postgres', $token);
            $this->assertInstanceOf(Connection::class, $connection);
            $source->cancel();
        });
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidUser() {
        Loop::run(function () {
            $connection = yield $this->connect('host=localhost user=invalid');
        });
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidConnectionString() {
        Loop::run(function () {
            $connection = yield $this->connect('invalid connection string');
        });
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidHost() {
        Loop::run(function () {
            $connection = yield $this->connect('hostaddr=invalid.host user=postgres');
        });
    }
}
