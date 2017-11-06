<?php

namespace Amp\Postgres\Test;

use Amp\Postgres\ConnectionPool;
use Amp\Postgres\Pool;
use Amp\Promise;
use Amp\Success;

class ConnectionPoolTest extends AbstractPoolTest {
    /**
     * @param array $connections
     *
     * @return \PHPUnit_Framework_MockObject_MockObject|\Amp\Postgres\Pool
     */
    protected function createPool(array $connections): Pool {
        $mock = $this->getMockBuilder(ConnectionPool::class)
            ->setConstructorArgs(['connection string', \count($connections)])
            ->setMethods(['createConnection'])
            ->getMock();

        $mock->method('createConnection')
            ->will($this->returnCallback(function () use ($connections): Promise {
                static $count = 0;
                return new Success($connections[$count++ % \count($connections)]);
            }));

        return $mock;
    }

    /**
     * @expectedException \Error
     * @expectedExceptionMessage Pool must contain at least one connection
     */
    public function testInvalidMaxConnections() {
        $pool = new ConnectionPool('connection string', 0);
    }
}
