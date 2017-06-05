<?php

namespace Amp\Postgres\Test;

use Amp\{ Promise, Success };
use Amp\Postgres\ConnectionPool;

class ConnectionPoolTest extends AbstractPoolTest {
    /**
     * @param array $connections
     *
     * @return \Amp\Postgres\Pool
     */
    protected function createPool(array $connections) {
        $mock = $this->getMockBuilder(ConnectionPool::class)
            ->setConstructorArgs(['connection string', \count($connections)])
            ->setMethods(['createConnection'])
            ->getMock();

        $mock->method('createConnection')
            ->will($this->returnCallback(function () use ($connections): Promise {
                static $count = 0;
                return new Success($connections[$count++]);
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
