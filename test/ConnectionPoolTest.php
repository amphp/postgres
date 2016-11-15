<?php declare(strict_types = 1);

namespace Amp\Postgres\Test;

use Amp\Postgres\ConnectionPool;
use Amp\Success;
use Interop\Async\Promise;

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
}
