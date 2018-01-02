<?php

namespace Amp\Postgres\Test;

use Amp\Delayed;
use Amp\Loop;
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

    public function testIdleConnectionsRemovedAfterTimeout() {
        Loop::run(function () {
            $pool = new ConnectionPool('host=localhost user=postgres');
            $pool->setIdleTimeout(2);
            $count = 3;

            $promises = [];
            for ($i = 0; $i < $count; ++$i) {
                $promises[] = $pool->query("SELECT $i");
            }

            $results = yield $promises;

            /** @var \Amp\Postgres\ResultSet $result */
            foreach ($results as $result) {
                while (yield $result->advance()); // Consume results to free connection
            }

            $this->assertSame($count, $pool->getConnectionCount());

            yield new Delayed(1000);

            $this->assertSame($count, $pool->getConnectionCount());

            $result = yield $pool->query("SELECT $i");
            while (yield $result->advance()); // Consume results to free connection

            yield new Delayed(1000);

            $this->assertSame(1, $pool->getConnectionCount());
        });
    }
}
