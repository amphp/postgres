<?php

namespace Amp\Postgres\Test;

use Amp\Postgres\AggregatePool;

class AggregatePoolTest extends AbstractPoolTest {
    /**
     * @param array $connections
     *
     * @return \Amp\Postgres\Pool
     */
    protected function createPool(array $connections) {
        $mock = $this->getMockBuilder(AggregatePool::class)
            ->setConstructorArgs(['', 0, count($connections)])
            ->setMethods(['createConnection'])
            ->getMock();

        $mock->method('createConnection')
            ->will($this->returnCallback(function () {
                $this->fail('The createConnection() method should not be called.');
            }));

        foreach ($connections as $connection) {
            $mock->addConnection($connection);
        }

        return $mock;
    }

    public function testGetMaxConnections() {
        $pool = $this->createPool([$this->createConnection()]);
        $this->assertSame(1, $pool->getMaxConnections());
        $pool->addConnection($this->createConnection());
        $this->assertSame(2, $pool->getMaxConnections());
    }

    public function testGetConnectionCount() {
        $pool = $this->createPool([$this->createConnection(), $this->createConnection()]);
        $this->assertSame(2, $pool->getConnectionCount());
    }

    public function testGetIdleConnectionCount() {
        $pool = $this->createPool([$this->createConnection(), $this->createConnection()]);
        $this->assertSame(2, $pool->getIdleConnectionCount());
        $promise = $pool->query("SELECT 1");
        $this->assertSame(1, $pool->getIdleConnectionCount());
    }
}
