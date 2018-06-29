<?php

namespace Amp\Postgres\Test;

use Amp\Delayed;
use Amp\Loop;
use Amp\Postgres\DefaultPool;
use Amp\Postgres\ConnectionConfig;
use PHPUnit\Framework\TestCase;

class DefaultPoolTest extends TestCase
{
    /**
     * @expectedException \Error
     * @expectedExceptionMessage Pool must contain at least one connection
     */
    public function testInvalidMaxConnections()
    {
        new DefaultPool(new ConnectionConfig('connection string'), 0);
    }

    public function testIdleConnectionsRemovedAfterTimeout()
    {
        Loop::run(function () {
            $pool = new DefaultPool(new ConnectionConfig('host=localhost user=postgres'));
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
