<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Interop\Async\Awaitable;

class AggregatePool extends AbstractPool {
    /**
     * @param \Amp\Postgres\Connection $connection
     */
    public function addConnection(Connection $connection) {
        parent::addConnection($connection);
    }

    /**
     * {@inheritdoc}
     */
    protected function createConnection(): Awaitable {
        throw new PoolError("Creating connections is not available in an aggregate pool");
    }

    /**
     * {@inheritdoc}
     */
    public function getMaxConnections(): int {
        $count = $this->getConnectionCount();

        if (!$count) {
            throw new PoolError("No connections in aggregate pool");
        }

        return $count;
    }
}
