<?php

namespace Amp\Postgres\Test;

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnector;
use Amp\Postgres\PostgresLink;
use Amp\Postgres\PostgresPool;
use Amp\Postgres\PqConnection;
use Amp\Sql\Common\ConnectionPool;
use function Amp\Postgres\cast;

/**
 * @requires extension pq
 */
class PqPoolTest extends AbstractLinkTest
{
    const POOL_SIZE = 3;

    /** @var \pq\Connection[] */
    protected array $handles = [];

    public function createLink(string $connectionString): PostgresLink
    {
        for ($i = 0; $i < self::POOL_SIZE; ++$i) {
            $this->handles[] = $handle = new \pq\Connection($connectionString);
            $handle->nonblocking = true;
            $handle->unbuffered = true;
        }

        $connector = $this->createMock(PostgresConnector::class);
        $connector->method('connect')
            ->will($this->returnCallback(function (): PqConnection {
                static $count = 0;
                if (!isset($this->handles[$count])) {
                    $this->fail("createConnection called too many times");
                }
                $handle = $this->handles[$count];
                ++$count;
                return $this->newConnection(PqConnection::class, $handle);
            }));

        $pool = new PostgresPool(new PostgresConfig('localhost'), \count($this->handles), ConnectionPool::DEFAULT_IDLE_TIMEOUT, true, $connector);

        $handle = \reset($this->handles);

        $handle->exec(self::DROP_QUERY);

        $result = $handle->exec(self::CREATE_QUERY);

        if (!$result) {
            $this->fail('Could not create test table.');
        }

        foreach ($this->getData() as $row) {
            $result = $handle->execParams(self::INSERT_QUERY, \array_map(cast(...), $row));

            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }

        return $pool;
    }

    public function tearDown(): void
    {
        $this->handles[0]->exec("ROLLBACK");
        $this->handles[0]->exec(self::DROP_QUERY);

        $this->handles = [];

        parent::tearDown();
    }
}
