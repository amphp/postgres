<?php

namespace Amp\Postgres\Test;

use Amp\Postgres\ConnectionConfig;
use Amp\Postgres\Link;
use Amp\Postgres\DefaultPool;
use Amp\Postgres\PqConnection;
use Amp\Promise;
use Amp\Sql\Connector;
use Amp\Success;

/**
 * @requires extension pq
 */
class PqPoolTest extends AbstractLinkTest
{
    const POOL_SIZE = 3;

    /** @var \pq\Connection[] */
    protected $handles = [];

    public function createLink(string $connectionString): Link
    {
        for ($i = 0; $i < self::POOL_SIZE; ++$i) {
            $this->handles[] = $handle = new \pq\Connection($connectionString);
            $handle->nonblocking = true;
            $handle->unbuffered = true;
        }

        $connector = $this->createMock(Connector::class);
        $connector->method('connect')
            ->will($this->returnCallback(function (): Promise {
                static $count = 0;
                if (!isset($this->handles[$count])) {
                    $this->fail("createConnection called too many times");
                }
                $handle = $this->handles[$count];
                ++$count;
                return new Success(new PqConnection($handle));
            }));

        $pool = new DefaultPool(new ConnectionConfig('connection string'), \count($this->handles), $connector);

        $handle = \reset($this->handles);

        $handle->exec("DROP TABLE IF EXISTS test");

        $result = $handle->exec("CREATE TABLE test (domain VARCHAR(63), tld VARCHAR(63), PRIMARY KEY (domain, tld))");

        if (!$result) {
            $this->fail('Could not create test table.');
        }

        foreach ($this->getData() as $row) {
            $result = $handle->execParams("INSERT INTO test VALUES (\$1, \$2)", $row);

            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }

        return $pool;
    }

    public function tearDown()
    {
        $this->handles[0]->exec("ROLLBACK");
        $this->handles[0]->exec("DROP TABLE test");
    }
}
