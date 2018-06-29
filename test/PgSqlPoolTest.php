<?php

namespace Amp\Postgres\Test;

use Amp\Postgres\Link;
use Amp\Postgres\Pool;
use Amp\Promise;
use Amp\Sql\Connector;
use Amp\Success;

/**
 * @requires extension pgsql
 */
class PgSqlPoolTest extends AbstractLinkTest
{
    const POOL_SIZE = 3;

    /** @var resource[] PostgreSQL connection resources. */
    protected $handles = [];

    public function createLink(string $connectionString): Link
    {
        for ($i = 0; $i < self::POOL_SIZE; ++$i) {
            $this->handles[] = \pg_connect($connectionString, \PGSQL_CONNECT_FORCE_NEW);
        }

        $connector = $this->createMock(Connector::class);
        $connector->method('connect')
            ->will($this->returnCallback(function (): Promise {
                static $count = 0;
                if (!isset($this->handles[$count])) {
                    $this->fail("createConnection called too many times");
                }
                ++$count;
                return new Success();
            }));

        $pool = new Pool('connection string', \count($this->handles), $connector);

        $handle = \reset($this->handles);

        \pg_query($handle, "DROP TABLE IF EXISTS test");

        $result = \pg_query($handle, "CREATE TABLE test (domain VARCHAR(63), tld VARCHAR(63), PRIMARY KEY (domain, tld))");

        if (!$result) {
            $this->fail('Could not create test table.');
        }

        foreach ($this->getData() as $row) {
            $result = \pg_query_params($handle, "INSERT INTO test VALUES (\$1, \$2)", $row);

            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }

        return $pool;
    }

    public function tearDown()
    {
        foreach ($this->handles as $handle) {
            \pg_get_result($handle); // Consume any leftover results from test.
        }

        \pg_query($this->handles[0], "ROLLBACK");
        \pg_query($this->handles[0], "DROP TABLE test");

        foreach ($this->handles as $handle) {
            \pg_close($handle);
        }
    }
}
