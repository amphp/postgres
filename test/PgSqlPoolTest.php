<?php

namespace Amp\Postgres\Test;

use Amp\Loop;
use Amp\Postgres\ConnectionConfig;
use Amp\Postgres\Link;
use Amp\Postgres\PgSqlConnection;
use Amp\Postgres\Pool;
use Amp\Promise;
use Amp\Sql\Connector;
use Amp\Success;

/**
 * @requires extension pgsql
 */
class PgSqlPoolTest extends AbstractPoolTest
{
    const POOL_SIZE = 3;

    /** @var resource[] PostgreSQL connection resources. */
    protected $handles = [];

    public function createLink(string $connectionString): Link
    {
        if (Loop::get()->getHandle() instanceof \EvLoop) {
            $this->markTestSkipped("ext-pgsql is not compatible with pecl-ev");
        }

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
                $handle = $this->handles[$count];
                ++$count;
                return new Success(new PgSqlConnection($handle, \pg_socket($handle)));
            }));

        $pool = new Pool(new ConnectionConfig('localhost'), \count($this->handles), Pool::DEFAULT_IDLE_TIMEOUT, true, $connector);

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
