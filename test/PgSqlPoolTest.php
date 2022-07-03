<?php

namespace Amp\Postgres\Test;

use Amp\Postgres\PgSqlConnection;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnector;
use Amp\Postgres\PostgresLink;
use Amp\Postgres\PostgresPool;
use Amp\Sql\Common\ConnectionPool;
use Revolt\EventLoop;
use function Amp\Postgres\cast;

/**
 * @requires extension pgsql
 */
class PgSqlPoolTest extends AbstractLinkTest
{
    const POOL_SIZE = 3;

    /** @var resource[] PostgreSQL connection resources. */
    protected array $handles = [];

    public function createLink(string $connectionString): PostgresLink
    {
        if (EventLoop::getDriver()->getHandle() instanceof \EvLoop) {
            $this->markTestSkipped("ext-pgsql is not compatible with pecl-ev");
        }

        for ($i = 0; $i < self::POOL_SIZE; ++$i) {
            $this->handles[] = \pg_connect($connectionString, \PGSQL_CONNECT_FORCE_NEW);
        }

        $connector = $this->createMock(PostgresConnector::class);
        $connector->method('connect')
            ->will($this->returnCallback(function (): PgSqlConnection {
                static $count = 0;
                if (!isset($this->handles[$count])) {
                    $this->fail("createConnection called too many times");
                }
                $handle = $this->handles[$count];
                ++$count;
                return $this->newConnection(PgsqlConnection::class, $handle, \pg_socket($handle));
            }));

        $pool = new PostgresPool(new PostgresConfig('localhost'), \count($this->handles), ConnectionPool::DEFAULT_IDLE_TIMEOUT, true, $connector);

        $handle = \reset($this->handles);

        \pg_query($handle, self::DROP_QUERY);

        $result = \pg_query($handle, self::CREATE_QUERY);

        if (!$result) {
            $this->fail('Could not create test table.');
        }

        foreach ($this->getData() as $row) {
            $result = \pg_query_params($handle, self::INSERT_QUERY, \array_map(cast(...), $row));

            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }

        return $pool;
    }

    public function tearDown(): void
    {
        foreach ($this->handles as $handle) {
            \pg_get_result($handle); // Consume any leftover results from test.
        }

        \pg_query($this->handles[0], "ROLLBACK");
        \pg_query($this->handles[0], self::DROP_QUERY);

        foreach ($this->handles as $handle) {
            \pg_close($handle);
        }

        $this->handles = [];

        parent::tearDown();
    }
}
