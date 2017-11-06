<?php

namespace Amp\Postgres\Test;

use Amp\Postgres\AggregatePool;
use Amp\Postgres\Link;
use Amp\Postgres\PgSqlConnection;

/**
 * @requires extension pgsql
 */
class PgSqlPoolTest extends AbstractLinkTest {
    /** @var resource[] PostgreSQL connection resources. */
    protected $handles = [];

    public function createLink(string $connectionString): Link {
        $pool = new AggregatePool;

        $handle = \pg_connect($connectionString, \PGSQL_CONNECT_FORCE_NEW);
        $socket = \pg_socket($handle);

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

        $this->handles[] = $handle;

        $pool->addConnection(new PgSqlConnection($handle, $socket));

        $handle = \pg_connect($connectionString, \PGSQL_CONNECT_FORCE_NEW);
        $socket = \pg_socket($handle);

        $this->handles[] = $handle;

        $pool->addConnection(new PgSqlConnection($handle, $socket));

        return $pool;
    }

    public function tearDown() {
        foreach ($this->handles as $handle) {
            \pg_get_result($handle); // Consume any leftover results from test.
        }

        \pg_query($this->handles[0], "ROLLBACK");
        \pg_query($this->handles[0], "DROP TABLE test");
    }
}
