<?php

namespace Amp\Postgres\Test;

use Amp\Loop;
use Amp\Postgres\Link;
use Amp\Postgres\PgSqlConnection;

/**
 * @requires extension pgsql
 */
class PgSqlConnectionTest extends AbstractConnectionTest
{
    /** @var resource PostgreSQL connection resource. */
    protected $handle;

    public function createLink(string $connectionString): Link
    {
        if (Loop::get()->getHandle() instanceof \EvLoop) {
            $this->markTestSkipped("ext-pgsql is not compatible with pecl-ev");
        }

        $this->handle = \pg_connect($connectionString);
        $socket = \pg_socket($this->handle);

        \pg_query($this->handle, "DROP TABLE IF EXISTS test");

        $result = \pg_query($this->handle, "CREATE TABLE test (domain VARCHAR(63), tld VARCHAR(63), PRIMARY KEY (domain, tld))");

        if (!$result) {
            $this->fail('Could not create test table.');
        }

        foreach ($this->getData() as $row) {
            $result = \pg_query_params($this->handle, "INSERT INTO test VALUES (\$1, \$2)", $row);

            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }

        return new PgSqlConnection($this->handle, $socket);
    }

    public function tearDown(): void
    {
        \pg_get_result($this->handle); // Consume any leftover results from test.
        \pg_query($this->handle, "ROLLBACK");
        \pg_query($this->handle, "DROP TABLE test");
        \pg_close($this->handle);
    }
}
