<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Postgres\PgSqlConnection;
use Amp\Postgres\PostgresLink;
use Revolt\EventLoop;
use function Amp\Postgres\Internal\cast;

/**
 * @requires extension pgsql
 */
class PgSqlConnectionTest extends AbstractConnectionTest
{
    /** @var resource PostgreSQL connection resource. */
    protected $handle;

    public function createLink(string $connectionString): PostgresLink
    {
        if (EventLoop::getDriver()->getHandle() instanceof \EvLoop) {
            $this->markTestSkipped("ext-pgsql is not compatible with pecl-ev");
        }

        $this->handle = \pg_connect($connectionString);
        $socket = \pg_socket($this->handle);

        \pg_query($this->handle, self::DROP_QUERY);

        $result = \pg_query($this->handle, self::CREATE_QUERY);

        if (!$result) {
            $this->fail('Could not create test table.');
        }

        foreach ($this->getData() as $row) {
            $result = \pg_query_params($this->handle, self::INSERT_QUERY, \array_map(cast(...), $row));
            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }

        return $this->newConnection(PgsqlConnection::class, $this->handle, $socket, 'mock-connection');
    }

    public function tearDown(): void
    {
        \pg_cancel_query($this->handle); // Cancel any outstanding query.
        \pg_get_result($this->handle); // Consume any leftover results from test.
        \pg_query($this->handle, "ROLLBACK");
        \pg_query($this->handle, self::DROP_QUERY);
        \pg_close($this->handle);

        $this->handle = null;

        parent::tearDown();
    }
}
