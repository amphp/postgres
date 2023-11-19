<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Postgres\ByteA;
use Amp\Postgres\PgSqlConnection;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresLink;
use Revolt\EventLoop;
use function Amp\Postgres\Internal\cast;

/**
 * @requires extension pgsql
 */
class PgSqlConnectionTest extends AbstractConnectionTest
{
    protected ?\PgSql\Connection $handle = null;

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

        foreach ($this->getParams() as $row) {
            $result = \pg_query_params($this->handle, self::INSERT_QUERY, \array_map($this->cast(...), $row));
            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }

        return $this->newConnection(
            PgSqlConnection::class,
            $this->handle,
            $socket,
            'mock-connection',
            PostgresConfig::fromString($connectionString),
        );
    }

    private function cast(mixed $param): mixed
    {
        return $param instanceof ByteA ? \pg_escape_bytea($this->handle, $param->getData()) : cast($param);
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
