<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Postgres\PgSqlConnection;
use Amp\Postgres\PostgresByteA;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;
use Amp\Postgres\PostgresLink;
use Amp\Sql\Common\SqlCommonConnectionPool;
use Amp\Sql\SqlConnector;
use Revolt\EventLoop;
use function Amp\Postgres\Internal\cast;

/**
 * @requires extension pgsql
 */
class PgSqlPoolTest extends AbstractConnectionTest
{
    const POOL_SIZE = 3;

    /** @var \PgSql\Connection[] PostgreSQL connection resources. */
    protected array $handles = [];

    public function createLink(string $connectionString): PostgresLink
    {
        if (EventLoop::getDriver()->getHandle() instanceof \EvLoop) {
            $this->markTestSkipped("ext-pgsql is not compatible with pecl-ev");
        }

        for ($i = 0; $i < self::POOL_SIZE; ++$i) {
            $this->handles[] = \pg_connect($connectionString, \PGSQL_CONNECT_FORCE_NEW);
        }

        $config = PostgresConfig::fromString($connectionString);

        $connector = $this->createMock(SqlConnector::class);
        $connector->method('connect')
            ->willReturnCallback(function () use ($config): PgSqlConnection {
                static $count = 0;
                if (!isset($this->handles[$count])) {
                    $this->fail("createConnection called too many times");
                }
                $handle = $this->handles[$count];
                ++$count;
                return $this->newConnection(
                    PgSqlConnection::class,
                    $handle,
                    \pg_socket($handle),
                    'mock-connection',
                    $config,
                );
            });

        $pool = new PostgresConnectionPool(new PostgresConfig('localhost'), \count($this->handles), SqlCommonConnectionPool::DEFAULT_IDLE_TIMEOUT, true, $connector);

        $handle = \reset($this->handles);

        \pg_query($handle, self::DROP_QUERY);

        $result = \pg_query($handle, self::CREATE_QUERY);

        if (!$result) {
            $this->fail('Could not create test table.');
        }

        foreach ($this->getParams() as $row) {
            $result = \pg_query_params($handle, self::INSERT_QUERY, \array_map(fn ($data) => $this->cast($handle, $data), $row));

            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }

        return $pool;
    }

    private function cast(\PgSql\Connection $handle, mixed $param): mixed
    {
        return $param instanceof PostgresByteA ? \pg_escape_bytea($handle, $param->getData()) : cast($param);
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
