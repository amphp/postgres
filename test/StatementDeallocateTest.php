<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\PgSqlConnection;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;
use Amp\Postgres\PqConnection;
use Amp\Sql\SqlConnector;
use Revolt\EventLoop;

class StatementDeallocateTest extends AsyncTestCase
{
    public function provideConnectors(): \Generator
    {
        $connector = $this->createMock(SqlConnector::class);
        $connector
            ->method('connect')
            ->willReturnCallback(function (PostgresConfig $config): PgSqlConnection {
                if (!\extension_loaded('pgsql')) {
                    self::markTestSkipped('requires pgsql extension');
                }

                if (EventLoop::getDriver()->getHandle() instanceof \EvLoop) {
                    self::markTestSkipped('ext-pgsql is not compatible with pecl-ev');
                }

                return PgSqlConnection::connect($config);
            });

        yield 'ext-pgsql' => [$connector];

        $connector = $this->createMock(SqlConnector::class);
        $connector
            ->method('connect')
            ->willReturnCallback(function (PostgresConfig $config): PqConnection {
                if (!\extension_loaded('pq')) {
                    self::markTestSkipped('requires pq extension');
                }

                return PqConnection::connect($config);
            });

        yield 'ext-pq' => [$connector];
    }

    /**
     * @dataProvider provideConnectors
     */
    public function testDeallocateRacingConnectionResetDoesNotBreakSubsequentOperations(SqlConnector $connector): void
    {
        $this->setTimeout(5);

        $pool = new PostgresConnectionPool(
            PostgresConfig::fromString('host=localhost user=postgres password=postgres'),
            maxConnections: 1,
            connector: $connector,
        );

        try {
            $statement = $pool->prepare('SELECT 1 AS value');
            $result = $statement->execute();
            \iterator_to_array($result);

            // Dropping the statement and result queues a DEALLOCATE for the prepared
            // statement, while the pool's DISCARD ALL reset on the next checkout drops
            // it server-side. The failing DEALLOCATE must not consume responses that
            // belong to operations dispatched after it.
            unset($result, $statement);

            for ($i = 1; $i <= 3; ++$i) {
                $rows = \iterator_to_array($pool->query(\sprintf('SELECT %d AS value', $i)));
                self::assertSame($i, $rows[0]['value']);
            }

            $statement = $pool->prepare('SELECT 2 AS value');
            $rows = \iterator_to_array($statement->execute());
            self::assertSame(2, $rows[0]['value']);
        } finally {
            $pool->close();
        }
    }
}
