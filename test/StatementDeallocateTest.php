<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;
use Revolt\EventLoop;

/**
 * @requires extension pgsql
 */
class StatementDeallocateTest extends AsyncTestCase
{
    public function testDeallocateRacingConnectionResetDoesNotBreakSubsequentOperations(): void
    {
        if (EventLoop::getDriver()->getHandle() instanceof \EvLoop) {
            $this->markTestSkipped("ext-pgsql is not compatible with pecl-ev");
        }

        $this->setTimeout(5);

        $pool = new PostgresConnectionPool(
            PostgresConfig::fromString('host=localhost user=postgres password=postgres'),
            1,
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
