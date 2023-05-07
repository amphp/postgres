<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Future;
use Amp\Postgres\PostgresListener;
use Amp\Postgres\PostgresNotification;
use Amp\Postgres\QueryExecutionError;
use Amp\Sql\ConnectionException;
use Amp\Sql\QueryError;
use Amp\Sql\SqlException;
use Revolt\EventLoop;
use function Amp\async;

abstract class AbstractConnectionTest extends AbstractLinkTest
{
    public function testIsClosed()
    {
        $this->assertFalse($this->link->isClosed());
    }

    public function testConnectionCloseDuringQuery(): void
    {
        $query = async($this->link->execute(...), 'SELECT pg_sleep(10)');
        $close = async($this->link->close(...));

        $start = \microtime(true);

        $close->await();

        try {
            $query->await();
            self::fail(\sprintf('Expected %s to be thrown', ConnectionException::class));
        } catch (SqlException) {
            // Expected
        }

        $this->assertLessThan(0.1, \microtime(true) - $start);
    }

    public function testListen()
    {
        $channel = "test";
        $listener = $this->link->listen($channel);

        $this->assertInstanceOf(PostgresListener::class, $listener);
        $this->assertSame($channel, $listener->getChannel());

        EventLoop::delay(0.1, function () use ($channel): void {
            $this->link->query(\sprintf("NOTIFY %s, '%s'", $channel, '0'));
            $this->link->query(\sprintf("NOTIFY %s, '%s'", $channel, '1'));
        });

        $count = 0;
        EventLoop::delay(0.2, fn () => $listener->unlisten());

        /** @var PostgresNotification $notification */
        foreach ($listener as $notification) {
            $this->assertSame($notification->getPayload(), (string) $count++);
        }

        $this->assertSame(2, $count);
    }

    /**
     * @depends testListen
     */
    public function testNotify()
    {
        $channel = "test";
        $listener = $this->link->listen($channel);

        EventLoop::delay(0.1, function () use ($channel) {
            $this->link->notify($channel, '0');
            $this->link->notify($channel, '1');
        });

        $count = 0;
        EventLoop::delay(0.2, fn () => $listener->unlisten());

        /** @var PostgresNotification $notification */
        foreach ($listener as $notification) {
            $this->assertSame($notification->getPayload(), (string) $count++);
        }

        $this->assertSame(2, $count);
    }

    /**
     * @depends testListen
     */
    public function testListenOnSameChannel()
    {
        $this->expectException(QueryError::class);
        $this->expectExceptionMessage('Already listening on channel');

        $channel = "test";
        Future\await([$this->link->listen($channel), $this->link->listen($channel)]);
    }

    public function testQueryAfterErroredQuery()
    {
        try {
            $result = $this->link->query("INSERT INTO test VALUES ('github', 'com', '{1, 2, 3}', true, 4.2)");
        } catch (QueryExecutionError $exception) {
            // Expected exception due to duplicate key.
        }

        $result = $this->link->query("INSERT INTO test VALUES ('gitlab', 'com', '{1, 2, 3}', true, 4.2)");

        $this->assertSame(1, $result->getRowCount());
    }
}
