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
use function Amp\delay;

abstract class AbstractConnectionTest extends AbstractLinkTest
{
    public function testIsClosed()
    {
        $this->assertFalse($this->executor->isClosed());
    }

    public function testConnectionCloseDuringQuery(): void
    {
        $query = async($this->executor->execute(...), 'SELECT pg_sleep(10)');
        $close = async($this->executor->close(...));

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
        $listener = $this->executor->listen($channel);

        $this->assertInstanceOf(PostgresListener::class, $listener);
        $this->assertSame($channel, $listener->getChannel());

        EventLoop::delay(0.1, function () use ($channel): void {
            $this->executor->query(\sprintf("NOTIFY %s, '%s'", $channel, '0'));
            $this->executor->query(\sprintf("NOTIFY %s, '%s'", $channel, '1'));
        });

        $count = 0;
        EventLoop::delay(0.2, fn () => $listener->unlisten());

        /** @var PostgresNotification $notification */
        foreach ($listener as $notification) {
            $this->assertSame($notification->payload, (string) $count++);
        }

        $this->assertSame(2, $count);
    }

    /**
     * @depends testListen
     */
    public function testNotify()
    {
        $channel = "test";
        $listener = $this->executor->listen($channel);

        EventLoop::delay(0.1, function () use ($channel) {
            $this->executor->notify($channel, '0');
            $this->executor->notify($channel, '1');
        });

        $count = 0;
        EventLoop::delay(0.2, fn () => $listener->unlisten());

        /** @var PostgresNotification $notification */
        foreach ($listener as $notification) {
            $this->assertSame($notification->payload, (string) $count++);
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
        Future\await([$this->executor->listen($channel), $this->executor->listen($channel)]);
    }

    public function testQueryAfterErroredQuery()
    {
        try {
            $result = $this->executor->query("INSERT INTO test VALUES ('github', 'com', '{1, 2, 3}', true, 4.2)");
        } catch (QueryExecutionError $exception) {
            // Expected exception due to duplicate key.
        }

        $result = $this->executor->query("INSERT INTO test VALUES ('gitlab', 'com', '{1, 2, 3}', true, 4.2)");

        $this->assertSame(1, $result->getRowCount());
    }

    public function testTransactionsCallbacksOnCommit(): void
    {
        $transaction = $this->executor->beginTransaction();
        $transaction->onCommit($this->createCallback(1));
        $transaction->onRollback($this->createCallback(0));
        $transaction->onClose($this->createCallback(1));

        $transaction->commit();
    }

    public function testTransactionsCallbacksOnRollback(): void
    {
        $transaction = $this->executor->beginTransaction();
        $transaction->onCommit($this->createCallback(0));
        $transaction->onRollback($this->createCallback(1));
        $transaction->onClose($this->createCallback(1));

        $transaction->rollback();
    }

    public function testTransactionsCallbacksOnDestruct(): void
    {
        $transaction = $this->executor->beginTransaction();
        $transaction->onCommit($this->createCallback(0));
        $transaction->onRollback($this->createCallback(1));
        $transaction->onClose($this->createCallback(1));

        unset($transaction);
        delay(0.1); // Destructor is async, so give control to the loop to invoke callbacks.
    }
}
