<?php

namespace Amp\Postgres\Test;

use Amp\Loop;
use Amp\Postgres\{ CommandResult, Connection, Listener, Transaction, TransactionError, TupleResult };
use PHPUnit\Framework\TestCase;

abstract class AbstractConnectionTest extends TestCase {
    /** @var \Amp\Postgres\Connection */
    protected $connection;

    /**
     * @return array Start test data for database.
     */
    public function getData() {
        return [
            ['amphp', 'org'],
            ['github', 'com'],
            ['google', 'com'],
            ['php', 'net'],
        ];
    }

    /**
     * @param string $connectionString
     *
     * @return \Amp\Postgres\Connection Connection object to be tested.
     */
    abstract public function createConnection(string $connectionString): Connection;

    public function setUp() {
        $this->connection = $this->createConnection('host=localhost user=postgres');
    }

    public function testQueryWithTupleResult() {
        Loop::run(function () {
            /** @var \Amp\Postgres\TupleResult $result */
            $result = yield $this->connection->query("SELECT * FROM test");

            $this->assertInstanceOf(TupleResult::class, $result);

            $this->assertSame(2, $result->numFields());

            $data = $this->getData();

            for ($i = 0; yield $result->advance(); ++$i) {
                $row = $result->getCurrent();
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
            }
        });
    }

    public function testQueryWithCommandResult() {
        Loop::run(function () {
            /** @var \Amp\Postgres\CommandResult $result */
            $result = yield $this->connection->query("INSERT INTO test VALUES ('canon', 'jp')");

            $this->assertInstanceOf(CommandResult::class, $result);
            $this->assertSame(1, $result->affectedRows());
        });
    }

    /**
     * @expectedException \Amp\Postgres\QueryError
     */
    public function testQueryWithEmptyQuery() {
        Loop::run(function () {
            /** @var \Amp\Postgres\CommandResult $result */
            $result = yield $this->connection->query('');
        });
    }

    /**
     * @expectedException \Amp\Postgres\QueryError
     */
    public function testQueryWithSyntaxError() {
        Loop::run(function () {
            /** @var \Amp\Postgres\CommandResult $result */
            $result = yield $this->connection->query("SELECT & FROM test");
        });
    }

    public function testPrepare() {
        Loop::run(function () {
            $query = "SELECT * FROM test WHERE domain=\$1";

            /** @var \Amp\Postgres\Statement $statement */
            $statement = yield $this->connection->prepare($query);

            $this->assertSame($query, $statement->getQuery());

            $data = $this->getData()[0];

            /** @var \Amp\Postgres\TupleResult $result */
            $result = yield $statement->execute($data[0]);

            $this->assertInstanceOf(TupleResult::class, $result);

            $this->assertSame(2, $result->numFields());

            while (yield $result->advance()) {
                $row = $result->getCurrent();
                $this->assertSame($data[0], $row['domain']);
                $this->assertSame($data[1], $row['tld']);
            }
        });
    }

    public function testExecute() {
        Loop::run(function () {
            $data = $this->getData()[0];

            /** @var \Amp\Postgres\TupleResult $result */
            $result = yield $this->connection->execute("SELECT * FROM test WHERE domain=\$1", $data[0]);

            $this->assertInstanceOf(TupleResult::class, $result);

            $this->assertSame(2, $result->numFields());

            while (yield $result->advance()) {
                $row = $result->getCurrent();
                $this->assertSame($data[0], $row['domain']);
                $this->assertSame($data[1], $row['tld']);
            }
        });
    }

    /**
     * @expectedException \Amp\Postgres\PendingOperationError
     */
    public function testSimultaneousQuery() {
        Loop::run(function () {
            $query1 = $this->connection->query("SELECT 0 as value");
            $query2 = $this->connection->query("SELECT 1 as value");

            try {
                yield $query1;
            } catch (\Throwable $exception) {
                $this->fail("The first query should be successful");
            }

            yield $query2;
        });
    }

    public function testTransaction() {
        Loop::run(function () {
            $isolation = Transaction::COMMITTED;

            /** @var \Amp\Postgres\Transaction $transaction */
            $transaction = yield $this->connection->transaction($isolation);

            $this->assertInstanceOf(Transaction::class, $transaction);

            $data = $this->getData()[0];

            $this->assertTrue($transaction->isActive());
            $this->assertSame($isolation, $transaction->getIsolationLevel());

            yield $transaction->savepoint('test');

            $result = yield $transaction->execute("SELECT * FROM test WHERE domain=\$1 FOR UPDATE", $data[0]);

            yield $transaction->rollbackTo('test');

            yield $transaction->commit();

            $this->assertFalse($transaction->isActive());

            try {
                $result = yield $transaction->execute("SELECT * FROM test");
                $this->fail('Query should fail after transaction commit');
            } catch (TransactionError $exception) {
                // Exception expected.
            }
        });
    }

    public function testListen() {
        Loop::run(function () {
            $channel = "test";
            /** @var \Amp\Postgres\Listener $listener */
            $listener = yield $this->connection->listen($channel);

            $this->assertInstanceOf(Listener::class, $listener);

            yield $this->connection->query(\sprintf("NOTIFY %s, '%s'", $channel, '0'));
            yield $this->connection->query(\sprintf("NOTIFY %s, '%s'", $channel, '1'));

            $count = 0;
            Loop::defer(function () use (&$count, $listener) {
                $listener->unlisten();
                $this->assertSame(2, $count);
            });

            while (yield $listener->advance()) {
                $this->assertSame($listener->getCurrent()->payload, (string) $count++);
            }
        });
    }

    /**
     * @depends testListen
     */
    public function testNotify() {
        Loop::run(function () {
            $channel = "test";
            /** @var \Amp\Postgres\Listener $listener */
            $listener = yield $this->connection->listen($channel);

            yield $this->connection->notify($channel, '0');
            yield $this->connection->notify($channel, '1');

            $count = 0;
            Loop::defer(function () use (&$count, $listener) {
                $listener->unlisten();
                $this->assertSame(2, $count);
            });

            while (yield $listener->advance()) {
                $this->assertSame($listener->getCurrent()->payload, (string) $count++);
            }
        });
    }

    /**
     * @depends testListen
     * @expectedException \Amp\Postgres\QueryError
     * @expectedExceptionMessage Already listening on channel
     */
    public function testListenOnSameChannel() {
        Loop::run(function () {
            $channel = "test";
            $listener = yield $this->connection->listen($channel);
            $listener = yield $this->connection->listen($channel);
        });
    }
}
