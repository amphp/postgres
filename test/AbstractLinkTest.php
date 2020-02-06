<?php

namespace Amp\Postgres\Test;

use Amp\Coroutine;
use Amp\Delayed;
use Amp\Iterator;
use Amp\Loop;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\Link;
use Amp\Postgres\Listener;
use Amp\Postgres\QueryExecutionError;
use Amp\Postgres\ResultSet;
use Amp\Postgres\Transaction;
use Amp\Promise;
use Amp\Sql\CommandResult;
use Amp\Sql\QueryError;
use Amp\Sql\Statement;
use Amp\Sql\Transaction as SqlTransaction;
use Amp\Sql\TransactionError;

abstract class AbstractLinkTest extends AsyncTestCase
{
    /** @var \Amp\Postgres\Connection */
    protected $connection;

    /**
     * @return array Start test data for database.
     */
    public function getData(): array
    {
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
     * @return \Amp\Postgres\Link Connection or Link object to be tested.
     */
    abstract public function createLink(string $connectionString): Link;

    public function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->createLink('host=localhost user=postgres');
    }

    public function testQueryWithTupleResult(): \Generator
    {
        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $this->connection->query("SELECT * FROM test");

        $this->assertInstanceOf(ResultSet::class, $result);

        $this->assertSame(2, $result->getFieldCount());

        $data = $this->getData();

        for ($i = 0; yield $result->advance(); ++$i) {
            $row = $result->getCurrent();
            $this->assertSame($data[$i][0], $row['domain']);
            $this->assertSame($data[$i][1], $row['tld']);
        }
    }

    public function testQueryWithUnconsumedTupleResult(): \Generator
    {
        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $this->connection->query("SELECT * FROM test");

        $this->assertInstanceOf(ResultSet::class, $result);

        unset($result); // Force destruction of result object.

        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $this->connection->query("SELECT * FROM test");

        $this->assertInstanceOf(ResultSet::class, $result);

        $data = $this->getData();

        for ($i = 0; yield $result->advance(); ++$i) {
            $row = $result->getCurrent();
            $this->assertSame($data[$i][0], $row['domain']);
            $this->assertSame($data[$i][1], $row['tld']);
        }
    }

    public function testQueryWithCommandResult(): \Generator
    {
        /** @var CommandResult $result */
        $result = yield $this->connection->query("INSERT INTO test VALUES ('canon', 'jp')");

        $this->assertInstanceOf(CommandResult::class, $result);
        $this->assertSame(1, $result->getAffectedRowCount());
    }

    public function testQueryWithEmptyQuery(): Promise
    {
        $this->expectException(QueryError::class);

        /** @var \Amp\Sql\CommandResult $result */
        return $this->connection->query('');
    }

    public function testQueryWithSyntaxError(): \Generator
    {
        /** @var \Amp\Sql\CommandResult $result */
        try {
            $result = yield $this->connection->query("SELECT & FROM test");
            $this->fail(\sprintf("An instance of %s was expected to be thrown", QueryExecutionError::class));
        } catch (QueryExecutionError $exception) {
            $diagnostics  = $exception->getDiagnostics();
            $this->assertArrayHasKey("sqlstate", $diagnostics);
        }
    }

    public function testPrepare(): \Generator
    {
        $query = "SELECT * FROM test WHERE domain=\$1";

        /** @var Statement $statement */
        $statement = yield $this->connection->prepare($query);

        $this->assertSame($query, $statement->getQuery());

        $data = $this->getData()[0];

        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $statement->execute([$data[0]]);

        $this->assertInstanceOf(ResultSet::class, $result);

        $this->assertSame(2, $result->getFieldCount());

        while (yield $result->advance()) {
            $row = $result->getCurrent();
            $this->assertSame($data[0], $row['domain']);
            $this->assertSame($data[1], $row['tld']);
        }
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareWithNamedParams(): \Generator
    {
        $query = "SELECT * FROM test WHERE domain=:domain AND tld=:tld";

        /** @var Statement $statement */
        $statement = yield $this->connection->prepare($query);

        $data = $this->getData()[0];

        $this->assertSame($query, $statement->getQuery());

        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $statement->execute(['domain' => $data[0], 'tld' => $data[1]]);

        $this->assertInstanceOf(ResultSet::class, $result);

        $this->assertSame(2, $result->getFieldCount());

        while (yield $result->advance()) {
            $row = $result->getCurrent();
            $this->assertSame($data[0], $row['domain']);
            $this->assertSame($data[1], $row['tld']);
        }
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareWithUnnamedParams(): \Generator
    {
        $query = "SELECT * FROM test WHERE domain=? AND tld=?";

        /** @var Statement $statement */
        $statement = yield $this->connection->prepare($query);

        $data = $this->getData()[0];

        $this->assertSame($query, $statement->getQuery());

        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $statement->execute([$data[0], $data[1]]);

        $this->assertInstanceOf(ResultSet::class, $result);

        $this->assertSame(2, $result->getFieldCount());

        while (yield $result->advance()) {
            $row = $result->getCurrent();
            $this->assertSame($data[0], $row['domain']);
            $this->assertSame($data[1], $row['tld']);
        }
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareWithNamedParamsWithDataAppearingAsNamedParam(): \Generator
    {
        $query = "SELECT * FROM test WHERE domain=:domain OR domain=':domain'";

        /** @var Statement $statement */
        $statement = yield $this->connection->prepare($query);

        $data = $this->getData()[0];

        $this->assertSame($query, $statement->getQuery());

        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $statement->execute(['domain' => $data[0]]);

        $this->assertInstanceOf(ResultSet::class, $result);

        $this->assertSame(2, $result->getFieldCount());

        while (yield $result->advance()) {
            $row = $result->getCurrent();
            $this->assertSame($data[0], $row['domain']);
            $this->assertSame($data[1], $row['tld']);
        }
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareInvalidQuery(): Promise
    {
        $this->expectException(QueryExecutionError::class);
        $this->expectExceptionMessage('column "invalid" does not exist');

        $query = "SELECT * FROM test WHERE invalid=\$1";

        /** @var Statement $statement */
        return $this->connection->prepare($query);
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareSameQuery(): \Generator
    {
        $sql = "SELECT * FROM test WHERE domain=\$1";

        /** @var Statement $statement1 */
        $statement1 = yield $this->connection->prepare($sql);

        /** @var Statement $statement2 */
        $statement2 = yield $this->connection->prepare($sql);

        $this->assertInstanceOf(Statement::class, $statement1);
        $this->assertInstanceOf(Statement::class, $statement2);

        unset($statement1);

        $data = $this->getData()[0];

        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $statement2->execute([$data[0]]);

        $this->assertInstanceOf(ResultSet::class, $result);

        $this->assertSame(2, $result->getFieldCount());

        while (yield $result->advance()) {
            $row = $result->getCurrent();
            $this->assertSame($data[0], $row['domain']);
            $this->assertSame($data[1], $row['tld']);
        }
    }

    /**
     * @depends testPrepareSameQuery
     */
    public function testSimultaneousPrepareSameQuery(): \Generator
    {
        $sql = "SELECT * FROM test WHERE domain=\$1";

        $statement1 = $this->connection->prepare($sql);
        $statement2 = $this->connection->prepare($sql);

        /**
         * @var Statement $statement1
         * @var Statement $statement2
         */
        list($statement1, $statement2) = yield [$statement1, $statement2];

        $this->assertInstanceOf(Statement::class, $statement1);
        $this->assertInstanceOf(Statement::class, $statement2);

        $data = $this->getData()[0];

        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $statement1->execute([$data[0]]);

        $this->assertInstanceOf(ResultSet::class, $result);

        $this->assertSame(2, $result->getFieldCount());

        while (yield $result->advance()) {
            $row = $result->getCurrent();
            $this->assertSame($data[0], $row['domain']);
            $this->assertSame($data[1], $row['tld']);
        }

        unset($statement1);

        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $statement2->execute([$data[0]]);

        $this->assertInstanceOf(ResultSet::class, $result);

        $this->assertSame(2, $result->getFieldCount());

        while (yield $result->advance()) {
            $row = $result->getCurrent();
            $this->assertSame($data[0], $row['domain']);
            $this->assertSame($data[1], $row['tld']);
        }
    }

    public function testPrepareSimilarQueryReturnsDifferentStatements(): \Generator
    {
        /** @var Statement $statement1 */
        $statement1 = $this->connection->prepare("SELECT * FROM test WHERE domain=\$1");

        /** @var Statement $statement2 */
        $statement2 = $this->connection->prepare("SELECT * FROM test WHERE domain=:domain");

        list($statement1, $statement2) = yield [$statement1, $statement2];

        $this->assertInstanceOf(Statement::class, $statement1);
        $this->assertInstanceOf(Statement::class, $statement2);

        $this->assertNotSame($statement1, $statement2);

        $data = $this->getData()[0];

        $results = [];

        $results[] = yield Iterator\toArray(yield $statement1->execute([$data[0]]));
        $results[] = yield Iterator\toArray(yield $statement2->execute(['domain' => $data[0]]));

        foreach ($results as $result) {
            /** @var \Amp\Postgres\ResultSet $result */
            foreach ($result as $row) {
                $this->assertSame($data[0], $row['domain']);
                $this->assertSame($data[1], $row['tld']);
            }
        }
    }

    public function testPrepareThenExecuteWithUnconsumedTupleResult(): \Generator
    {
        /** @var Statement $statement */
        $statement = yield $this->connection->prepare("SELECT * FROM test");

        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $statement->execute();

        $this->assertInstanceOf(ResultSet::class, $result);

        unset($result); // Force destruction of result object.

        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $statement->execute();

        $this->assertInstanceOf(ResultSet::class, $result);

        $data = $this->getData();

        for ($i = 0; yield $result->advance(); ++$i) {
            $row = $result->getCurrent();
            $this->assertSame($data[$i][0], $row['domain']);
            $this->assertSame($data[$i][1], $row['tld']);
        }
    }

    public function testExecute(): \Generator
    {
        $data = $this->getData()[0];

        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $this->connection->execute("SELECT * FROM test WHERE domain=\$1", [$data[0]]);

        $this->assertInstanceOf(ResultSet::class, $result);

        $this->assertSame(2, $result->getFieldCount());

        while (yield $result->advance()) {
            $row = $result->getCurrent();
            $this->assertSame($data[0], $row['domain']);
            $this->assertSame($data[1], $row['tld']);
        }
    }

    /**
     * @depends testExecute
     */
    public function testExecuteWithNamedParams(): \Generator
    {
        $data = $this->getData()[0];

        /** @var \Amp\Postgres\ResultSet $result */
        $result = yield $this->connection->execute(
            "SELECT * FROM test WHERE domain=:domain",
            ['domain' =>  $data[0]]
        );

        $this->assertInstanceOf(ResultSet::class, $result);

        $this->assertSame(2, $result->getFieldCount());

        while (yield $result->advance()) {
            $row = $result->getCurrent();
            $this->assertSame($data[0], $row['domain']);
            $this->assertSame($data[1], $row['tld']);
        }
    }
    /**
     * @depends testExecute
     */
    public function testExecuteWithInvalidParams(): Promise
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage("Value for unnamed parameter at position 0 missing");

        return $this->connection->execute("SELECT * FROM test WHERE domain=\$1");
    }

    /**
     * @depends testExecute
     */
    public function testExecuteWithInvalidNamedParams(): Promise
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage("Value for named parameter 'domain' missing");

        return $this->connection->execute("SELECT * FROM test WHERE domain=:domain", ['tld' => 'com']);
    }

    /**
     * @depends testQueryWithTupleResult
     */
    public function testSimultaneousQuery(): Promise
    {
        $callback = \Amp\coroutine(function ($value) {
            /** @var \Amp\Postgres\ResultSet $result */
            $result = yield $this->connection->query("SELECT {$value} as value");

            if ($value) {
                yield new Delayed(100);
            }

            while (yield $result->advance()) {
                $row = $result->getCurrent();
                $this->assertEquals($value, $row['value']);
            }
        });

        return Promise\all([$callback(0), $callback(1)]);
    }

    /**
     * @depends testSimultaneousQuery
     */
    public function testSimultaneousQueryWithOneFailing(): \Generator
    {
        $callback = \Amp\coroutine(function ($query) {
            /** @var \Amp\Postgres\ResultSet $result */
            $result = yield $this->connection->query($query);

            $data = $this->getData();

            for ($i = 0; yield $result->advance(); ++$i) {
                $row = $result->getCurrent();
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
            }

            return $result;
        });

        $result = null;

        try {
            $successful = $callback("SELECT * FROM test");
            $failing = $callback("SELECT & FROM test");

            $result = yield $successful;
            yield $failing;
        } catch (QueryError $exception) {
            $this->assertInstanceOf(ResultSet::class, $result);
            return;
        }

        $this->fail(\sprintf("Test did not throw an instance of %s", QueryError::class));
    }

    public function testSimultaneousQueryAndPrepare(): Promise
    {
        $promises = [];
        $promises[] = new Coroutine((function () {
            /** @var \Amp\Postgres\ResultSet $result */
            $result = yield $this->connection->query("SELECT * FROM test");

            $data = $this->getData();

            for ($i = 0; yield $result->advance(); ++$i) {
                $row = $result->getCurrent();
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
            }
        })());

        $promises[] = new Coroutine((function () {
            /** @var Statement $statement */
            $statement = (yield $this->connection->prepare("SELECT * FROM test"));

            /** @var \Amp\Postgres\ResultSet $result */
            $result = yield $statement->execute();

            $data = $this->getData();

            for ($i = 0; yield $result->advance(); ++$i) {
                $row = $result->getCurrent();
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
            }
        })());

        return Promise\all($promises);
    }

    public function testSimultaneousPrepareAndExecute(): Promise
    {
        $promises[] = new Coroutine((function () {
            /** @var Statement $statement */
            $statement = yield $this->connection->prepare("SELECT * FROM test");

            /** @var \Amp\Postgres\ResultSet $result */
            $result = yield $statement->execute();

            $data = $this->getData();

            for ($i = 0; yield $result->advance(); ++$i) {
                $row = $result->getCurrent();
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
            }
        })());

        $promises[] = new Coroutine((function () {
            /** @var \Amp\Postgres\ResultSet $result */
            $result = yield $this->connection->execute("SELECT * FROM test");

            $data = $this->getData();

            for ($i = 0; yield $result->advance(); ++$i) {
                $row = $result->getCurrent();
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
            }
        })());

        return Promise\all($promises);
    }

    public function testTransaction(): \Generator
    {
        $isolation = SqlTransaction::ISOLATION_COMMITTED;

        /** @var \Amp\Postgres\Transaction $transaction */
        $transaction = yield $this->connection->beginTransaction($isolation);

        $this->assertInstanceOf(Transaction::class, $transaction);

        $data = $this->getData()[0];

        $this->assertTrue($transaction->isAlive());
        $this->assertTrue($transaction->isActive());
        $this->assertSame($isolation, $transaction->getIsolationLevel());

        yield $transaction->createSavepoint('test');

        $statement = yield $transaction->prepare("SELECT * FROM test WHERE domain=:domain");
        $result = yield $statement->execute(['domain' => $data[0]]);

        $this->assertInstanceOf(ResultSet::class, $result);

        unset($result); // Force destruction of result object.

        $result = yield $transaction->execute("SELECT * FROM test WHERE domain=\$1 FOR UPDATE", [$data[0]]);

        $this->assertInstanceOf(ResultSet::class, $result);

        unset($result); // Force destruction of result object.

        yield $transaction->rollbackTo('test');

        yield $transaction->commit();

        $this->assertFalse($transaction->isAlive());
        $this->assertFalse($transaction->isActive());

        try {
            $result = yield $transaction->execute("SELECT * FROM test");
            $this->fail('Query should fail after transaction commit');
        } catch (TransactionError $exception) {
            // Exception expected.
        }
    }

    public function testListen(): \Generator
    {
        $channel = "test";
        /** @var \Amp\Postgres\Listener $listener */
        $listener = yield $this->connection->listen($channel);

        $this->assertInstanceOf(Listener::class, $listener);
        $this->assertSame($channel, $listener->getChannel());

        Loop::delay(100, function () use ($channel) {
            yield $this->connection->query(\sprintf("NOTIFY %s, '%s'", $channel, '0'));
            yield $this->connection->query(\sprintf("NOTIFY %s, '%s'", $channel, '1'));
        });

        $count = 0;
        Loop::delay(200, function () use ($listener) {
            $listener->unlisten();
        });

        while (yield $listener->advance()) {
            $this->assertSame($listener->getCurrent()->payload, (string) $count++);
        }

        $this->assertSame(2, $count);
    }

    /**
     * @depends testListen
     */
    public function testNotify(): \Generator
    {
        $channel = "test";
        /** @var \Amp\Postgres\Listener $listener */
        $listener = yield $this->connection->listen($channel);

        Loop::delay(100, function () use ($channel) {
            yield $this->connection->notify($channel, '0');
            yield $this->connection->notify($channel, '1');
        });

        $count = 0;
        Loop::delay(200, function () use ($listener) {
            $listener->unlisten();
        });

        while (yield $listener->advance()) {
            $this->assertSame($listener->getCurrent()->payload, (string) $count++);
        }

        $this->assertSame(2, $count);
    }

    /**
     * @depends testListen
     */
    public function testListenOnSameChannel(): Promise
    {
        $this->expectException(QueryError::class);
        $this->expectExceptionMessage('Already listening on channel');

        $channel = "test";
        return Promise\all([$this->connection->listen($channel), $this->connection->listen($channel)]);
    }
}
