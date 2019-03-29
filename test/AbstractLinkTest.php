<?php

namespace Amp\Postgres\Test;

use Amp\Coroutine;
use Amp\Delayed;
use Amp\Loop;
use Amp\Postgres\Link;
use Amp\Postgres\Listener;
use Amp\Postgres\QueryExecutionError;
use Amp\Postgres\ResultSet;
use Amp\Postgres\Transaction;
use Amp\Sql\CommandResult;
use Amp\Sql\QueryError;
use Amp\Sql\Statement;
use Amp\Sql\Transaction as SqlTransaction;
use Amp\Sql\TransactionError;
use PHPUnit\Framework\TestCase;

abstract class AbstractLinkTest extends TestCase
{
    /** @var \Amp\Postgres\Connection */
    protected $connection;

    /**
     * @return array Start test data for database.
     */
    public function getData()
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

    public function setUp()
    {
        $this->connection = $this->createLink('host=localhost user=postgres');
    }

    public function testQueryWithTupleResult()
    {
        Loop::run(function () {
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
        });
    }

    public function testQueryWithUnconsumedTupleResult()
    {
        Loop::run(function () {
            /** @var \Amp\Postgres\ResultSet $result */
            $result = yield $this->connection->query("SELECT * FROM test");

            $this->assertInstanceOf(ResultSet::class, $result);

            /** @var \Amp\Postgres\ResultSet $result */
            $result = yield $this->connection->query("SELECT * FROM test");

            $this->assertInstanceOf(ResultSet::class, $result);

            $data = $this->getData();

            for ($i = 0; yield $result->advance(); ++$i) {
                $row = $result->getCurrent();
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
            }
        });
    }

    public function testQueryWithCommandResult()
    {
        Loop::run(function () {
            /** @var CommandResult $result */
            $result = yield $this->connection->query("INSERT INTO test VALUES ('canon', 'jp')");

            $this->assertInstanceOf(CommandResult::class, $result);
            $this->assertSame(1, $result->getAffectedRowCount());
        });
    }

    /**
     * @expectedException \Amp\Sql\QueryError
     */
    public function testQueryWithEmptyQuery()
    {
        Loop::run(function () {
            /** @var \Amp\Sql\CommandResult $result */
            $result = yield $this->connection->query('');
        });
    }

    public function testQueryWithSyntaxError()
    {
        Loop::run(function () {
            /** @var \Amp\Sql\CommandResult $result */
            try {
                $result = yield $this->connection->query("SELECT & FROM test");
                $this->fail(\sprintf("An instance of %s was expected to be thrown", QueryExecutionError::class));
            } catch (QueryExecutionError $exception) {
                $diagnostics  = $exception->getDiagnostics();
                $this->assertArrayHasKey("sqlstate", $diagnostics);
            }
        });
    }

    public function testPrepare()
    {
        Loop::run(function () {
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
        });
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareWithNamedParams()
    {
        Loop::run(function () {
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
        });
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareWithUnnamedParams()
    {
        Loop::run(function () {
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
        });
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareWithNamedParamsWithDataAppearingAsNamedParam()
    {
        Loop::run(function () {
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
        });
    }

    /**
     * @depends testPrepare
     * @expectedException \Amp\Postgres\QueryExecutionError
     * @expectedExceptionMessage column "invalid" does not exist
     */
    public function testPrepareInvalidQuery()
    {
        Loop::run(function () {
            $query = "SELECT * FROM test WHERE invalid=\$1";

            /** @var Statement $statement */
            $statement = yield $this->connection->prepare($query);
        });
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareSameQuery()
    {
        Loop::run(function () {
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
        });
    }

    /**
     * @depends testPrepareSameQuery
     */
    public function testSimultaneousPrepareSameQuery()
    {
        Loop::run(function () {
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
        });
    }

    public function testPrepareSimilarQueryReturnsDifferentStatements()
    {
        Loop::run(function () {
            /** @var Statement $statement1 */
            $statement1 = $this->connection->prepare("SELECT * FROM test WHERE domain=\$1");

            /** @var Statement $statement2 */
            $statement2 = $this->connection->prepare("SELECT * FROM test WHERE domain=:domain");

            list($statement1, $statement2) = yield [$statement1, $statement2];

            $this->assertInstanceOf(Statement::class, $statement1);
            $this->assertInstanceOf(Statement::class, $statement2);

            $this->assertNotSame($statement1, $statement2);
        });
    }

    public function testPrepareThenExecuteWithUnconsumedTupleResult()
    {
        Loop::run(function () {
            /** @var Statement $statement */
            $statement = yield $this->connection->prepare("SELECT * FROM test");

            /** @var \Amp\Postgres\ResultSet $result */
            $result = yield $statement->execute();

            $this->assertInstanceOf(ResultSet::class, $result);

            /** @var \Amp\Postgres\ResultSet $result */
            $result = yield $statement->execute();

            $this->assertInstanceOf(ResultSet::class, $result);

            $data = $this->getData();

            for ($i = 0; yield $result->advance(); ++$i) {
                $row = $result->getCurrent();
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
            }
        });
    }

    public function testExecute()
    {
        Loop::run(function () {
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
        });
    }

    /**
     * @depends testExecute
     */
    public function testExecuteWithNamedParams()
    {
        Loop::run(function () {
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
        });
    }
    /**
     * @depends testExecute
     * @expectedException \Error
     * @expectedExceptionMessage Value for unnamed parameter at position 0 missing
     */
    public function testExecuteWithInvalidParams()
    {
        Loop::run(function () {
            $result = yield $this->connection->execute("SELECT * FROM test WHERE domain=\$1");
        });
    }

    /**
     * @depends testExecute
     * @expectedException \Error
     * @expectedExceptionMessage Value for named parameter 'domain' missing
     */
    public function testExecuteWithInvalidNamedParams()
    {
        Loop::run(function () {
            $result = yield $this->connection->execute("SELECT * FROM test WHERE domain=:domain", ['tld' => 'com']);
        });
    }

    /**
     * @depends testQueryWithTupleResult
     */
    public function testSimultaneousQuery()
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

        Loop::run(function () use ($callback) {
            yield [$callback(0), $callback(1)];
        });
    }

    /**
     * @depends testSimultaneousQuery
     */
    public function testSimultaneousQueryWithOneFailing()
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

        try {
            Loop::run(function () use (&$result, $callback) {
                $successful = $callback("SELECT * FROM test");
                $failing = $callback("SELECT & FROM test");

                $result = yield $successful;
                yield $failing;
            });
        } catch (QueryError $exception) {
            $this->assertInstanceOf(ResultSet::class, $result);
            return;
        }

        $this->fail(\sprintf("Test did not throw an instance of %s", QueryError::class));
    }

    public function testSimultaneousQueryAndPrepare()
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

        Loop::run(function () use ($promises) {
            yield $promises;
        });
    }

    public function testSimultaneousPrepareAndExecute()
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

        Loop::run(function () use ($promises) {
            yield $promises;
        });
    }

    public function testTransaction()
    {
        Loop::run(function () {
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

            $result = yield $transaction->execute("SELECT * FROM test WHERE domain=\$1 FOR UPDATE", [$data[0]]);

            $this->assertInstanceOf(ResultSet::class, $result);

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
        });
    }

    public function testListen()
    {
        Loop::run(function () {
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
        });
    }

    /**
     * @depends testListen
     */
    public function testNotify()
    {
        Loop::run(function () {
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
        });
    }

    /**
     * @depends testListen
     * @expectedException \Amp\Sql\QueryError
     * @expectedExceptionMessage Already listening on channel
     */
    public function testListenOnSameChannel()
    {
        Loop::run(function () {
            $channel = "test";
            $listener = yield $this->connection->listen($channel);
            $listener = yield $this->connection->listen($channel);
        });
    }
}
