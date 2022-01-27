<?php

namespace Amp\Postgres\Test;

use Amp\Future;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\Link;
use Amp\Postgres\Listener;
use Amp\Postgres\QueryExecutionError;
use Amp\Postgres\Transaction;
use Amp\Sql\QueryError;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\Transaction as SqlTransaction;
use Amp\Sql\TransactionError;
use Revolt\EventLoop;
use function Amp\async;

abstract class AbstractLinkTest extends AsyncTestCase
{
    protected const CREATE_QUERY = "CREATE TABLE test (domain VARCHAR(63) NOT NULL,
                                                       tld VARCHAR(63) NOT NULL,
                                                       keys INTEGER[] NOT NULL,
                                                       enabled BOOLEAN NOT NULL,
                                                       number DOUBLE PRECISION NOT NULL,
                                                       nullable CHAR(1) DEFAULT NULL,
                                                       PRIMARY KEY (domain, tld))";
    protected const DROP_QUERY = "DROP TABLE IF EXISTS test";
    protected const INSERT_QUERY = 'INSERT INTO test VALUES ($1, $2, $3, $4, $5, $6)';
    protected const FIELD_COUNT = 6;

    protected Link $link;

    /**
     * @return array Start test data for database.
     */
    public function getData(): array
    {
        return [
            ['amphp', 'org', [1], true, 3.14159, null],
            ['github', 'com', [1, 2, 3, 4, 5], false, 2.71828, null],
            ['google', 'com', [1, 2, 3, 4], true, 1.61803, null],
            ['php', 'net', [1, 2], false, 0.0, null],
        ];
    }

    protected function verifyResult(Result $result, array $data): void
    {
        //$this->assertSame(self::FIELD_COUNT, $result->getFieldCount());

        $i = 0;
        foreach ($result as $row) {
            $this->assertSame($data[$i][0], $row['domain']);
            $this->assertSame($data[$i][1], $row['tld']);
            $this->assertSame($data[$i][2], $row['keys']);
            $this->assertSame($data[$i][3], $row['enabled']);
            $this->assertIsFloat($data[$i][4], $row['number']);
            $this->assertNull($row['nullable']);
            ++$i;
        }
    }

    /**
     * @param string $connectionString
     *
     * @return Link Connection or Link object to be tested.
     */
    abstract public function createLink(string $connectionString): Link;

    public function setUp(): void
    {
        parent::setUp();
        $this->link = $this->createLink('host=localhost user=postgres');
    }

    public function tearDown(): void
    {
        parent::tearDown();
        $this->link->close();
    }

    public function testQueryWithTupleResult()
    {
        $result = $this->link->query("SELECT * FROM test");

        $data = $this->getData();

        $this->verifyResult($result, $data);

        $this->assertNull($result->getNextResult());
    }

    public function testMultipleQueryWithTupleResult()
    {
        $result = $this->link->query("SELECT * FROM test; SELECT * FROM test");

        $data = $this->getData();

        $this->verifyResult($result, $data);

        $result = $result->getNextResult();

        $this->assertInstanceOf(Result::class, $result);

        $this->verifyResult($result, $data);

        $this->assertNull($result->getNextResult());
    }

    public function testMultipleQueryWithCommandResultFirst()
    {
        $result = $this->link->query("INSERT INTO test VALUES ('canon', 'jp', '{1}', true, 4.2); SELECT * FROM test");

        $this->assertSame(1, $result->getRowCount());

        $result = $result->getNextResult();

        $this->assertInstanceOf(Result::class, $result);

        $data = $this->getData();
        $data[] = ['canon', 'jp', [1], true, 4.2]; // Add inserted row to expected data.

        $this->verifyResult($result, $data);

        $this->assertNull($result->getNextResult());
    }

    public function testMultipleQueryWithCommandResultSecond()
    {
        $result = $this->link->query("SELECT * FROM test; INSERT INTO test VALUES ('canon', 'jp', '{1}', true, 4.2)");

        $data = $this->getData();

        $this->verifyResult($result, $data);

        $result = $result->getNextResult();

        $this->assertSame(1, $result->getRowCount());

        $this->assertNull($result->getNextResult());
    }

    public function testQueryWithUnconsumedTupleResult()
    {
        $result = $this->link->query("SELECT * FROM test");

        $this->assertInstanceOf(Result::class, $result);

        unset($result); // Force destruction of result object.

        $result = $this->link->query("SELECT * FROM test");

        $this->assertInstanceOf(Result::class, $result);

        $data = $this->getData();

        $this->verifyResult($result, $data);
    }

    public function testQueryWithCommandResult(): void
    {
        $result = $this->link->query("INSERT INTO test VALUES ('canon', 'jp', '{1}', true, 4.2)");

        $this->assertSame(1, $result->getRowCount());
    }

    public function testQueryWithEmptyQuery(): void
    {
        $this->expectException(QueryError::class);
        $this->link->query('');
    }

    public function testQueryWithSyntaxError()
    {
        /** @var Result $result */
        try {
            $result = $this->link->query("SELECT & FROM test");
            $this->fail(\sprintf("An instance of %s was expected to be thrown", QueryExecutionError::class));
        } catch (QueryExecutionError $exception) {
            $diagnostics = $exception->getDiagnostics();
            $this->assertArrayHasKey("sqlstate", $diagnostics);
        }
    }

    public function testPrepare()
    {
        $query = "SELECT * FROM test WHERE domain=\$1";

        $statement = $this->link->prepare($query);

        $this->assertSame($query, $statement->getQuery());

        $data = $this->getData()[0];

        $result = $statement->execute([$data[0]]);

        $this->verifyResult($result, [$data]);
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareWithNamedParams()
    {
        $query = "SELECT * FROM test WHERE domain=:domain AND tld=:tld";

        $statement = $this->link->prepare($query);

        $data = $this->getData()[0];

        $this->assertSame($query, $statement->getQuery());

        $result = $statement->execute(['domain' => $data[0], 'tld' => $data[1]]);

        $this->verifyResult($result, [$data]);
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareWithUnnamedParams()
    {
        $query = "SELECT * FROM test WHERE domain=? AND tld=?";

        $statement = $this->link->prepare($query);

        $data = $this->getData()[0];

        $this->assertSame($query, $statement->getQuery());

        $result = $statement->execute([$data[0], $data[1]]);

        $this->verifyResult($result, [$data]);
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareWithNamedParamsWithDataAppearingAsNamedParam()
    {
        $query = "SELECT * FROM test WHERE domain=:domain OR domain=':domain'";

        $statement = $this->link->prepare($query);

        $data = $this->getData()[0];

        $this->assertSame($query, $statement->getQuery());

        $result = $statement->execute(['domain' => $data[0]]);

        $this->verifyResult($result, [$data]);
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareInvalidQuery()
    {
        $this->expectException(QueryExecutionError::class);
        $this->expectExceptionMessage('column "invalid" does not exist');

        $query = "SELECT * FROM test WHERE invalid=\$1";

        $statement = $this->link->prepare($query);

        $statement->execute(['param']);
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareSameQuery()
    {
        $sql = "SELECT * FROM test WHERE domain=\$1";

        $statement1 = $this->link->prepare($sql);

        $statement2 = $this->link->prepare($sql);

        $this->assertInstanceOf(Statement::class, $statement1);
        $this->assertInstanceOf(Statement::class, $statement2);

        unset($statement1);

        $data = $this->getData()[0];

        $result = $statement2->execute([$data[0]]);

        $this->verifyResult($result, [$data]);
    }

    /**
     * @depends testPrepareSameQuery
     */
    public function testSimultaneousPrepareSameQuery()
    {
        $sql = "SELECT * FROM test WHERE domain=\$1";

        $statement1 = async(fn() => $this->link->prepare($sql));
        $statement2 = async(fn() => $this->link->prepare($sql));

        [$statement1, $statement2] = Future\all([$statement1, $statement2]);

        $data = $this->getData()[0];

        $result = $statement1->execute([$data[0]]);

        $this->verifyResult($result, [$data]);

        unset($statement1);

        $result = $statement2->execute([$data[0]]);

        $this->verifyResult($result, [$data]);
    }

    public function testPrepareSimilarQueryReturnsDifferentStatements()
    {
        $statement1 = async(fn() => $this->link->prepare("SELECT * FROM test WHERE domain=\$1"));

        $statement2 = async(fn() => $this->link->prepare("SELECT * FROM test WHERE domain=:domain"));

        [$statement1, $statement2] = Future\all([$statement1, $statement2]);

        $this->assertInstanceOf(Statement::class, $statement1);
        $this->assertInstanceOf(Statement::class, $statement2);

        $this->assertNotSame($statement1, $statement2);

        $data = $this->getData()[0];

        $results = [];

        $results[] = \iterator_to_array($statement1->execute([$data[0]]));
        $results[] = \iterator_to_array($statement2->execute(['domain' => $data[0]]));

        foreach ($results as $result) {
            /** @var Result $result */
            foreach ($result as $row) {
                $this->assertSame($data[0], $row['domain']);
                $this->assertSame($data[1], $row['tld']);
            }
        }
    }

    public function testPrepareThenExecuteWithUnconsumedTupleResult()
    {
        $statement = $this->link->prepare("SELECT * FROM test");

        $result = $statement->execute();

        unset($result); // Force destruction of result object.

        $result = $statement->execute();

        $data = $this->getData();

        $this->verifyResult($result, $data);
    }

    public function testExecute()
    {
        $data = $this->getData()[0];

        $result = $this->link->execute("SELECT * FROM test WHERE domain=\$1", [$data[0]]);

        $this->verifyResult($result, [$data]);
    }

    /**
     * @depends testExecute
     */
    public function testExecuteWithNamedParams()
    {
        $data = $this->getData()[0];

        $result = $this->link->execute(
            "SELECT * FROM test WHERE domain=:domain",
            ['domain' => $data[0]]
        );

        $this->verifyResult($result, [$data]);
    }

    /**
     * @depends testExecute
     */
    public function testExecuteWithInvalidParams()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage("Value for unnamed parameter at position 0 missing");

        $this->link->execute("SELECT * FROM test WHERE domain=\$1");
    }

    /**
     * @depends testExecute
     */
    public function testExecuteWithInvalidNamedParams()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage("Value for named parameter 'domain' missing");

        $this->link->execute("SELECT * FROM test WHERE domain=:domain", ['tld' => 'com']);
    }

    /**
     * @depends testQueryWithTupleResult
     */
    public function testSimultaneousQuery()
    {
        $callback = fn (int $value) => async(function () use ($value): void {
            $result = $this->link->query("SELECT {$value} as value");

            foreach ($result as $row) {
                $this->assertEquals($value, $row['value']);
            }
        });

        Future\all([$callback(0), $callback(1)]);
    }

    /**
     * @depends testSimultaneousQuery
     */
    public function testSimultaneousQueryWithOneFailing()
    {
        $callback = fn (string $query) => async(function () use ($query): Result {
            $result = $this->link->query($query);

            $data = $this->getData();

            $i = 0;
            foreach ($result as $row) {
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
                ++$i;
            }

            return $result;
        });

        $result = null;

        try {
            $successful = $callback("SELECT * FROM test");
            $failing = $callback("SELECT & FROM test");

            $result = $successful->await();
            $failing->await();
        } catch (QueryError $exception) {
            $this->assertInstanceOf(Result::class, $result);
            return;
        }

        $this->fail(\sprintf("Test did not throw an instance of %s", QueryError::class));
    }

    public function testSimultaneousQueryAndPrepare()
    {
        $promises = [];
        $promises[] = async(function () {
            $result = $this->link->query("SELECT * FROM test");
            $data = $this->getData();
            $this->verifyResult($result, $data);
        });

        $promises[] = async(function () {
            $statement = ($this->link->prepare("SELECT * FROM test"));
            $result = $statement->execute();
            $data = $this->getData();
            $this->verifyResult($result, $data);
        });

        Future\all($promises);
    }

    public function testSimultaneousPrepareAndExecute()
    {
        $promises[] = async(function () {
            $statement = $this->link->prepare("SELECT * FROM test");
            $result = $statement->execute();
            $data = $this->getData();
            $this->verifyResult($result, $data);
        });

        $promises[] = async(function () {
            $result = $this->link->execute("SELECT * FROM test");
            $data = $this->getData();
            $this->verifyResult($result, $data);
        });

        Future\all($promises);
    }

    public function testTransaction()
    {
        $isolation = SqlTransaction::ISOLATION_COMMITTED;

        $transaction = $this->link->beginTransaction($isolation);

        $this->assertInstanceOf(Transaction::class, $transaction);

        $data = $this->getData()[0];

        $this->assertTrue($transaction->isAlive());
        $this->assertTrue($transaction->isActive());
        $this->assertSame($isolation, $transaction->getIsolationLevel());

        $transaction->createSavepoint('test');

        $statement = $transaction->prepare("SELECT * FROM test WHERE domain=:domain");
        $result = $statement->execute(['domain' => $data[0]]);

        unset($result); // Force destruction of result object.

        $result = $transaction->execute("SELECT * FROM test WHERE domain=\$1 FOR UPDATE", [$data[0]]);

        unset($result); // Force destruction of result object.

        $transaction->rollbackTo('test');

        $transaction->commit();

        $this->assertFalse($transaction->isAlive());
        $this->assertFalse($transaction->isActive());

        try {
            $result = $transaction->execute("SELECT * FROM test");
            $this->fail('Query should fail after transaction commit');
        } catch (TransactionError $exception) {
            // Exception expected.
        }
    }

    public function testTransactionIsReleasedOnCommit(): void
    {
        $transaction = $this->link->beginTransaction();

        $this->assertInstanceOf(Transaction::class, $transaction);

        $data = $this->getData()[0];

        $result = $transaction->execute("SELECT * FROM test WHERE domain=:domain", ['domain' => $data[0]]);
        unset($result);
        $transaction->commit();

        try {
            $transaction->execute("SELECT * FROM test");
            $this->fail('Query should fail after transaction commit');
        } catch (TransactionError $exception) {
            // Exception expected.
        }

        $this->assertTrue($this->link->isAlive());

        $result = $this->link->execute('SELECT * FROM test WHERE domain = ?', [$data[0]]);
        self::assertSame(1, $result->getRowCount());
        unset($result);
    }

    public function testTransactionIsReleasedOnRollback(): void
    {
        $transaction = $this->link->beginTransaction();

        $this->assertInstanceOf(Transaction::class, $transaction);

        $data = $this->getData()[0];

        $result = $transaction->query("DELETE FROM test");
        unset($result);
        $transaction->rollback();

        try {
            $transaction->execute("SELECT * FROM test");
            $this->fail('Query should fail after transaction commit');
        } catch (TransactionError $exception) {
            // Exception expected.
        }

        $this->assertTrue($this->link->isAlive());

        $result = $this->link->execute('SELECT * FROM test WHERE domain = ?', [$data[0]]);
        self::assertSame(1, $result->getRowCount());
        unset($result);
    }

    public function testListen()
    {
        $channel = "test";
        $listener = $this->link->listen($channel);

        $this->assertInstanceOf(Listener::class, $listener);
        $this->assertSame($channel, $listener->getChannel());

        EventLoop::delay(0.1, function () use ($channel): void {
            $this->link->query(\sprintf("NOTIFY %s, '%s'", $channel, '0'));
            $this->link->query(\sprintf("NOTIFY %s, '%s'", $channel, '1'));
        });

        $count = 0;
        EventLoop::delay(0.2, fn () => $listener->unlisten());

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
        $listener = $this->link->listen($channel);

        EventLoop::delay(0.1, function () use ($channel) {
            $this->link->notify($channel, '0');
            $this->link->notify($channel, '1');
        });

        $count = 0;
        EventLoop::delay(0.2, fn () => $listener->unlisten());

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
        Future\all([$this->link->listen($channel), $this->link->listen($channel)]);
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
