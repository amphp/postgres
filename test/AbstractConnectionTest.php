<?php

namespace Amp\Postgres\Test;

use Amp\Coroutine;
use Amp\Delayed;
use Amp\Loop;
use Amp\Postgres\CommandResult;
use Amp\Postgres\Connection;
use Amp\Postgres\QueryError;
use Amp\Postgres\Transaction;
use Amp\Postgres\TransactionError;
use Amp\Postgres\TupleResult;

abstract class AbstractConnectionTest extends \PHPUnit_Framework_TestCase {
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

    abstract public function createConnection(string $connectionString): Connection;

    abstract public function getConnectCallable(): callable;

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
     * @depends testQueryWithTupleResult
     */
    public function testSimultaneousQuery() {
        $callback = \Amp\coroutine(function ($value) {
            /** @var \Amp\Postgres\TupleResult $result */
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
            yield \Amp\Promise\all([$callback(0), $callback(1)]);
        });
    }

    /**
     * @depends testSimultaneousQuery
     */
    public function testSimultaneousQueryWithFirstFailing() {
        $callback = \Amp\coroutine(function ($query) {
            /** @var \Amp\Postgres\TupleResult $result */
            $result = yield $this->connection->query($query);

            $data = $this->getData();

            for ($i = 0; yield $result->advance(); ++$i) {
                $row = $result->getCurrent();
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
            }
        });

        try {
            Loop::run(function () use ($callback) {
                $failing = $callback("SELECT & FROM test");
                $successful = $callback("SELECT * FROM test");

                yield $successful;
                yield $failing;
            });
        } catch (QueryError $exception) {
            return;
        }

        $this->fail(\sprintf("Test did not throw an instance of %s", QueryError::class));
    }

    public function testSimultaneousQueryAndPrepare() {
        $promises = [];
        $promises[] = new Coroutine((function () {
            /** @var \Amp\Postgres\TupleResult $result */
            $result = yield $this->connection->query("SELECT * FROM test");

            $data = $this->getData();

            for ($i = 0; yield $result->advance(); ++$i) {
                $row = $result->getCurrent();
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
            }
        })());

        $promises[] = new Coroutine((function () {
            /** @var \Amp\Postgres\Statement $statement */
            $statement = (yield $this->connection->prepare("SELECT * FROM test"));

            /** @var \Amp\Postgres\TupleResult $result */
            $result = yield $statement->execute();

            $data = $this->getData();

            for ($i = 0; yield $result->advance(); ++$i) {
                $row = $result->getCurrent();
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
            }
        })());

        Loop::run(function () use ($promises) {
            yield \Amp\Promise\all($promises);
        });
    }

    public function testSimultaneousPrepareAndExecute() {
        $promises[] = new Coroutine((function () {
            /** @var \Amp\Postgres\Statement $statement */
            $statement = yield $this->connection->prepare("SELECT * FROM test");

            /** @var \Amp\Postgres\TupleResult $result */
            $result = yield $statement->execute();

            $data = $this->getData();

            for ($i = 0; yield $result->advance(); ++$i) {
                $row = $result->getCurrent();
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
            }
        })());

        $promises[] = new Coroutine((function () {
            /** @var \Amp\Postgres\TupleResult $result */
            $result = yield $this->connection->execute("SELECT * FROM test");

            $data = $this->getData();

            for ($i = 0; yield $result->advance(); ++$i) {
                $row = $result->getCurrent();
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
            }
        })());

        Loop::run(function () use ($promises) {
            yield \Amp\Promise\all($promises);
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

    public function testConnect() {
        Loop::run(function () {
            $connect = $this->getConnectCallable();
            $connection = yield $connect('host=localhost user=postgres');
            $this->assertInstanceOf(Connection::class, $connection);
        });
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidUser() {
        Loop::run(function () {
            $connect = $this->getConnectCallable();
            $connection = yield $connect('host=localhost user=invalid', 100);
        });
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidConnectionString() {
        Loop::run(function () {
            $connect = $this->getConnectCallable();
            $connection = yield $connect('invalid connection string', 100);
        });
    }

    /**
     * @expectedException \Amp\Postgres\FailureException
     */
    public function testConnectInvalidHost() {
        Loop::run(function () {
            $connect = $this->getConnectCallable();
            $connection = yield $connect('hostaddr=invalid.host user=postgres', 100);
        });
    }
}
