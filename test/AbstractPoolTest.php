<?php

namespace Amp\Postgres\Test;

use Amp\Delayed;
use Amp\Loop;
use Amp\Postgres\CommandResult;
use Amp\Postgres\Connection;
use Amp\Postgres\Listener;
use Amp\Postgres\Pool;
use Amp\Postgres\PqStatement;
use Amp\Postgres\ResultSet;
use Amp\Postgres\Transaction;
use Amp\Promise;
use Amp\Success;
use PHPUnit\Framework\TestCase;
use function Amp\call;

abstract class AbstractPoolTest extends TestCase {
    /**
     * @param array $connections
     *
     * @return \Amp\Postgres\Pool
     */
    abstract protected function createPool(array $connections): Pool;

    /**
     * @return \PHPUnit_Framework_MockObject_MockObject|\Amp\Postgres\Connection
     */
    protected function createConnection(): Connection {
        $mock = $this->createMock(Connection::class);
        $mock->method('isAlive')->willReturn(true);
        return $mock;
    }

    /**
     * @param int $count
     *
     * @return \Amp\Postgres\Connection[]|\PHPUnit_Framework_MockObject_MockObject[]
     */
    protected function makeConnectionSet(int $count) {
        $connections = [];

        for ($i = 0; $i < $count; ++$i) {
            $connections[] = $this->createConnection();
        }

        return $connections;
    }

    /**
     * @return array
     */
    public function getMethodsAndResults() {
        return [
            [3, 'query', ResultSet::class, ["SELECT * FROM test"]],
            [2, 'query', CommandResult::class, ["INSERT INTO test VALUES (1, 7)"]],
            [5, 'listen', Listener::class, ["test"]],
            [4, 'execute', ResultSet::class, ["SELECT * FROM test WHERE id=\$1 AND time>\$2", [1, time()]]],
            [4, 'notify', CommandResult::class, ["test", "payload"]],
        ];
    }

    /**
     * @dataProvider getMethodsAndResults
     *
     * @param int $count
     * @param string $method
     * @param string $resultClass
     * @param mixed[] $params
     */
    public function testSingleQuery(int $count, string $method, string $resultClass, array $params = []) {
        $result = $this->getMockBuilder($resultClass)
            ->disableOriginalConstructor()
            ->getMock();

        $connections = $this->makeConnectionSet($count);

        $connection = $connections[0];
        $connection->expects($this->once())
            ->method($method)
            ->with(...$params)
            ->will($this->returnValue(new Delayed(10, $result)));

        $pool = $this->createPool($connections);
        $pool->resetConnections(false);

        Loop::run(function () use ($method, $pool, $params, $result, $resultClass) {
            $return = yield $pool->{$method}(...$params);
            $this->assertInstanceOf($resultClass, $return);
        });
    }

    /**
     * @dataProvider getMethodsAndResults
     *
     * @param int $count
     * @param string $method
     * @param string $resultClass
     * @param mixed[] $params
     */
    public function testConsecutiveQueries(int $count, string $method, string $resultClass, array $params = []) {
        $rounds = 3;
        $result = $this->getMockBuilder($resultClass)
            ->disableOriginalConstructor()
            ->getMock();

        $connections = $this->makeConnectionSet($count);

        foreach ($connections as $connection) {
            $connection->method($method)
                ->with(...$params)
                ->will($this->returnValue(new Delayed(10, $result)));
        }

        $pool = $this->createPool($connections);
        $pool->resetConnections(false);

        Loop::run(function () use ($resultClass, $count, $rounds, $pool, $method, $params) {
            $promises = [];

            for ($i = 0; $i < $count * $rounds; ++$i) {
                $promises[] = $pool->{$method}(...$params);
            }

            $results = yield Promise\all($promises);

            foreach ($results as $result) {
                $this->assertInstanceOf($resultClass, $result);
            }
        });
    }

    /**
     * @return array
     */
    public function getConnectionCounts() {
        return array_map(function ($count) { return [$count]; }, range(1, 10));
    }

    /**
     * @dataProvider getConnectionCounts
     *
     * @param int $count
     */
    public function testTransaction(int $count) {
        $connections = $this->makeConnectionSet($count);

        $connection = $connections[0];
        $result = $this->getMockBuilder(Transaction::class)
            ->disableOriginalConstructor()
            ->getMock();

        $connection->expects($this->once())
            ->method('transaction')
            ->with(Transaction::COMMITTED)
            ->will($this->returnValue(new Delayed(10, $result)));

        $pool = $this->createPool($connections);
        $pool->resetConnections(false);

        Loop::run(function () use ($pool, $result) {
            $return = yield $pool->transaction(Transaction::COMMITTED);
            $this->assertInstanceOf(Transaction::class, $return);
            yield $return->rollback();
        });
    }

    /**
     * @dataProvider getConnectionCounts
     *
     * @param int $count
     */
    public function testConsecutiveTransactions(int $count) {
        $rounds = 3;
        $result = $this->getMockBuilder(Transaction::class)
            ->disableOriginalConstructor()
            ->getMock();

        $connections = $this->makeConnectionSet($count);

        foreach ($connections as $connection) {
            $connection->method('transaction')
                ->with(Transaction::COMMITTED)
                ->will($this->returnCallback(function () use ($result) {
                    return new Delayed(10, $result);
                }));
        }

        $pool = $this->createPool($connections);
        $pool->resetConnections(false);

        Loop::run(function () use ($count, $rounds, $pool) {
            $promises = [];
            for ($i = 0; $i < $count; ++$i) {
                $promises[] = $pool->transaction(Transaction::COMMITTED);
            }

            $results = yield Promise\all(\array_map(function (Promise $promise) {
                return call(function () use ($promise) {
                    $transaction = yield $promise;
                    $this->assertInstanceOf(Transaction::class, $transaction);
                    return yield $transaction->rollback();
                });
            }, $promises));

            foreach ($results as $result) {
                $this->assertInstanceof(CommandResult::class, $result);
            }
        });
    }

    /**
     * @dataProvider getConnectionCounts
     *
     * @param int $count
     */
    public function testExtractConnection(int $count) {
        $connections = $this->makeConnectionSet($count);
        $query = "SELECT * FROM test";

        foreach ($connections as $connection) {
            $connection->expects($this->once())
                ->method('query')
                ->with($query);
        }

        $pool = $this->createPool($connections);
        $pool->resetConnections(false);

        Loop::run(function () use ($pool, $query, $count) {
            $promises = [];
            for ($i = 0; $i < $count; ++$i) {
                $promises[] = $pool->extractConnection();
            }

            $results = yield Promise\all($promises);

            foreach ($results as $result) {
                $this->assertInstanceof(Connection::class, $result);
                $result->query($query);
            }
        });
    }

    public function testConnectionClosedWhileInPool() {
        Loop::run(function () {
            $connections = $this->makeConnectionSet(1);

            $connection = $this->createMock(Connection::class);
            $connection->method('isAlive')->willReturnOnConsecutiveCalls(true, true, true, false);

            $count = \array_unshift($connections, $connection);

            foreach ($connections as $connection) {
                $connection->method('query')
                    ->willReturn(new Delayed(10));
            }

            $pool = $this->createPool($connections);
            $pool->resetConnections(false);

            // Perform queries to load mock connections into pool.
            for ($i = 0; $i < $count; ++$i) {
                yield $pool->query("SELECT 1");
            }

            $extracted = yield $pool->extractConnection();
            $this->assertNotSame($connections[0], $extracted);
            $this->assertSame($connections[1], $extracted);
        });
    }

    public function testPooledStatementWhenConnectionCloses() {
        Loop::run(function () {
            $connections = [];
            $params = [1, 2, 3];

            $statement = $this->createMock(PqStatement::class);
            $statement->method('isAlive')
                ->willReturn(false);
            $statement->expects($this->never())
                ->method('execute');

            $connection = $this->createMock(Connection::class);
            $connection->expects($this->once())
                ->method('prepare')
                ->willReturn(new Success($statement));
            $connections[] = $connection;

            $statement = $this->createMock(PqStatement::class);
            $statement->method('isAlive')
                ->willReturn(true);
            $statement->expects($this->once())
                ->method('execute')
                ->with($params);

            $connection = $this->createMock(Connection::class);
            $connection->expects($this->once())
                ->method('prepare')
                ->willReturn(new Success($statement));
            $connections[] = $connection;

            $pool = $this->createPool($connections);
            $pool->resetConnections(false);

            $statement = yield $pool->prepare("SQL");
            $statement->execute($params);
        });
    }
}
