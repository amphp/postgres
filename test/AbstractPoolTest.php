<?php

namespace Amp\Postgres\Test;

use Amp\Loop;
use Amp\Postgres\CommandResult;
use Amp\Postgres\Connection;
use Amp\Postgres\Statement;
use Amp\Postgres\Transaction;
use Amp\Postgres\TupleResult;
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
    abstract protected function createPool(array $connections);

    /**
     * @return \PHPUnit_Framework_MockObject_MockObject|\Amp\Postgres\Connection
     */
    private function createConnection() {
        return $this->createMock(Connection::class);
    }

    /**
     * @param int $count
     *
     * @return \Amp\Postgres\Connection[]|\PHPUnit_Framework_MockObject_MockObject[]
     */
    private function makeConnectionSet(int $count) {
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
            [3, 'query', TupleResult::class, "SELECT * FROM test"],
            [2, 'query', CommandResult::class, "INSERT INTO test VALUES (1, 7)"],
            [1, 'prepare', Statement::class, "SELECT * FROM test WHERE id=\$1"],
            [4, 'execute', TupleResult::class, "SELECT * FROM test WHERE id=\$1 AND time>\$2", 1, time()],
        ];
    }

    /**
     * @dataProvider getMethodsAndResults
     *
     * @param int $count
     * @param string $method
     * @param string $resultClass
     * @param mixed ...$params
     */
    public function testSingleQuery(int $count, string $method, string $resultClass, ...$params) {
        $result = $this->getMockBuilder($resultClass)
            ->disableOriginalConstructor()
            ->getMock();

        $connections = $this->makeConnectionSet($count);

        $connection = $connections[0];
        $connection->expects($this->once())
            ->method($method)
            ->with(...$params)
            ->will($this->returnValue(new Success($result)));

        $pool = $this->createPool($connections);

        Loop::run(function () use ($method, $pool, $params, $result) {
            $return = yield $pool->{$method}(...$params);

            $this->assertSame($result, $return);
        });
    }

    /**
     * @dataProvider getMethodsAndResults
     *
     * @param int $count
     * @param string $method
     * @param string $resultClass
     * @param mixed ...$params
     */
    public function testConsecutiveQueries(int $count, string $method, string $resultClass, ...$params) {
        $rounds = 3;
        $result = $this->getMockBuilder($resultClass)
            ->disableOriginalConstructor()
            ->getMock();

        $connections = $this->makeConnectionSet($count);

        foreach ($connections as $connection) {
            $connection->method($method)
                ->with(...$params)
                ->will($this->returnValue(new Success($result)));
        }

        $pool = $this->createPool($connections);

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
            ->will($this->returnValue(new Success($result)));

        $pool = $this->createPool($connections);

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
                    return new Success($result);
                }));
        }

        $pool = $this->createPool($connections);

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
    public function testGetConnection(int $count) {
        $connections = $this->makeConnectionSet($count);
        $query = "SELECT * FROM test";

        foreach ($connections as $connection) {
            $connection->expects($this->once())
                ->method('query')
                ->with($query);
        }

        $pool = $this->createPool($connections);

        Loop::run(function () use ($pool, $query, $count) {
            $promises = [];
            for ($i = 0; $i < $count; ++$i) {
                $promises[] = $pool->getConnection();
            }

            $results = yield Promise\all($promises);

            foreach ($results as $result) {
                $this->assertInstanceof(Connection::class, $result);
                $result->query($query);
            }
        });
    }
}
