<?php declare(strict_types = 1);

namespace Amp\Postgres\Test;

use Amp\Postgres\{ CommandResult, Connection, Statement, Transaction, TupleResult };
use Amp\Success;
use Interop\Async\Loop;

abstract class AbstractPoolTest extends \PHPUnit_Framework_TestCase {
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
    private function makeConnectionSet($count) {
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
    public function testSingleQuery($count, $method, $resultClass, ...$params) {
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
    
        Loop::execute(\Amp\wrap(function () use ($method, $pool, $params, $result) {
            $return = yield $pool->{$method}(...$params);

            $this->assertSame($result, $return);
        }));
    }

    /**
     * @dataProvider getMethodsAndResults
     *
     * @param int $count
     * @param string $method
     * @param string $resultClass
     * @param mixed ...$params
     */
    public function testConsecutiveQueries($count, $method, $resultClass, ...$params) {
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
    
    
        Loop::execute(\Amp\wrap(function () Use ($count, $rounds, $pool, $method, $params) {
            $promises = [];
    
            for ($i = 0; $i < $count * $rounds; ++$i) {
                $promises[] = $pool->{$method}(...$params);
            }
        }));
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
    public function testTransaction($count) {
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
    
        Loop::execute(\Amp\wrap(function () use ($pool, $result) {
            $return = yield $pool->transaction(Transaction::COMMITTED);
            $this->assertInstanceOf(Transaction::class, $return);
            yield $return->rollback();
        }));
    }

    /**
     * @dataProvider getConnectionCounts
     *
     * @param int $count
     */
    public function testConsecutiveTransactions($count) {
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
    
        Loop::execute(\Amp\wrap(function () use ($count, $rounds, $pool) {
            $promises = [];
            for ($i = 0; $i < $count * $rounds; ++$i) {
                $promises[] = $pool->transaction(Transaction::COMMITTED);
            }
            
            yield \Amp\all(\Amp\map(function (Transaction $transaction) {
                return $transaction->rollback();
            }, $promises));
        }));
    }
}
