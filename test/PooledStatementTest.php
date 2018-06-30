<?php

namespace Amp\Postgres\Test;

use Amp\Delayed;
use Amp\Loop;
use Amp\PHPUnit\TestCase;
use Amp\Postgres\ConnectionConfig;
use Amp\Postgres\Pool;
use Amp\Postgres\ResultSet;
use Amp\Sql\PooledStatement;
use Amp\Sql\Statement;
use Amp\Success;

class PooledStatementTest extends TestCase
{
    public function testActiveStatementsRemainAfterTimeout()
    {
        Loop::run(function () {
            $pool = new Pool(new ConnectionConfig('host=localhost user=postgres'));

            $statement = $this->createMock(Statement::class);
            $statement->method('getQuery')
                ->willReturn('SELECT 1');
            $statement->method('lastUsedAt')
                ->willReturn(\time());
            $statement->expects($this->once())
                ->method('execute');

            $pooledStatement = new PooledStatement($pool, $statement, $this->createCallback(0));

            $this->assertTrue($pooledStatement->isAlive());
            $this->assertSame(\time(), $pooledStatement->lastUsedAt());

            yield new Delayed(1500); // Give timeout watcher enough time to execute.

            $pooledStatement->execute();

            $this->assertTrue($pooledStatement->isAlive());
            $this->assertSame(\time(), $pooledStatement->lastUsedAt());
        });
    }

    public function testIdleStatementsRemovedAfterTimeout()
    {
        Loop::run(function () {
            $pool = new Pool(new ConnectionConfig('host=localhost user=postgres'));

            $statement = $this->createMock(Statement::class);
            $statement->method('getQuery')
                ->willReturn('SELECT 1');
            $statement->method('lastUsedAt')
                ->willReturn(0);
            $statement->expects($this->never())
                ->method('execute');

            $prepare = function () {
                $statement = $this->createMock(Statement::class);
                $statement->expects($this->once())
                    ->method('execute')
                    ->willReturn(new Success($this->createMock(ResultSet::class)));
                return new Success($statement);
            };

            $pooledStatement = new PooledStatement($pool, $statement, $prepare);

            $this->assertTrue($pooledStatement->isAlive());
            $this->assertSame(\time(), $pooledStatement->lastUsedAt());

            yield new Delayed(1500); // Give timeout watcher enough time to execute and remove mock statement object.

            $result = yield $pooledStatement->execute();

            $this->assertInstanceOf(ResultSet::class, $result);

            $this->assertTrue($pooledStatement->isAlive());
            $this->assertSame(\time(), $pooledStatement->lastUsedAt());
        });
    }
}
