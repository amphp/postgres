<?php

namespace Amp\Postgres\Test;

use Amp\Loop;
use Amp\Postgres\StatementPool;

abstract class AbstractPoolTest extends AbstractLinkTest
{
    public function testPrepareSameQueryReturnsSameStatementPool()
    {
        Loop::run(function () {
            $sql = "SELECT * FROM test WHERE domain=\$1";

            /** @var StatementPool $statement1 */
            $statement1 = yield $this->connection->prepare($sql);

            /** @var StatementPool $statement2 */
            $statement2 = yield $this->connection->prepare($sql);

            $this->assertInstanceOf(StatementPool::class, $statement1);
            $this->assertInstanceOf(StatementPool::class, $statement2);

            $this->assertSame($statement1, $statement2);
        });
    }

}
