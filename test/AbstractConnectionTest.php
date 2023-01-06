<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Sql\ConnectionException;
use function Amp\async;

abstract class AbstractConnectionTest extends AbstractLinkTest
{
    public function testIsClosed()
    {
        $this->assertFalse($this->link->isClosed());
    }

    public function testConnectionCloseDuringQuery(): void
    {
        $query = async($this->link->execute(...), 'SELECT pg_sleep(10)');
        $close = async($this->link->close(...));

        $start = \microtime(true);

        $close->await();

        try {
            $query->await();
            self::fail(\sprintf('Expected %s to be thrown', ConnectionException::class));
        } catch (ConnectionException) {
            // Expected
        }

        $this->assertLessThan(0.1, \microtime(true) - $start);
    }
}
