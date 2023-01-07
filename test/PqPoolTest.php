<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Postgres\ByteA;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;
use Amp\Postgres\PostgresLink;
use Amp\Postgres\PqConnection;
use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\SqlConnector;
use function Amp\Postgres\Internal\cast;

/**
 * @requires extension pq
 */
class PqPoolTest extends AbstractLinkTest
{
    const POOL_SIZE = 3;

    /** @var \pq\Connection[] */
    protected array $handles = [];

    public function createLink(string $connectionString): PostgresLink
    {
        for ($i = 0; $i < self::POOL_SIZE; ++$i) {
            $this->handles[] = $handle = new \pq\Connection($connectionString);
            $handle->nonblocking = true;
            $handle->unbuffered = true;
        }

        $connector = $this->createMock(SqlConnector::class);
        $connector->method('connect')
            ->willReturnCallback(function (): PqConnection {
                static $count = 0;
                if (!isset($this->handles[$count])) {
                    $this->fail("createConnection called too many times");
                }
                $handle = $this->handles[$count];
                ++$count;
                return $this->newConnection(PqConnection::class, $handle);
            });

        $pool = new PostgresConnectionPool(new PostgresConfig('localhost'), \count($this->handles), ConnectionPool::DEFAULT_IDLE_TIMEOUT, true, $connector);

        $handle = \reset($this->handles);

        $handle->exec(self::DROP_QUERY);

        $result = $handle->exec(self::CREATE_QUERY);

        if (!$result) {
            $this->fail('Could not create test table.');
        }

        foreach ($this->getParams() as $row) {
            $result = $handle->execParams(self::INSERT_QUERY, \array_map(fn ($data) => $this->cast($handle, $data), $row));

            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }

        return $pool;
    }

    private function cast(\pq\Connection $connection, mixed $param): mixed
    {
        return $param instanceof ByteA ? $connection->escapeBytea($param->getData()) : cast($param);
    }

    public function tearDown(): void
    {
        $this->handles[0]->exec("ROLLBACK");
        $this->handles[0]->exec(self::DROP_QUERY);

        $this->handles = [];

        parent::tearDown();
    }
}
