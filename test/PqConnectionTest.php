<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Postgres\Internal\PqBufferedResultSet;
use Amp\Postgres\Internal\PqUnbufferedResultSet;
use Amp\Postgres\PostgresLink;
use Amp\Postgres\PqConnection;
use function Amp\Postgres\Internal\cast;

/**
 * @requires extension pq
 */
class PqConnectionTest extends AbstractConnectionTest
{
    /** @var \pg\Connection|null PostgreSQL connection resource. */
    protected ?\pq\Connection $handle;

    public function createLink(string $connectionString): PostgresLink
    {
        $this->handle = new \pq\Connection($connectionString);
        $this->handle->nonblocking = true;
        $this->handle->unbuffered = true;

        $this->handle->exec(self::DROP_QUERY);

        $result = $this->handle->exec(self::CREATE_QUERY);

        if (!$result) {
            $this->fail('Could not create test table.');
        }

        foreach ($this->getData() as $row) {
            $result = $this->handle->execParams(self::INSERT_QUERY, \array_map(cast(...), $row));

            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }

        return $this->newConnection(PqConnection::class, $this->handle);
    }

    public function tearDown(): void
    {
        $this->handle->reset();
        $this->handle->exec("ROLLBACK");
        $this->handle->exec(self::DROP_QUERY);

        $this->handle = null;

        parent::tearDown();
    }

    public function testBufferedResults(): void
    {
        \assert($this->link instanceof PqConnection);
        $this->link->shouldBufferResults();

        $this->assertTrue($this->link->isBufferingResults());

        $result = $this->link->query("SELECT * FROM test");
        \assert($result instanceof PqBufferedResultSet);

        $data = $this->getData();
        $this->verifyResult($result, $data);
    }

    /**
     * @depends testBufferedResults
     */
    public function testUnbufferedResults(): void
    {
        \assert($this->link instanceof PqConnection);
        $this->link->shouldNotBufferResults();

        $this->assertFalse($this->link->isBufferingResults());

        $result = $this->link->query("SELECT * FROM test");
        \assert($result instanceof PqUnbufferedResultSet);

        $data = $this->getData();
        $this->verifyResult($result, $data);
    }

    public function testNextResultBeforeConsumption()
    {
        $result = $this->link->query("SELECT * FROM test; SELECT * FROM test;");

        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Consume entire current result before requesting next result');

        $result->getNextResult();
    }

    public function testUnconsumedMultiResult()
    {
        $result = $this->link->query("SELECT * FROM test; SELECT * FROM test");

        unset($result);

        $result = $this->link->query("SELECT * FROM test; SELECT * FROM test");

        $this->verifyResult($result, $this->getData());

        $result = $result->getNextResult();
        self::assertNotNull($result);

        $this->verifyResult($result, $this->getData());
    }
}
