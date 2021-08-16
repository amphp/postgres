<?php

namespace Amp\Postgres\Test;

use Amp\Loop;
use Amp\Postgres\Link;
use Amp\Postgres\PqBufferedResultSet;
use Amp\Postgres\PqConnection;
use Amp\Postgres\PqUnbufferedResultSet;

/**
 * @requires extension pq
 */
class PqConnectionTest extends AbstractConnectionTest
{
    /** @var resource PostgreSQL connection resource. */
    protected $handle;

    public function createLink(string $connectionString): Link
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
            $result = $this->handle->execParams(self::INSERT_QUERY, \array_map('Amp\\Postgres\\cast', $row));

            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }

        return new PqConnection($this->handle);
    }

    public function tearDown(): void
    {
        $this->handle->exec("ROLLBACK");
        $this->handle->exec(self::DROP_QUERY);
    }

    public function testBufferedResults()
    {
        Loop::run(function () {
            \assert($this->connection instanceof PqConnection);
            $this->connection->shouldBufferResults();

            $this->assertTrue($this->connection->isBufferingResults());

            $result = yield $this->connection->query("SELECT * FROM test");
            \assert($result instanceof PqBufferedResultSet);

            $data = $this->getData();
            yield from $this->verifyResult($result, $data);
        });
    }

    /**
     * @depends testBufferedResults
     */
    public function testUnbufferedResults(): \Generator
    {
        \assert($this->connection instanceof PqConnection);
        $this->connection->shouldNotBufferResults();

        $this->assertFalse($this->connection->isBufferingResults());

        $result = yield $this->connection->query("SELECT * FROM test");
        \assert($result instanceof PqUnbufferedResultSet);

        $data = $this->getData();
        yield from $this->verifyResult($result, $data);
    }
}
