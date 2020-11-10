<?php

namespace Amp\Postgres\Test;

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

        $this->handle->exec("DROP TABLE IF EXISTS test");

        $result = $this->handle->exec("CREATE TABLE test (domain VARCHAR(63), tld VARCHAR(63), PRIMARY KEY (domain, tld))");

        if (!$result) {
            $this->fail('Could not create test table.');
        }

        foreach ($this->getData() as $row) {
            $result = $this->handle->execParams("INSERT INTO test VALUES (\$1, \$2)", $row);

            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }

        return new PqConnection($this->handle);
    }

    public function cleanup(): void
    {
        $this->handle->exec("ROLLBACK");
        $this->handle->exec("DROP TABLE test");

        parent::cleanup();
    }

    public function testBufferedResults()
    {
        $this->link->shouldBufferResults();

        $this->assertTrue($this->link->isBufferingResults());

        $result = $this->link->query("SELECT * FROM test");
        \assert($result instanceof PqBufferedResultSet);

        $data = $this->getData();

        for ($i = 0; $row = $result->continue(); ++$i) {
            $this->assertSame($data[$i][0], $row['domain']);
            $this->assertSame($data[$i][1], $row['tld']);
        }
    }

    /**
     * @depends testBufferedResults
     */
    public function testUnbufferedResults()
    {
        $this->link->shouldNotBufferResults();

        $this->assertFalse($this->link->isBufferingResults());

        $result = $this->link->query("SELECT * FROM test");
        \assert($result instanceof PqUnbufferedResultSet);

        $data = $this->getData();

        for ($i = 0; $row = $result->continue(); ++$i) {
            $this->assertSame($data[$i][0], $row['domain']);
            $this->assertSame($data[$i][1], $row['tld']);
        }
    }
}
