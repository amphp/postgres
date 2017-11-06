<?php

namespace Amp\Postgres\Test;

use Amp\Postgres\AggregatePool;
use Amp\Postgres\Link;
use Amp\Postgres\PqConnection;

/**
 * @requires extension pq
 */
class PqPoolTest extends AbstractLinkTest {
    /** @var \pq\Connection[] */
    protected $handles = [];

    public function createLink(string $connectionString): Link {
        $pool = new AggregatePool;

        $handle = new \pq\Connection($connectionString);

        $handle->exec("DROP TABLE IF EXISTS test");

        $result = $handle->exec("CREATE TABLE test (domain VARCHAR(63), tld VARCHAR(63), PRIMARY KEY (domain, tld))");

        if (!$result) {
            $this->fail('Could not create test table.');
        }

        foreach ($this->getData() as $row) {
            $result = $handle->execParams("INSERT INTO test VALUES (\$1, \$2)", $row);

            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }

        $this->handles[] = $handle;

        $pool->addConnection(new PqConnection($handle));

        $handle = new \pq\Connection($connectionString);

        $this->handles[] = $handle;

        $pool->addConnection(new PqConnection($handle));

        return $pool;
    }

    public function tearDown() {
        $this->handles[0]->exec("ROLLBACK");
        $this->handles[0]->exec("DROP TABLE test");
    }
}
