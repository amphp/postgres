<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PqConnection;

/**
 * @requires extension pq
 */
class PqNestedTransactionTest extends AbstractNestedTransactionTest
{
    public function connect(PostgresConfig $connectionConfig): PostgresConnection
    {
        return PqConnection::connect($connectionConfig);
    }
}
