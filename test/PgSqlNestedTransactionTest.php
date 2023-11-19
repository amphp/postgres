<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Postgres\PgSqlConnection;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;

/**
 * @requires extension pgsql
 */
class PgSqlNestedTransactionTest extends AbstractNestedTransactionTest
{
    public function connect(PostgresConfig $connectionConfig): PostgresConnection
    {
        return PgSqlConnection::connect($connectionConfig);
    }
}
