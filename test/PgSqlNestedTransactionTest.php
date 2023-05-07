<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Postgres\PgSqlConnection;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresLink;
use Amp\Postgres\PostgresNestableTransaction;
use Amp\Postgres\PostgresTransaction;

/**
 * @requires extension pgsql
 */
class PgSqlNestedTransactionTest extends AbstractLinkTest
{
    protected PgSqlConnection $connection;
    protected PostgresTransaction $transaction;

    public function createLink(string $connectionString): PostgresLink
    {
        $connectionConfig = PostgresConfig::fromString($connectionString);
        $connection = PgSqlConnection::connect($connectionConfig);

        $connection->query(self::DROP_QUERY);

        $connection->query(self::CREATE_QUERY);

        foreach ($this->getParams() as $row) {
            $connection->execute(self::INSERT_QUERY, $row);
        }

        $this->connection = $connection;
        $this->transaction = $connection->beginTransaction();

        return new PostgresNestableTransaction($this->transaction);
    }

    public function tearDown(): void
    {
        $this->transaction->rollback();

        parent::tearDown();
    }
}
