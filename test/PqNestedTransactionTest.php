<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresLink;
use Amp\Postgres\PostgresNestableTransaction;
use Amp\Postgres\PostgresTransaction;
use Amp\Postgres\PqConnection;

/**
 * @requires extension pgsql
 */
class PqNestedTransactionTest extends AbstractLinkTest
{
    protected PostgresTransaction $transaction;

    public function createLink(string $connectionString): PostgresLink
    {
        $connectionConfig = PostgresConfig::fromString($connectionString);
        $connection = PqConnection::connect($connectionConfig);

        $connection->query(self::DROP_QUERY);

        $connection->query(self::CREATE_QUERY);

        foreach ($this->getParams() as $row) {
            $connection->execute(self::INSERT_QUERY, $row);
        }

        $this->transaction = $connection->beginTransaction();

        return new PostgresNestableTransaction($this->transaction);
    }

    public function tearDown(): void
    {
        //$this->transaction->rollback();

        parent::tearDown();
    }
}
