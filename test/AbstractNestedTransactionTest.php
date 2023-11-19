<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresLink;
use Amp\Postgres\PostgresTransaction;

abstract class AbstractNestedTransactionTest extends AbstractLinkTest
{
    protected PostgresTransaction $transaction;
    protected PostgresTransaction $nested;

    abstract public function connect(PostgresConfig $connectionConfig): PostgresConnection;

    public function createLink(string $connectionString): PostgresLink
    {
        $connectionConfig = PostgresConfig::fromString($connectionString);
        $connection = $this->connect($connectionConfig);

        $connection->query(self::DROP_QUERY);

        $connection->query(self::CREATE_QUERY);

        foreach ($this->getParams() as $row) {
            $connection->execute(self::INSERT_QUERY, $row);
        }

        $this->transaction = $connection->beginTransaction();
        $this->nested = $this->transaction->beginTransaction();

        return $this->nested;
    }

    public function tearDown(): void
    {
        $this->nested->rollback();
        $this->transaction->rollback();

        parent::tearDown();
    }
}
