<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\Postgres\PostgresResult;
use Amp\Postgres\PostgresStatement;
use Amp\Postgres\PostgresTransaction;
use Amp\Sql\Common\NestedTransaction;

/**
 * @internal
 * @extends NestedTransaction<PostgresResult, PostgresStatement, PostgresTransaction>
 */
final class PostgresNestedTransaction extends NestedTransaction implements PostgresTransaction
{
    use PostgresTransactionDelegate;

    protected function getTransaction(): PostgresTransaction
    {
        return $this->transaction;
    }
}
