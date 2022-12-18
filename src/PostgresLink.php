<?php declare(strict_types=1);

namespace Amp\Postgres;

use Amp\Sql\Link;
use Amp\Sql\TransactionIsolation;
use Amp\Sql\TransactionIsolationLevel;

/**
 * @extends Link<PostgresResult, PostgresStatement, PostgresTransaction>
 */
interface PostgresLink extends Link
{
    /**
     * @return PostgresTransaction Transaction object specific to this library.
     */
    public function beginTransaction(
        TransactionIsolation $isolation = TransactionIsolationLevel::Committed
    ): PostgresTransaction;
}
