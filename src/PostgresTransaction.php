<?php declare(strict_types=1);

namespace Amp\Postgres;

use Amp\Sql\Transaction;

/**
 * Note that notifications sent during a transaction are not delivered until the transaction has been committed.
 *
 * @extends Transaction<PostgresResult, PostgresStatement, PostgresTransaction>
 */
interface PostgresTransaction extends PostgresLink, Transaction
{
}
