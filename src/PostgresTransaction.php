<?php

namespace Amp\Postgres;

use Amp\Sql\Transaction;

/**
 * Note that notifications sent during a transaction are not delivered until the transaction has been committed.
 */
interface PostgresTransaction extends PostgresExecutor, PostgresQuoter, Transaction
{
}
