<?php

namespace Amp\Postgres;

use Amp\Sql\Transaction as SqlTransaction;

/**
 * Note that notifications sent during a transaction are not delivered until the transaction has been committed.
 */
interface Transaction extends Executor, Quoter, SqlTransaction
{
}
