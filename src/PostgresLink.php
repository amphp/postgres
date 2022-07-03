<?php

namespace Amp\Postgres;

use Amp\Sql\Link;
use Amp\Sql\TransactionIsolation;
use Amp\Sql\TransactionIsolationLevel;

interface PostgresLink extends Link, PostgresReceiver
{
    /**
     * @return PostgresTransaction Transaction object specific to this library.
     */
    public function beginTransaction(
        TransactionIsolation $isolation = TransactionIsolationLevel::Committed
    ): PostgresTransaction;
}
