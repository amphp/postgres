<?php

namespace Amp\Postgres;

use Amp\Sql\Link as SqlLink;
use Amp\Sql\TransactionIsolation;

interface Link extends Receiver, SqlLink
{
    /**
     * @inheritDoc
     *
     * @return Transaction Transaction object specific to this library.
     */
    public function beginTransaction(TransactionIsolation $isolation = TransactionIsolation::COMMITTED): Transaction;
}
