<?php

namespace Amp\Postgres;

use Amp\Sql\Link as SqlLink;

interface Link extends Receiver, SqlLink
{
    /**
     * @inheritDoc
     *
     * @return Transaction Transaction object specific to this library.
     */
    public function beginTransaction(int $isolation = Transaction::ISOLATION_COMMITTED): Transaction;
}
