<?php

namespace Amp\Postgres;

use Amp\Sql\Transaction as SqlTransaction;

interface Transaction extends Quoter, SqlTransaction
{
}
