<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\Common\StatementPool as SqlStatementPool;
use Amp\Sql\Statement;
use Amp\Success;

final class StatementPool extends SqlStatementPool
{
    protected function prepare(Statement $statement): Promise
    {
        return new Success($statement); // Nothing to be done.
    }
}
