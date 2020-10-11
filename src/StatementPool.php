<?php

namespace Amp\Postgres;

use Amp\Sql\Common\StatementPool as SqlStatementPool;
use Amp\Sql\Statement;

final class StatementPool extends SqlStatementPool
{
    protected function prepare(Statement $statement): Statement
    {
        return $statement; // Nothing to be done.
    }
}
