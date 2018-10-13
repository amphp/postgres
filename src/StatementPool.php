<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\Common\StatementPool as SqlStatementPool;
use Amp\Sql\ResultSet as SqlResultSet;
use Amp\Sql\Statement;
use Amp\Success;

final class StatementPool extends SqlStatementPool
{
    protected function createResultSet(SqlResultSet $resultSet, callable $release): SqlResultSet
    {
        \assert($resultSet instanceof ResultSet);
        return new PooledResultSet($resultSet, $release);
    }

    protected function prepare(Statement $statement): Promise
    {
        return new Success($statement); // Nothing to be done.
    }
}
