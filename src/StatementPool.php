<?php

namespace Amp\Postgres;

use Amp\Sql\ResultSet as SqlResultSet;
use Amp\Sql\StatementPool as SqlStatementPool;

class StatementPool extends SqlStatementPool
{
    protected function createResultSet(SqlResultSet $resultSet, callable $release): SqlResultSet
    {
        \assert($resultSet instanceof ResultSet);
        return new PooledResultSet($resultSet, $release);
    }
}
