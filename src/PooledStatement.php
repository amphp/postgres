<?php

namespace Amp\Postgres;

use Amp\Sql\Common\PooledStatement as SqlPooledStatement;
use Amp\Sql\ResultSet as SqlResultSet;

final class PooledStatement extends SqlPooledStatement
{
    protected function createResultSet(SqlResultSet $resultSet, callable $release): SqlResultSet
    {
        \assert($resultSet instanceof ResultSet);
        return new PooledResultSet($resultSet, $release);
    }
}
