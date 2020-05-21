<?php

namespace Amp\Postgres;

use Amp\Sql\Common\PooledResultSet as SqlPooledResultSet;
use Amp\Sql\ResultSet as SqlResultSet;

final class PooledResultSet extends SqlPooledResultSet implements ResultSet
{
    /** @var ResultSet */
    private $result;

    /**
     * @param ResultSet $result
     * @param callable  $release
     */
    public function __construct(ResultSet $result, callable $release)
    {
        parent::__construct($result, $release);
        $this->result = $result;
    }

    protected function createNewInstanceFrom(SqlResultSet $result, callable $release): SqlPooledResultSet
    {
        \assert($result instanceof ResultSet);
        return new self($result, $release);
    }

    public function getFieldCount(): int
    {
        return $this->result->getFieldCount();
    }
}
