<?php

namespace Amp\Postgres;

use Amp\Sql\Common\PooledResultSet as SqlPooledResultSet;

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

    public function getFieldCount(): int
    {
        return $this->result->getFieldCount();
    }
}
