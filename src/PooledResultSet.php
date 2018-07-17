<?php

namespace Amp\Postgres;

use Amp\Sql\PooledResultSet as SqlPooledResultSet;

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

    public function numFields(): int
    {
        return $this->result->numFields();
    }
}
