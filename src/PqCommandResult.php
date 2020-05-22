<?php

namespace Amp\Postgres;

use Amp\Sql\Result;
use pq;

final class PqCommandResult implements Result
{
    /** @var \pq\Result PostgreSQL result object. */
    private $result;

    /**
     * @param \pq\Result $result PostgreSQL result object.
     */
    public function __construct(pq\Result $result)
    {
        $this->result = $result;
    }

    /**
     * @return int Number of rows affected by the INSERT, UPDATE, or DELETE query.
     */
    public function getAffectedRowCount(): int
    {
        return $this->result->affectedRows;
    }
}
