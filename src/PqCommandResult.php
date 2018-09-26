<?php

namespace Amp\Postgres;

use Amp\Sql\CommandResult;
use pq;

final class PqCommandResult implements CommandResult
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
