<?php

namespace Amp\Postgres;
use pq;

class PqCommandResult implements CommandResult {
    /** @var \pq\Result PostgreSQL result object. */
    private $result;

    /**
     * @param \pq\Result $result PostgreSQL result object.
     */
    public function __construct(pq\Result $result) {
        $this->result = $result;
    }

    /**
     * @return int Number of rows affected by the INSERT, UPDATE, or DELETE query.
     */
    public function affectedRows(): int {
        return $this->result->affectedRows;
    }

    /**
     * @return int
     */
    public function count() {
        return $this->affectedRows();
    }
}