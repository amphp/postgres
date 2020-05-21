<?php

namespace Amp\Postgres;

use Amp\DisposedException;
use Amp\Failure;
use Amp\Promise;
use Amp\Success;
use pq;

final class PqBufferedResultSet implements ResultSet
{
    /** @var \pq\Result */
    private $result;

    /** @var int */
    private $position = 0;

    /**
     * @param pq\Result $result PostgreSQL result object.
     */
    public function __construct(pq\Result $result)
    {
        $this->result = $result;
        $this->result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
    }

    /**
     * @inheritDoc
     */
    public function continue(): Promise
    {
        if ($this->result === null) {
            return new Failure(new DisposedException);
        }

        if (++$this->position > $this->result->numRows) {
            return new Success(null);
        }

        return new Success($this->result->fetchRow(pq\Result::FETCH_ASSOC));
    }

    /**
     * @inheritDoc
     */
    public function dispose()
    {
        $this->result = null;
    }

    public function getNextResultSet(): Promise
    {
        return new Success; // Empty stub for now.
    }

    public function getFieldCount(): int
    {
        return $this->result->numCols;
    }
}
