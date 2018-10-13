<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Success;
use pq;

final class PqBufferedResultSet implements ResultSet
{
    /** @var \pq\Result */
    private $result;

    /** @var int */
    private $position = 0;

    /** @var mixed Last row emitted. */
    private $currentRow;

    /**
     * @param pq\Result $result PostgreSQL result object.
     */
    public function __construct(pq\Result $result)
    {
        $this->result = $result;
        $this->result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
    }

    /**
     * {@inheritdoc}
     */
    public function advance(): Promise
    {
        $this->currentRow = null;

        if (++$this->position > $this->result->numRows) {
            return new Success(false);
        }

        return new Success(true);
    }

    /**
     * {@inheritdoc}
     */
    public function getCurrent(): array
    {
        if ($this->currentRow !== null) {
            return $this->currentRow;
        }

        if ($this->position > $this->result->numRows) {
            throw new \Error("No more rows remain in the result set");
        }

        return $this->currentRow = $this->result->fetchRow(pq\Result::FETCH_ASSOC);
    }

    public function getNumRows(): int
    {
        return $this->result->numRows;
    }

    public function getFieldCount(): int
    {
        return $this->result->numCols;
    }
}
