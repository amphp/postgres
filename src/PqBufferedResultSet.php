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
    public function getCurrent(int $type = self::FETCH_ASSOC)
    {
        if ($this->currentRow !== null) {
            return $this->currentRow;
        }

        if ($this->position > $this->result->numRows) {
            throw new \Error("No more rows remain in the result set");
        }

        switch ($type) {
            case self::FETCH_ASSOC:
                return $this->currentRow = $this->result->fetchRow(pq\Result::FETCH_ASSOC);
            case self::FETCH_ARRAY:
                return $this->currentRow = $this->result->fetchRow(pq\Result::FETCH_ARRAY);
            case self::FETCH_OBJECT:
                return $this->currentRow = $this->result->fetchRow(pq\Result::FETCH_OBJECT);
            default:
                throw new \Error("Invalid result fetch type");
        }
    }

    public function numRows(): int
    {
        return $this->result->numRows;
    }

    public function numFields(): int
    {
        return $this->result->numCols;
    }
}
