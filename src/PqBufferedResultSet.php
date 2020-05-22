<?php

namespace Amp\Postgres;

use Amp\DisposedException;
use Amp\Failure;
use Amp\Promise;
use Amp\Sql\Result;
use Amp\Success;
use pq;

final class PqBufferedResultSet implements Result
{
    /** @var \pq\Result */
    private $result;

    /** @var int */
    private $position = 0;

    /** @var Promise<ResultSet|null> */
    private $nextResult;

    /**
     * @param pq\Result $result PostgreSQL result object.
     * @param Promise<ResultSet|null> $nextResult Promise for next result set.
     */
    public function __construct(pq\Result $result, Promise $nextResult)
    {
        $this->result = $result;
        $this->result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
        $this->nextResult = $nextResult;
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
    public function dispose(): void
    {
        $this->result = null;
    }

    /**
     * @inheritDoc
     */
    public function getNextResult(): Promise
    {
        return $this->nextResult;
    }

    /**
     * @inheritDoc
     */
    public function getRowCount(): int
    {
        return $this->result->numRows;
    }
}
