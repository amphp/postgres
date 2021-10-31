<?php

namespace Amp\Postgres;

use Amp\Future;
use Amp\Pipeline\AsyncGenerator;
use Amp\Sql\Result;
use pq;

final class PqBufferedResultSet implements Result, \IteratorAggregate
{
    private AsyncGenerator $generator;

    private int $rowCount;

    private int $columnCount;

    /** @var Future<Result|null> */
    private Future $nextResult;

    /**
     * @param pq\Result $result PostgreSQL result object.
     * @param Future<Result|null> $nextResult Promise for next result set.
     */
    public function __construct(pq\Result $result, Future $nextResult)
    {
        $this->rowCount = $result->numRows;
        $this->columnCount = $result->numCols;
        $this->nextResult = $nextResult;

        $this->generator = new AsyncGenerator(static function () use ($result): \Generator {
            $position = 0;

            while (++$position <= $result->numRows) {
                $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
                yield $result->fetchRow(pq\Result::FETCH_ASSOC);
            }
        });
    }

    /**
     * @inheritDoc
     */
    public function getIterator(): \Traversable
    {
        return $this->generator->getIterator();
    }

    /**
     * @inheritDoc
     */
    public function getNextResult(): ?Result
    {
        return $this->nextResult->await();
    }

    /**
     * @inheritDoc
     */
    public function getRowCount(): int
    {
        return $this->rowCount;
    }

    /**
     * @inheritDoc
     */
    public function getColumnCount(): int
    {
        return $this->columnCount;
    }
}
