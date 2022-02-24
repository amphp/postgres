<?php

namespace Amp\Postgres;

use Amp\Future;
use Amp\Pipeline\ConcurrentIterator;
use Amp\Pipeline\Pipeline;
use Amp\Sql\Result;
use pq;

final class PqBufferedResultSet implements Result, \IteratorAggregate
{
    private readonly ConcurrentIterator $generator;

    private readonly int $rowCount;

    private readonly int $columnCount;

    /** @var Future<Result|null> */
    private readonly Future $nextResult;

    /**
     * @param pq\Result $result PostgreSQL result object.
     * @param Future<Result|null> $nextResult Promise for next result set.
     */
    public function __construct(pq\Result $result, Future $nextResult)
    {
        $this->rowCount = $result->numRows;
        $this->columnCount = $result->numCols;
        $this->nextResult = $nextResult;

        $this->generator = Pipeline::fromIterable(static function () use ($result): \Generator {
            $position = 0;

            while (++$position <= $result->numRows) {
                $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
                yield $result->fetchRow(pq\Result::FETCH_ASSOC);
            }
        })->getIterator();
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
