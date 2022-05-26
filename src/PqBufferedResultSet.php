<?php

namespace Amp\Postgres;

use Amp\Future;
use Amp\Sql\Result;
use pq;

final class PqBufferedResultSet implements Result, \IteratorAggregate
{
    private readonly \Generator $iterator;

    private readonly int $rowCount;

    private readonly int $columnCount;

    /**
     * @param Future<Result|null> $nextResult Promise for next result set.
     */
    public function __construct(
        pq\Result $result,
        private readonly Future $nextResult,
    ) {
        $this->rowCount = $result->numRows;
        $this->columnCount = $result->numCols;

        $this->iterator = self::generate($result);
    }

    private static function generate(pq\Result $result): \Generator
    {
        $position = 0;

        while (++$position <= $result->numRows) {
            $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
            yield $result->fetchRow(pq\Result::FETCH_ASSOC);
        }
    }

    public function getIterator(): \Traversable
    {
        return $this->iterator;
    }

    public function getNextResult(): ?Result
    {
        return $this->nextResult->await();
    }

    public function getRowCount(): int
    {
        return $this->rowCount;
    }

    public function getColumnCount(): int
    {
        return $this->columnCount;
    }
}
