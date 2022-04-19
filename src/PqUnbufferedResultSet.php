<?php

namespace Amp\Postgres;

use Amp\Future;
use Amp\Pipeline\ConcurrentIterator;
use Amp\Pipeline\Pipeline;
use Amp\Sql\Result;
use pq;
use Revolt\EventLoop;

final class PqUnbufferedResultSet implements Result, \IteratorAggregate
{
    /** @var ConcurrentIterator<array<string, mixed>> */
    private readonly ConcurrentIterator $iterator;

    /** @var Future<Result|null> */
    private readonly Future $nextResult;

    private readonly int $columnCount;

    /**
     * @param \Closure():(\pq\Result|null) $fetch Function to fetch next result row.
     * @param \pq\Result $result Initial pq\Result result object.
     * @param Future<Result|null> $nextResult
     */
    public function __construct(\Closure $fetch, pq\Result $result, Future $nextResult)
    {
        $this->nextResult = $nextResult;
        $this->columnCount = $result->numCols;

        $this->iterator = Pipeline::fromIterable(static function () use ($result, $fetch): \Generator {
            try {
                do {
                    $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
                    yield $result->fetchRow(pq\Result::FETCH_ASSOC);
                    $result = $fetch();
                } while ($result instanceof pq\Result);
            } finally {
                if ($result === null) {
                    return; // Result fully consumed.
                }

                EventLoop::queue(static function () use ($fetch): void {
                    try {
                        // Discard remaining rows in the result set.
                        while ($fetch() instanceof pq\Result) ;
                    } catch (\Throwable) {
                        // Ignore errors while discarding result.
                    }
                });
            }
        })->getIterator();
    }

    public function getNextResult(): ?Result
    {
        return $this->nextResult->await();
    }

    public function getIterator(): \Traversable
    {
        return $this->iterator;
    }

    public function getRowCount(): ?int
    {
        return null; // Unbuffered result sets do not have a total row count.
    }

    public function getColumnCount(): int
    {
        return $this->columnCount;
    }
}
