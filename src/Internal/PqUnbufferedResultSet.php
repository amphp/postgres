<?php

namespace Amp\Postgres\Internal;

use Amp\Future;
use Amp\Postgres\PostgresResult;
use pq;
use Revolt\EventLoop;

/** @internal  */
final class PqUnbufferedResultSet implements PostgresResult, \IteratorAggregate
{
    private readonly \Generator $iterator;

    private readonly int $columnCount;

    /**
     * @param \Closure():(\pq\Result|null) $fetch Function to fetch next result row.
     * @param \pq\Result $result Initial pq\Result result object.
     * @param Future<PostgresResult|null> $nextResult
     */
    public function __construct(
        private readonly \Closure $fetch,
        pq\Result $result,
        private readonly Future $nextResult,
    ) {
        $this->columnCount = $result->numCols;

        $this->iterator = self::generate($fetch, $result);
    }

    private static function generate(\Closure $fetch, pq\Result $result): \Generator
    {
        do {
            $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
            yield $result->fetchRow(pq\Result::FETCH_ASSOC);
            $result = $fetch();
        } while ($result instanceof pq\Result);
    }

    public function __destruct()
    {
        if ($this->iterator->valid()) {
            $fetch = $this->fetch;
            EventLoop::queue(static function () use ($fetch): void {
                try {
                    while ($fetch() instanceof pq\Result) {
                        // Discard remaining rows in the result set.
                    }
                } catch (\Throwable) {
                    // Ignore errors while discarding result.
                }
            });
        }
    }

    public function getIterator(): \Traversable
    {
        // Using a Generator to keep a reference to $this.
        yield from $this->iterator;
    }

    public function getNextResult(): ?PostgresResult
    {
        return $this->nextResult->await();
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
