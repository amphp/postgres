<?php

namespace Amp\Postgres\Internal;

use Amp\Future;
use Amp\Postgres\PostgresResult;
use pq;
use Revolt\EventLoop;

/** @internal  */
final class PqUnbufferedResultSet implements PostgresResult, \IteratorAggregate
{
    private readonly \Generator $generator;

    private readonly int $columnCount;

    /**
     * @param \Closure():(\pq\Result|null) $fetch Function to fetch next result row.
     * @param \pq\Result $result Initial pq\Result result object.
     * @param Future<PostgresResult|null> $nextResult
     */
    public function __construct(
        \Closure $fetch,
        pq\Result $result,
        private readonly Future $nextResult,
    ) {
        $this->columnCount = $result->numCols;

        $this->generator = self::generate($fetch, $result);
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
        EventLoop::queue(self::dispose(...), $this->generator);
    }

    private static function dispose(\Generator $generator): void
    {
        try {
            // Discard remaining rows in the result set.
            while ($generator->valid()) {
                $generator->next();
            }
        } catch (\Throwable) {
            // Ignore errors while discarding result.
        }
    }

    public function getIterator(): \Traversable
    {
        // Using a Generator to keep a reference to $this.
        yield from $this->generator;
    }

    public function getNextResult(): ?PostgresResult
    {
        if ($this->generator->valid()) {
            throw new \Error('Consume entire current result before requesting next result');
        }

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
