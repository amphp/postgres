<?php

namespace Amp\Postgres;

use Amp\Future;
use Amp\Pipeline\AsyncGenerator;
use Amp\Sql\Result;
use pq;
use Revolt\EventLoop;

final class PqUnbufferedResultSet implements Result, \IteratorAggregate
{
    /** @var AsyncGenerator<array<string, mixed>, null, null> */
    private AsyncGenerator $generator;

    /** @var Future<Result|null> */
    private Future $nextResult;

    /**
     * @param callable():Future<\pq\Result|null> $fetch Function to fetch next result row.
     * @param \pq\Result $result Initial pq\Result result object.
     * @param Future<Result|null> $nextResult
     */
    public function __construct(callable $fetch, pq\Result $result, Future $nextResult)
    {
        $this->nextResult = $nextResult;
        $this->generator = new AsyncGenerator(static function () use ($result, $fetch): \Generator {
            try {
                do {
                    $future = $fetch();
                    $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
                    yield $result->fetchRow(pq\Result::FETCH_ASSOC);
                    $result = $future->await();
                } while ($result instanceof pq\Result);
            } finally {
                if ($result === null) {
                    return; // Result fully consumed.
                }

                EventLoop::queue(static function () use ($future, $fetch): void {
                    try {
                        // Discard remaining rows in the result set.
                        while (($result = $future->await()) instanceof pq\Result) {
                            $future = $fetch();
                        }
                    } catch (\Throwable $exception) {
                        // Ignore errors while discarding result.
                    }
                });
            }
        });
    }

    public function getNextResult(): ?Result
    {
        return $this->nextResult->await();
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
    public function getRowCount(): ?int
    {
        return null; // Unbuffered result sets do not have a total row count.
    }
}
