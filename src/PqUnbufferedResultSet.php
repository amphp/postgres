<?php

namespace Amp\Postgres;

use Amp\AsyncGenerator;
use Amp\Promise;
use Amp\Sql\Result;
use pq;
use function Amp\await;
use function Amp\defer;

final class PqUnbufferedResultSet implements Result, \IteratorAggregate
{
    /** @var AsyncGenerator<array<string, mixed>, null, null> */
    private AsyncGenerator $generator;

    /** @var Promise<Result|null> */
    private Promise $nextResult;

    /**
     * @param callable():Promise<\pq\Result|null> $fetch Function to fetch next result row.
     * @param \pq\Result $result Initial pq\Result result object.
     * @param Promise<Result|null> $nextResult
     */
    public function __construct(callable $fetch, pq\Result $result, Promise $nextResult)
    {
        $this->nextResult = $nextResult;
        $this->generator = new AsyncGenerator(static function () use ($result, $fetch): \Generator {
            try {
                do {
                    $promise = $fetch();
                    $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
                    yield $result->fetchRow(pq\Result::FETCH_ASSOC);
                    $result = await($promise);
                } while ($result instanceof pq\Result);
            } finally {
                if ($result === null) {
                    return; // Result fully consumed.
                }

                defer(static function () use ($promise, $fetch): void {
                    try {
                        // Discard remaining rows in the result set.
                        while (($result = await($promise)) instanceof pq\Result) {
                            $promise = $fetch();
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
        return await($this->nextResult);
    }

    /**
     * @inheritDoc
     */
    public function continue(): ?array
    {
        return $this->generator->continue();
    }

    /**
     * @inheritDoc
     */
    public function dispose(): void
    {
        $this->generator->dispose();
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
