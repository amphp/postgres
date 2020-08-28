<?php

namespace Amp\Postgres;

use Amp\AsyncGenerator;
use Amp\DisposedException;
use Amp\Promise;
use Amp\Sql\Result;
use pq;

final class PqUnbufferedResultSet implements Result
{
    /** @var AsyncGenerator<array<string, mixed>, null, null> */
    private $generator;

    /** @var Promise<Result|null> */
    private $nextResult;

    /**
     * @param callable():Promise<\pq\Result|null> $fetch Function to fetch next result row.
     * @param \pq\Result $result Initial pq\Result result object.
     * @param Promise<Result|null> $nextResult
     */
    public function __construct(callable $fetch, pq\Result $result, Promise $nextResult)
    {
        $this->nextResult = $nextResult;
        $this->generator = new AsyncGenerator(static function (callable $emit) use ($result, $fetch): \Generator {
            try {
                do {
                    $promise = $fetch();
                    $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
                    yield $emit($result->fetchRow(pq\Result::FETCH_ASSOC));
                    $result = yield $promise;
                } while ($result instanceof pq\Result);
            } catch (DisposedException $exception) {
                // Discard remaining rows in the result set.
                while (($result = yield $promise) instanceof pq\Result) {
                    $promise = $fetch();
                }
            }
        });
    }

    public function getNextResult(): Promise
    {
        return $this->nextResult;
    }

    /**
     * @inheritDoc
     */
    public function continue(): Promise
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
    public function getRowCount(): ?int
    {
        return null; // Unbuffered result sets do not have a total row count.
    }
}
