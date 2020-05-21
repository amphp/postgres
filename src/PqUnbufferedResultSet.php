<?php

namespace Amp\Postgres;

use Amp\AsyncGenerator;
use Amp\DisposedException;
use Amp\Promise;
use Amp\Success;
use pq;

final class PqUnbufferedResultSet implements ResultSet
{
    /** @var int */
    private $numCols;

    /** @var AsyncGenerator */
    private $generator;

    /**
     * @param callable():Promise<pq\Result> $fetch Function to fetch next result row.
     * @param \pq\Result $result PostgreSQL result object.
     * @param callable():void $release Invoked once the result has been fully consumed.
     */
    public function __construct(callable $fetch, pq\Result $result, callable $release)
    {
        $this->numCols = $result->numCols;

        $this->generator = new AsyncGenerator(static function (callable $yield) use (
            $release, $result, $fetch
        ): \Generator {
            try {
                do {
                    $promise = $fetch();
                    $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
                    yield $yield($result->fetchRow(pq\Result::FETCH_ASSOC));
                    $result = yield $promise;
                } while ($result instanceof pq\Result);
            } catch (DisposedException $exception) {
                // Discard remaining rows in the result set.
                while ((yield $promise) instanceof pq\Result) {
                    $promise = $fetch();
                }
            } finally {
                $release();
            }
        });
    }

    public function getNextResultSet(): Promise
    {
        return new Success; // Empty stub for now.
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
    public function dispose()
    {
        $this->generator->dispose();
    }

    /**
     * @return int Number of fields (columns) in each result set.
     */
    public function getFieldCount(): int
    {
        return $this->numCols;
    }
}
