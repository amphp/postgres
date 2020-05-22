<?php

namespace Amp\Postgres;

use Amp\AsyncGenerator;
use Amp\Deferred;
use Amp\DisposedException;
use Amp\Promise;
use pq;

final class PqUnbufferedResultSet implements ResultSet
{
    /** @var int */
    private $numCols;

    /** @var AsyncGenerator */
    private $generator;

    /** @var Deferred */
    private $next;

    /**
     * @param callable():Promise<pq\Result|ResultSet|null> $fetch Function to fetch next result row.
     * @param \pq\Result $result PostgreSQL result object.
     * @param callable():void $release Invoked once the result has been fully consumed.
     */
    public function __construct(callable $fetch, pq\Result $result, callable $release)
    {
        $this->numCols = $result->numCols;

        $this->next = $deferred = new Deferred;
        $this->generator = new AsyncGenerator(static function (callable $yield) use (
            $deferred, $release, $result, $fetch
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
                while (($result = yield $promise) instanceof pq\Result) {
                    $promise = $fetch();
                }
            } finally {
                if ($result instanceof ResultSet) {
                    $deferred->resolve($result);
                    return;
                }

                // Only release if there was no next result set.
                $release();

                $deferred->resolve(null);
            }
        });
    }

    public function getNextResultSet(): Promise
    {
        return $this->next->promise();
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
     * @return int Number of fields (columns) in each result set.
     */
    public function getFieldCount(): int
    {
        return $this->numCols;
    }
}
