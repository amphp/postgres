<?php

namespace Amp\Postgres;

use Amp\Producer;
use pq;

class PqUnbufferedResult extends TupleResult implements Operation {
    use Internal\Operation;
    
    /** @var int */
    private $numCols;
    
    /**
     * @param callable(): \Amp\Promise $fetch Function to fetch next result row.
     * @param \pq\Result $result PostgreSQL result object.
     */
    public function __construct(callable $fetch, pq\Result $result) {
        $this->numCols = $result->numCols;
        parent::__construct(new Producer(function (callable $emit) use ($result, $fetch) {
            try {
                do {
                    $next = $fetch(); // Request next result before current is consumed.
                    yield $emit($result->fetchRow(pq\Result::FETCH_ASSOC));
                    $result = yield $next;
                } while ($result instanceof pq\Result);
            } finally {
                $this->complete();
            }
        }));
    }

    public function numFields(): int {
        return $this->numCols;
    }
}