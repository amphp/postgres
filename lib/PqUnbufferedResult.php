<?php

namespace Amp\Postgres;

use Amp\{ CallableMaker, Producer };
use pq;

class PqUnbufferedResult extends TupleResult implements Operation {
    use CallableMaker, Internal\Operation {
        __destruct as private release;
    }
    
    /** @var int */
    private $numCols;
    
    /**
     * @param callable(): \AsyncInterop\Promise $fetch Function to fetch next result row.
     * @param \pq\Result $result PostgreSQL result object.
     */
    public function __construct(callable $fetch, pq\Result $result) {
        $this->numCols = $result->numCols;
        parent::__construct(new Producer(function (callable $emit) use ($result, $fetch) {
            $count = 0;
            try {
                do {
                    $next = $fetch(); // Request next result before current is consumed.
                    ++$count;
                    yield $emit($result->fetchRow(pq\Result::FETCH_ASSOC));
                    $result = yield $next;
                } while ($result instanceof pq\Result);
            } finally {
                $this->complete();
            }
            return $count;
        }));
    }

    public function __destruct() {
        parent::__destruct();
        $this->release();
    }

    public function numFields(): int {
        return $this->numCols;
    }
}