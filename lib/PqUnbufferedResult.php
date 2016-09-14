<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Amp\{ Coroutine, Emitter };
use pq;

class PqUnbufferedResult extends TupleResult implements Operation {
    use Internal\Operation;
    
    /** @var int */
    private $numCols;
    
    /**
     * @param callable(): \Generator $fetch Coroutine function to fetch next result row.
     * @param \pq\Result $result PostgreSQL result object.
     */
    public function __construct(callable $fetch, pq\Result $result) {
        $this->numCols = $result->numCols;
        parent::__construct(new Emitter(function (callable $emit) use ($result, $fetch) {
            $count = 0;
            try {
                do {
                    $next = new Coroutine($fetch()); // Request next result before current is consumed.
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
    
    public function numFields(): int {
        return $this->numCols;
    }
}