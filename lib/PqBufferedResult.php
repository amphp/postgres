<?php

namespace Amp\Postgres;

use Amp\Producer;
use pq;

class PqBufferedResult extends TupleResult implements \Countable {
    /** @var \pq\Result */
    private $result;

    /**
     * @param pq\Result $result PostgreSQL result object.
     */
    public function __construct(pq\Result $result) {
        $this->result = $result;
        parent::__construct(new Producer(static function (callable $emit) use ($result) {
            for ($count = 0; $row = $result->fetchRow(pq\Result::FETCH_ASSOC); ++$count) {
                yield $emit($row);
            }
            return $count;
        }));
    }
    
    public function numRows(): int {
        return $this->result->numRows;
    }
    
    public function numFields(): int {
        return $this->result->numCols;
    }
    
    public function count(): int {
        return $this->numRows();
    }
}
