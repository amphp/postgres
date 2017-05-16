<?php

namespace Amp\Postgres;

use Amp\Producer;
use pq;

class PqBufferedResult extends TupleResult {
    /** @var \pq\Result */
    private $result;

    /**
     * @param pq\Result $result PostgreSQL result object.
     */
    public function __construct(pq\Result $result) {
        $this->result = $result;
        parent::__construct(new Producer(static function (callable $emit) use ($result) {
            while ($row = $result->fetchRow(pq\Result::FETCH_ASSOC)) {
                yield $emit($row);
            }
        }));
    }

    public function numRows(): int {
        return $this->result->numRows;
    }

    public function numFields(): int {
        return $this->result->numCols;
    }
}
