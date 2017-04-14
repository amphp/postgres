<?php

namespace Amp\Postgres;

use Amp\StreamIterator;

abstract class TupleResult extends StreamIterator implements Result {
    /**
     * Returns the number of fields (columns) in each row.
     *
     * @return int
     */
    abstract public function numFields(): int;
}
