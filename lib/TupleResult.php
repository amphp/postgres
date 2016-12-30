<?php

namespace Amp\Postgres;

use Amp\Observer;

abstract class TupleResult extends Observer implements Result {
    /**
     * Returns the number of fields (columns) in each row.
     *
     * @return int
     */
    abstract public function numFields(): int;
}
