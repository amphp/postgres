<?php

namespace Amp\Postgres;

use Amp\Listener;

abstract class TupleResult extends Listener implements Result {
    /**
     * Returns the number of fields (columns) in each row.
     *
     * @return int
     */
    abstract public function numFields(): int;
}
