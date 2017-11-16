<?php

namespace Amp\Postgres;

use Amp\Iterator;
use Amp\Promise;

interface TupleResult extends Iterator {
    const FETCH_ARRAY = 0;
    const FETCH_ASSOC = 1;
    const FETCH_OBJECT = 2;

    /**
     * {@inheritdoc}
     *
     * @param int $type Next row fetch type. Use the FETCH_* constants provided by this interface.
     */
    public function advance(int $type = self::FETCH_ASSOC): Promise;

    /**
     * Returns the number of fields (columns) in each row.
     *
     * @return int
     */
    public function numFields(): int;
}
