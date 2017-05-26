<?php

namespace Amp\Postgres;

use Amp\Iterator;
use Amp\Promise;

abstract class TupleResult implements Iterator {
    /** @var \Amp\Iterator */
    private $iterator;

    /**
     * @param \Amp\Iterator $iterator
     */
    public function __construct(Iterator $iterator) {
        $this->iterator = $iterator;
    }

    /**
     * {@inheritdoc}
     */
    public function advance(): Promise {
        return $this->iterator->advance();
    }

    /**
     * {@inheritdoc}
     *
     * @return mixed[] Map of column values.
     */
    public function getCurrent(): array {
        return $this->iterator->getCurrent();
    }

    /**
     * Returns the number of fields (columns) in each row.
     *
     * @return int
     */
    abstract public function numFields(): int;
}
