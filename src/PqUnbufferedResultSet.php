<?php

namespace Amp\Postgres;

use Amp\Producer;
use Amp\Promise;
use pq;

final class PqUnbufferedResultSet implements ResultSet, Operation {
    /** @var int */
    private $numCols;

    /** @var \Amp\Producer */
    private $producer;

    /** @var array|object Last row emitted. */
    private $currentRow;

    /** @var int Next row fetch type. */
    private $type = self::FETCH_ASSOC;

    /** @var \Amp\Postgres\Internal\ReferenceQueue */
    private $queue;

    /**
     * @param callable(): \Amp\Promise $fetch Function to fetch next result row.
     * @param \pq\Result $result PostgreSQL result object.
     */
    public function __construct(callable $fetch, pq\Result $result) {
        $this->numCols = $result->numCols;
        $this->queue = $queue = new Internal\ReferenceQueue;

        $this->producer = new Producer(static function (callable $emit) use ($queue, $result, $fetch) {
            try {
                do {
                    $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
                    $emit($result);
                    $result = yield $fetch();
                } while ($result instanceof pq\Result);
            } finally {
                $queue->unreference();
            }
        });
    }

    /**
     * {@inheritdoc}
     */
    public function advance(int $type = self::FETCH_ASSOC): Promise {
        $this->currentRow = null;
        $this->type = $type;

        return $this->producer->advance();
    }

    /**
     * {@inheritdoc}
     */
    public function getCurrent() {
        if ($this->currentRow !== null) {
            return $this->currentRow;
        }

        /** @var \pq\Result $result */
        $result = $this->producer->getCurrent();

        switch ($this->type) {
            case self::FETCH_ASSOC:
                return $this->currentRow = $result->fetchRow(pq\Result::FETCH_ASSOC);
            case self::FETCH_ARRAY:
                return $this->currentRow = $result->fetchRow(pq\Result::FETCH_ARRAY);
            case self::FETCH_OBJECT:
                return $this->currentRow = $result->fetchRow(pq\Result::FETCH_OBJECT);
            default:
                throw new \Error("Invalid result fetch type");
        }
    }

    /**
     * @return int Number of fields (columns) in each result set.
     */
    public function numFields(): int {
        return $this->numCols;
    }

    /**
     * {@inheritdoc}
     */
    public function onDestruct(callable $onComplete) {
        $this->queue->onDestruct($onComplete);
    }
}
