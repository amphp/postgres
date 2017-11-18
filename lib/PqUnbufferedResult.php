<?php

namespace Amp\Postgres;

use Amp\Coroutine;
use Amp\Producer;
use Amp\Promise;
use pq;

class PqUnbufferedResult implements TupleResult, Operation {
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
                    $next = $fetch(); // Request next result before current is consumed.
                    yield $emit($result);
                    $result = yield $next;
                } while ($result instanceof pq\Result);
            } finally {
                $queue->unreference();
            }
        });
    }

    public function __destruct() {
        if (!$this->queue->isReferenced()) { // Producer above did not complete, so consume remaining results.
            Promise\rethrow(new Coroutine($this->dispose()));
        }
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

        switch ($this->type) {
            case self::FETCH_ASSOC:
                return $this->currentRow = $this->producer->getCurrent()->fetchRow(pq\Result::FETCH_ASSOC);
            case self::FETCH_ARRAY:
                return $this->currentRow = $this->producer->getCurrent()->fetchRow(pq\Result::FETCH_ARRAY);
            case self::FETCH_OBJECT:
                return $this->currentRow = $this->producer->getCurrent()->fetchRow(pq\Result::FETCH_OBJECT);
            default:
                throw new \Error("Invalid result fetch type");
        }
    }

    private function dispose(): \Generator {
        try {
            while (yield $this->producer->advance()); // Discard unused result rows.
        } catch (\Throwable $exception) {
            // Ignore failure while discarding results.
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
