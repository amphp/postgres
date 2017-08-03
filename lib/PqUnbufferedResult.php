<?php

namespace Amp\Postgres;

use Amp\Coroutine;
use Amp\Producer;
use Amp\Promise;
use pq;

class PqUnbufferedResult extends TupleResult implements Operation {
    /** @var int */
    private $numCols;

    /** @var \Amp\Producer */
    private $producer;

    /** @var \Amp\Postgres\Internal\CompletionQueue */
    private $queue;

    /**
     * @param callable(): \Amp\Promise $fetch Function to fetch next result row.
     * @param \pq\Result $result PostgreSQL result object.
     */
    public function __construct(callable $fetch, pq\Result $result) {
        $this->numCols = $result->numCols;
        $this->queue = $queue = new Internal\CompletionQueue;

        parent::__construct($this->producer = new Producer(static function (callable $emit) use ($queue, $result, $fetch) {
            try {
                do {
                    $next = $fetch(); // Request next result before current is consumed.
                    yield $emit($result->fetchRow(pq\Result::FETCH_ASSOC));
                    $result = yield $next;
                } while ($result instanceof pq\Result);
            } finally {
                $queue->complete();
            }
        }));
    }

    public function __destruct() {
        if (!$this->queue->isComplete()) { // Producer above did not complete, so consume remaining results.
            Promise\rethrow(new Coroutine($this->dispose()));
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
    public function onComplete(callable $onComplete) {
        $this->queue->onComplete($onComplete);
    }
}
