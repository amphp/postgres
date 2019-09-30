<?php

namespace Amp\Postgres;

use Amp\Producer;
use Amp\Promise;
use pq;
use function Amp\asyncCall;

final class PqUnbufferedResultSet implements ResultSet
{
    /** @var int */
    private $numCols;

    /** @var \Amp\Producer */
    private $producer;

    /** @var array|object Last row emitted. */
    private $currentRow;

    /** @var bool */
    private $destroyed = false;

    /**
     * @param callable():  $fetch Function to fetch next result row.
     * @param \pq\Result $result PostgreSQL result object.
     */
    public function __construct(callable $fetch, pq\Result $result, callable $release)
    {
        $this->numCols = $result->numCols;

        $destroyed = &$this->destroyed;

        $this->producer = new Producer(static function (callable $emit) use (&$destroyed, $release, $result, $fetch) {
            try {
                do {
                    $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
                    $next = $fetch();
                    yield $emit($result);
                    $result = yield $next;
                } while ($result instanceof pq\Result);
            } finally {
                $destroyed = true;
                $release();
            }
        });
    }

    public function __destruct()
    {
        if ($this->destroyed) {
            return;
        }

        $producer = $this->producer;
        asyncCall(static function () use ($producer) {
            try {
                while (yield $producer->advance());
            } catch (\Throwable $exception) {
                // Ignore iterator failure when destroying.
            }
        });
    }

    /**
     * {@inheritdoc}
     */
    public function advance(): Promise
    {
        $this->currentRow = null;

        return $this->producer->advance();
    }

    /**
     * {@inheritdoc}
     */
    public function getCurrent(): array
    {
        if ($this->currentRow !== null) {
            return $this->currentRow;
        }

        $result = $this->producer->getCurrent();
        \assert($result instanceof pq\Result);

        return $this->currentRow = $result->fetchRow(pq\Result::FETCH_ASSOC);
    }

    /**
     * @return int Number of fields (columns) in each result set.
     */
    public function getFieldCount(): int
    {
        return $this->numCols;
    }
}
