<?php

namespace Amp\Postgres;

use Amp\Producer;
use Amp\Promise;
use pq;

final class PqUnbufferedResultSet implements ResultSet
{
    /** @var int */
    private $numCols;

    /** @var \Amp\Producer */
    private $producer;

    /** @var array|object Last row emitted. */
    private $currentRow;

    /**
     * @param callable():  $fetch Function to fetch next result row.
     * @param \pq\Result $result PostgreSQL result object.
     */
    public function __construct(callable $fetch, pq\Result $result, callable $release)
    {
        $this->numCols = $result->numCols;

        $this->producer = new Producer(static function (callable $emit) use ($release, $result, $fetch) {
            try {
                do {
                    $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
                    $emit($result);
                    $result = yield $fetch();
                } while ($result instanceof pq\Result);
            } finally {
                $release();
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
    public function getCurrent(int $type = self::FETCH_ASSOC)
    {
        if ($this->currentRow !== null) {
            return $this->currentRow;
        }

        /** @var \pq\Result $result */
        $result = $this->producer->getCurrent();

        switch ($type) {
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
    public function numFields(): int
    {
        return $this->numCols;
    }
}
