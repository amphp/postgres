<?php

namespace Amp\Postgres;

use Amp\AsyncGenerator;
use Amp\Promise;
use Amp\Sql\Result;
use pq;

final class PqBufferedResultSet implements Result
{
    /** @var AsyncGenerator */
    private $generator;

    /** @var int */
    private $rowCount;

    /** @var Promise<Result|null> */
    private $nextResult;

    /**
     * @param pq\Result $result PostgreSQL result object.
     * @param Promise<Result|null> $nextResult Promise for next result set.
     */
    public function __construct(pq\Result $result, Promise $nextResult)
    {
        $this->rowCount = $result->numRows;
        $this->nextResult = $nextResult;

        $this->generator = new AsyncGenerator(static function (callable $emit) use ($result): \Generator {
            $position = 0;

            while (++$position <= $result->numRows) {
                $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
                yield $emit($result->fetchRow(pq\Result::FETCH_ASSOC));
            }
        });
    }

    /**
     * @inheritDoc
     */
    public function continue(): Promise
    {
        return $this->generator->continue();
    }

    /**
     * @inheritDoc
     */
    public function dispose(): void
    {
        $this->generator->dispose();
    }

    /**
     * @inheritDoc
     */
    public function onDisposal(callable $onDisposal): void
    {
        $this->generator->onDisposal($onDisposal);
    }

    /**
     * @inheritDoc
     */
    public function onCompletion(callable $onCompletion): void
    {
        $this->generator->onCompletion($onCompletion);
    }

    /**
     * @inheritDoc
     */
    public function getNextResult(): Promise
    {
        return $this->nextResult;
    }

    /**
     * @inheritDoc
     */
    public function getRowCount(): int
    {
        return $this->rowCount;
    }
}
