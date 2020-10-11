<?php

namespace Amp\Postgres;

use Amp\AsyncGenerator;
use Amp\Promise;
use Amp\Sql\Result;
use pq;
use function Amp\await;

final class PqBufferedResultSet implements Result
{
    private AsyncGenerator $generator;

    private int $rowCount;

    /** @var Promise<Result|null> */
    private Promise $nextResult;

    /**
     * @param pq\Result $result PostgreSQL result object.
     * @param Promise<Result|null> $nextResult Promise for next result set.
     */
    public function __construct(pq\Result $result, Promise $nextResult)
    {
        $this->rowCount = $result->numRows;
        $this->nextResult = $nextResult;

        $this->generator = new AsyncGenerator(static function () use ($result): \Generator {
            $position = 0;

            while (++$position <= $result->numRows) {
                $result->autoConvert = pq\Result::CONV_SCALAR | pq\Result::CONV_ARRAY;
                yield $result->fetchRow(pq\Result::FETCH_ASSOC);
            }
        });
    }

    /**
     * @inheritDoc
     */
    public function continue(): ?array
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
    public function getNextResult(): ?Result
    {
        return await($this->nextResult);
    }

    /**
     * @inheritDoc
     */
    public function getRowCount(): int
    {
        return $this->rowCount;
    }
}
