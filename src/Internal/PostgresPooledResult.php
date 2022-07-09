<?php

namespace Amp\Postgres\Internal;

use Amp\Postgres\PostgresResult;
use Amp\Sql\Common\PooledResult;
use Amp\Sql\Result;

/**
 * @internal
 * @extends PooledResult<PostgresResult>
 */
final class PostgresPooledResult extends PooledResult implements PostgresResult
{
    protected function newInstanceFrom(Result $result, \Closure $release): self
    {
        \assert($result instanceof PostgresResult);
        return new self($result, $release);
    }

    public function getNextResult(): ?PostgresResult
    {
        return parent::getNextResult();
    }
}
