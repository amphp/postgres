<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\Postgres\PostgresResult;
use Amp\Sql\Common\PooledResult;
use Amp\Sql\Result;

/**
 * @internal
 * @psalm-import-type TFieldType from PostgresResult
 * @extends PooledResult<TFieldType, PostgresResult>
 */
final class PostgresPooledResult extends PooledResult implements PostgresResult
{
    protected static function newInstanceFrom(Result $result, \Closure $release): self
    {
        \assert($result instanceof PostgresResult);
        return new self($result, $release);
    }

    public function getNextResult(): ?PostgresResult
    {
        return parent::getNextResult();
    }
}
