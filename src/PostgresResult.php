<?php

namespace Amp\Postgres;

use Amp\Sql\Result;

/**
 * Recursive template types currently not supported, list<mixed> should be list<TFieldType>.
 * @psalm-type TFieldType list<mixed>|bool|int|float|string|null
 * @extends Result<TFieldType>
 */
interface PostgresResult extends Result
{
    /**
     * Changes return type to this library's Result type.
     */
    public function getNextResult(): ?self;
}
