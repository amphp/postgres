<?php

namespace Amp\Postgres;

use Amp\Sql\Result;

interface PostgresResult extends Result
{
    /**
     * Changes return type to this library's Result type.
     */
    public function getNextResult(): ?self;
}
