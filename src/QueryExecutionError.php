<?php

namespace Amp\Postgres;

use Amp\Sql\QueryError;

class QueryExecutionError extends QueryError
{
    /** @var mixed[] */
    private $diagnostics;

    public function __construct(string $message, array $diagnostics, \Throwable $previous = null, string $query = '')
    {
        parent::__construct($message, $query, $previous);
        $this->diagnostics = $diagnostics;
    }

    public function getDiagnostics(): array
    {
        return $this->diagnostics;
    }
}
