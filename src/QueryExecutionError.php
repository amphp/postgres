<?php

namespace Amp\Postgres;

use Amp\Sql\QueryError;

class QueryExecutionError extends QueryError {
    /** @var mixed[] */
    private $diagnostics;

    public function __construct(string $message, array $diagnostics, \Throwable $previous = null) {
        parent::__construct($message, 0, $previous);
        $this->diagnostics = $diagnostics;
    }

    public function getDiagnostics(): array {
        return $this->diagnostics;
    }
}
