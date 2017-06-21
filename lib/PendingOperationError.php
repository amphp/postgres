<?php

namespace Amp\Postgres;

class PendingOperationError extends \Error {
    public function __construct(
        string $message = "The previous operation must complete before starting another",
        int $code = 0,
        \Throwable $previous = null
    ) {
        parent::__construct($message, $code, $previous);
    }
}
