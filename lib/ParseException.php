<?php

namespace Amp\Postgres;

class ParseException extends FailureException {
    public function __construct(string $message = '') {
        $message = "Parse error while splitting array" . (($message === '') ? '' : ": " . $message);
        parent::__construct($message);
    }
}
