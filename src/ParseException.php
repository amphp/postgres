<?php

namespace Amp\Postgres;

use Amp\Sql\FailureException;

class ParseException extends FailureException
{
    public function __construct(string $message = '')
    {
        $message = "Parse error while splitting array" . (($message === '') ? '' : ": " . $message);
        parent::__construct($message);
    }
}
