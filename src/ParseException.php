<?php

namespace Amp\Postgres;

use Amp\Sql\SqlException;

final class ParseException extends SqlException
{
    public function __construct(string $message = '')
    {
        $message = "Parse error while splitting array" . (($message === '') ? '' : ": " . $message);
        parent::__construct($message);
    }
}
