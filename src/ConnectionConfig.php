<?php

namespace Amp\Postgres;

use Amp\Sql\ConnectionConfig as SqlConnectionConfig;

final class ConnectionConfig implements SqlConnectionConfig
{
    /** @var string */
    private $connectionString;

    public function __construct(string $connectionString)
    {
        $this->connectionString = $connectionString;
    }

    public function connectionString(): string
    {
        return $this->connectionString;
    }
}
