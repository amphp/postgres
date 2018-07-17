<?php

namespace Amp\Postgres;

use Amp\Sql\ConnectionConfig as SqlConnectionConfig;

final class ConnectionConfig extends SqlConnectionConfig
{
    const DEFAULT_PORT = 5432;

    public static function fromString(string $connectionString): self
    {
        $parts = self::parseConnectionString($connectionString);

        if (!isset($parts["host"])) {
            throw new \Error("Host must be provided in connection string");
        }

        return new self(
            $parts["host"],
            $parts["port"] ?? self::DEFAULT_PORT,
            $parts["user"] ?? null,
            $parts["password"] ?? null,
            $parts["db"] ?? null
        );
    }

    public function __construct(
        string $host,
        int $port = self::DEFAULT_PORT,
        string $user = null,
        string $password = null,
        string $database = null
    ) {
        parent::__construct($host, $port, $user, $password, $database);
    }
}
