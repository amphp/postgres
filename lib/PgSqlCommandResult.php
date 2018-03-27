<?php

namespace Amp\Postgres;

final class PgSqlCommandResult implements CommandResult {
    /** @var resource PostgreSQL result resource. */
    private $handle;

    /**
     * @param resource $handle PostgreSQL result resource.
     */
    public function __construct($handle) {
        $this->handle = $handle;
    }

    /**
     * Frees the result resource.
     */
    public function __destruct() {
        \pg_free_result($this->handle);
    }

    /**
     * @return int Number of rows affected by the INSERT, UPDATE, or DELETE query.
     */
    public function affectedRows(): int {
        return \pg_affected_rows($this->handle);
    }

    /**
     * @return string
     */
    public function lastOid(): string {
        return (string) \pg_last_oid($this->handle);
    }
}
